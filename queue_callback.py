from dask.distributed import Client, Queue, Future, Event
import os
import asyncio

DASK_CLIENT_HOST = os.getenv('DASK_CLIENT_HOST','127.0.0.1')
client = Client(F'tcp://{DASK_CLIENT_HOST}:8786')
q = Queue(name = 'my-test-queue', client = client)
def callback_function_test(future):
    print('fututre callback is called')
    if future.cancelled() == False:
        if future.exception():
            print(F'future exception happened {future.exception()} and status {future.status}')
        print(str(future.result()))

# Decorator to run input function in a different thread
def threaded():
    print('threaded decorator was initialized')
    def inner(func):
        print('inner method is set')
        def wrapper(*args, **kwargs):
            def __exec():
                out = func(args)
                return out
            return asyncio.get_event_loop().run_in_executor(None, __exec)
        return wrapper
    return inner

# @threaded(_callback)
@threaded()
def check_futures(args):
    errs = []
    print('checking for old hanging tasks')
    if len(args) == 0:
        # probably here you would want to record the error log in DB
        err = 'no_dask_client'
        return [err]
    dask_client = args[0]
    futures_to_cancel = []
    events_to_set = []
    for k,v in dask_client.who_has().items():
        if '_ts' in k and len(v) == 0:
            ts = 0
            try:
                ts = int(k.split('_ts')[1])
            except ValueError:
                errs = errs + [F'couldnt_convert_time_in_task_key:{k}']
            except IndexError:
                errs = errs + errs[F'couldnt_parse_time_in_task_key:{k}']
            if ts > 0:
                # calling twice as sometimes it shows updated status only after two same calls in a row
                # TODO: haven't figure out why this is happening yet
                fut_status = Future(k).status
                fut_status = Future(k).status
                if fut_status == 'pending':
                    futures_to_cancel = futures_to_cancel + [Future(k)]
                    events_to_set = events_to_set + [Event('event_'+k)]
    print(F'len(futures_to_cancel) = {len(futures_to_cancel)}')
    # in case if the task hasn't been started yet, cancel is the right approach
    try:
        dask_client.cancel(futures_to_cancel, force = True)
        print('hanging tasks are successfully cancelled')
    except Exception as e:
        print(F'couldnt cancel the tasks {e}')
        errs = errs + [str(e)]
    print(F'who_has updated {dask_client.who_has()}')
    print('setting events')
    for event in events_to_set:
        event.set()
    print('all events are set')
    print(F'who_has updated {dask_client.who_has()}')
    return errs

count = 0
while True:
    fut = q.get()
    if fut:
        print('adding add_done_callback')
        fut.add_done_callback(callback_function_test)
        count += 1
        if count == 5:
            check_fut_errs = check_futures(client)