from dask.distributed import Client, Queue, Event
from dask.distributed import Semaphore
import multiprocessing
import psycopg2
import random
import time
import os
import uuid

DB_HOST = os.getenv('DB_HOST','')
DB_PORT = os.getenv('DB_PORT','')
DB_NAME = os.getenv('DB_NAME','')
DB_USER = os.getenv('DB_USER','')
DB_PASSWORD = os.getenv('DB_PASSWORD','')
DASK_CLIENT_HOST = os.getenv('DASK_CLIENT_HOST','127.0.0.1')
STATEMENT_TIMEOUT=10000
# CONN_STRING = "host='%s' port='%s' dbname='%s' user='%s' password = '%s' options='-c statement_timeout=%s'" %(DB_HOST,DB_PORT,
#                     DB_NAME, DB_USER,DB_PASSWORD,STATEMENT_TIMEOUT)
SQL_CONN_STRING = "host='%s' port='%s' dbname='%s' user='%s' password = '%s'" %(DB_HOST,DB_PORT,
                    DB_NAME, DB_USER,DB_PASSWORD)

def processLog_sem(CONN_STRING, passing_vars, sem, event):
    a = 'init '
    with sem:
        with psycopg2.connect(CONN_STRING) as cnxn: 
            try:
                a = 'hello world'
                print('processLog_sem connected')
                rand = random.randint(passing_vars[0],passing_vars[1])
                print(F'event name {event.name}')
                # an important part, which regularly checks if event is set
                for _ in range(100):
                    time.sleep(rand/100)
                    if event.is_set():
                        print(F'event {event.name} is set')
                        return 'terminated'
                a = a + ' rand' + str(rand)
                if rand < 25:
                    assert 1 == 2 # for showing purpose initiating an error
                # cnxn.close() -> not needed, as the construction with connect as cnxn automatically closes the connection upon return
                a = F'success:{a}'
            except Exception as e:
                a = F'error:{str(e)}'
                return a
            finally:
                # cnxn.close() -> not needed, as the construction with connect as cnxn automatically closes the connection upon return
                a = a + '. Finally returned'
                return a
        return a


class Consumer(multiprocessing.Process):
    def __init__(self):
        print('consumer initialized')

    def run(self):

        client = Client(F'tcp://{DASK_CLIENT_HOST}:8786')
        # Dask Semaphone works exactly as standard threading Semaphores
        # in our example it might be useful to control the amount of simultaneous SQL connections
        # however in this script doesn't play any important role, purely for demonstration how to use it
        sem = Semaphore(max_leases = 10, name='my-database-conn')
        # every future will be appended to a queue and controlled by a different script
        queue = Queue('my-test-queue', client)
        count = 0
        while True:
            # waiting time interval for out function/calculation of the record
            passing_vars = (20, 30)
            # generating a custom task_key in the same manner as default generation by Dask 
            # with appending the timestamp of submitting the task so we can detect old tasks later during the check
            task_key = 'processLog_sem-' + str(uuid.uuid4()) + '_ts' + str(round(time.time()))
            # Dask Event mimic asyncio.Event, such event name will help us to 
            event = Event('event_' + task_key)
            print('submitting')
            # submits the function processLog_sem with input params to the scheduler
            # it's similar as asyncio.ensure_futures 
            # the return is the future, which then we put to the queue to control from the other script
            fut = client.submit(processLog_sem, SQL_CONN_STRING, passing_vars, sem = sem, event = event, key = task_key)
            queue.put(fut)
            # time.sleep(random.randint(3,10))
            time.sleep(random.randint(1,3))
            count +=1
            if count == 7:
                print('sleep')
                time.sleep(1000000000)
        sem.close()

def main():
    consumer = Consumer()
    consumer.run()

if __name__ == "__main__":
    main()