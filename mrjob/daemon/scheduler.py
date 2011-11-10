from multiprocessing import Process
from multiprocessing import Queue
from time import sleep


def schedule(queue, function, *args, **kwargs):
    queue.put([function, args, kwargs])

def scheduler(in_queue):
    while True:

