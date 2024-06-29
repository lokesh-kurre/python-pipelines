import threading
import multiprocessing as mp
from multiprocessing.pool import ThreadPool, Pool

def worker(data, lock):
    with lock:
        try:
            print(mp.current_process().pid, threading.current_thread().name)
            print(len(data), data)
        except TypeError as e:
            print(data)

if __name__ == "__main__":
    print("Main Process", mp.current_process().pid, threading.current_thread().name)
    data = [1, 2, 3]
    chunksize = 2
    new_data = [ data[_i:_i+chunksize] for _i in range(0, len(data), chunksize)]
    print("INPUT_DATA", new_data)

    lock = threading.Lock()
    with ThreadPool(3) as pool:
        pool.map(lambda x : worker(x, lock), new_data)