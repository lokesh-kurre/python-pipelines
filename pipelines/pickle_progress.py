import io
import pickle
import argparse
import threading
import pandas as pd
from tqdm import tqdm
from functools import partial
from multiprocessing import Lock, current_process
from multiprocessing.pool import Pool, ThreadPool

def init_pool_processes(the_lock, verbose_mode= None):
    '''Initialize each process with a global variable lock.
    '''
    global lock, verbose
    lock = the_lock
    verbose = verbose_mode
    if verbose and len(verbose) > 1:
        with lock:
            print("Worker Process", current_process().pid, threading.current_thread().name, "Initialized with verbose set.")


def worker(row, thread_lock, *, verbose= None):
    index, data = row

    # ... processing steps, and storing result into `result` variable
    data["result"] = data['columnA'] % 2
    import time
    time.sleep(3)
    
    # dumping using pickle
    with lock:
        with thread_lock:
            if verbose and len(verbose) > 2:
                print("Worker Thread", current_process().pid, threading.current_thread().name)
            with open("./data/log.pickle", "ab") as file:
                pickle.dump(data.to_dict(), file)

def threads_worker(data, no_of_worker_threads= 10, *, disable_progress_bar= False):
    if verbose and len(verbose) > 1:
        with lock:
            print("Worker Process", current_process().pid, threading.current_thread().name, "running with data of size", len(data))
            print("Worker Progress bar is", "disabled" if disable_progress_bar else "enabled")
    thread_lock = threading.Lock()
    with ThreadPool(no_of_worker_threads) as pool:
        list(tqdm(pool.imap(lambda row: worker(row, thread_lock, verbose= verbose), data.iterrows()), total=len(data), disable= disable_progress_bar))

def process(data, *, result= None, threads= 10, processes= 1, chunk_size= None, verbose= ""):
    worker_processes = processes or 1
    
    # create enpty file log.pickle
    open("./data/log.pickle", 'w').close()

    chunk_size =  chunk_size or (len(data) // worker_processes)
    chunked_data: list[pd.DataFrame] = [ data[_i:_i+chunk_size] for _i in range(0, len(data), chunk_size)]
    
    if verbose and len(verbose) > 0:
        print("Main Process", current_process().pid, threading.current_thread().name)
        print("Main Process fed data of size", len(data), "chunk_size", chunk_size)
        print("Main Progress bar is", "disabled" if worker_processes == 1 else "enabled")

    lock = Lock()
    with Pool(worker_processes, initializer= init_pool_processes, initargs= (lock, verbose)) as pool:
        threads_worker_partial_args = partial(threads_worker, no_of_worker_threads= threads, disable_progress_bar= processes != 1)
        list(tqdm(pool.imap(threads_worker_partial_args, chunked_data), total=len(chunked_data), disable= worker_processes == 1))

    print("Processing complete. Check ./data/log.pickle for results.")

def read_logs_pickle(filepath):
    rows = []
    with open(filepath, "rb") as file:
        buff = io.BytesIO(file.read())
    while True:
        try:
            data = pickle.load(buff)
            rows.append(data)
        except EOFError:
            break
    return pd.DataFrame(rows)

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser("pipeline", description= "general pipeline which uses efficient multi process and multi thread to handle task provided, and logs the same")
    arg_parser.add_argument('-t', '--threads', type= int, default= 10, help= 'no of thread to be used default 10')
    arg_parser.add_argument('-p', '--processes', type= int, default= 1, help= 'no of processes to be used default 1')
    arg_parser.add_argument('-c', '--chunk-size', type= int, default= None, help= 'chunksize to feed into worker process')
    arg_parser.add_argument('-v', '--verbose', default= [], const= 'v', action= 'append_const', help= 'verbosity, can be used repeatedly')
    arg_parser.add_argument('filepath', help= 'filepath of test csv')
    
    args = arg_parser.parse_args()
    # test_csv = pd.read_csv(args.filepath)
    test_csv = pd.DataFrame({
        "columnA": pd.Series([1, 2, 3, 4], dtype=int),
        "columnB": pd.Series([["a, b"], "c\"d\"", "d", "lsls"], dtype= object),
        "columnC": pd.Series([["a, b"], "c\"d\"", "d", "lsls"], dtype= object),
    })

    process(test_csv, threads= args.threads, processes= args.processes, chunk_size= args.chunk_size, verbose= args.verbose)

    # result = read_logs_pickle("./data/log.pickle")
    # print(result)
    # print(type(result.iloc[2, 2]))