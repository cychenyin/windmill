from threading import Event
import concurrent.futures
import thread
import time

# submit static method and callback static method

def run_job(job):
    e = Event()
    e.clear()
    e.wait(job)
    return (1, job)
    
class submitter:
    
    def __init__(self):
        self._pool = concurrent.futures.ThreadPoolExecutor(5)
    
    def submit(self, job):
        def callback(f):
            exc, tb = (f.exception_info() if hasattr(f, 'exception_info') else (f.exception(), getattr(f.exception(), '__traceback__', None)))
            if exc:
                _run_job_error(job, exc, tb)
            else:
                _run_job_success(job, f.result())
        
        future = self._pool.submit(run_job, job)
        future.add_done_callback(callback)
        return future

def _run_job_error(job, exc, tb):
    print 'error---------'
    print  (job, exc, tb)

def _run_job_success(job, result):
    print 'success=========(job, result)'
    print (job, result)

class executor:
    pass

def main():
    s = submitter()
    future = s.submit(25)
    print "summited"
    
    print "sleeping"
    time.sleep(1)
    print "slept"
    
    if future.running():
        future.cancel()
        print "cancel"
        print future.cancelled()
    
    while(not future.done()):
        print 'waiting'
        time.sleep(1)
    
    print 'done'
    print future.result()
        
if __name__ == '__main__':
    main()
