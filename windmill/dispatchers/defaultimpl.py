# coding: utf-8
'''
Created on Dec 16, 2015

@author: MLS
'''
from collections import defaultdict
import logging
import json, re, socket
from windmill.dispatchers.base import BaseDispatcher,DispatchCode
from kazoo.client import KazooClient, KazooState
from datetime import datetime
from windmill.util import local_timestamp_to_datetime, datetime_to_utc_timestamp
from _mysql import result
# tz.localize(datetime.datetime.fromtimestamp(timestamp))

# import six
# from datetime import datetime, timedelta
# from pytz import utc
# from traceback import format_tb
# import os, sys, commands, subprocess
# from threading import Event
# from windmill.events import JobExecutionEvent, EVENT_SCHEDULER_START, EVENT_SCHEDULER_SHUTDOWN,EVENT_JOBSTORE_ADDED,EVENT_JOBSTORE_REMOVED, EVENT_JOB_ADDED, EVENT_JOB_REMOVED, EVENT_JOB_MODIFIED, EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

class DefaultDispather(BaseDispatcher):
    '''
    """Abstract base class that defines the interface that every dispatcher must implement."""
    '''
    _state = None

    def __init__(self, scheduler, executor, zk_hosts='127.0.0.1:2181', zk_root='/windmill'):
        super(DefaultDispather, self).__init__(scheduler, executor)
        self._state = defaultdict(lambda: [])
        self._zk_hosts = zk_hosts
        self._zk = KazooClient(hosts=zk_hosts)
        self._zk_root = zk_root
        self._local_hostname = socket.gethostname()
        
    def start(self):
        BaseDispatcher.start(self)
        self._zk.start()
        return self._zk.state == KazooState.CONNECTED
    
    def shutdown(self):
        BaseDispatcher.shutdown(self)
        self._state.clear()
        self._zk.stop()
    
    def dispatch(self, job, run_times):
        # none sense to run in repeat
        if len(run_times) > 1:
            run_times = run_times[-1:]
        run_time = run_times[0] 
        
        # allow hosts
        hosts = re.split('[，；,; \\r\\n]', job.conf.hosts) # 里面有两个中文字符， 2 chinese symbol included
        if self._local_hostname not in hosts:
            logging.error('jog ignored. local ip %s not in hosts %s' % (self._local_hostname, job.conf.hosts ))
            return DispatchCode.IGNORED
        
        #logging.error('run_time %s' % run_time)
        # run in just one of these hosts
        if job.conf.host_strategy == 1: 
            if not self._zk.client_state == KazooState.CONNECTED:
                msg = 'zk state is %s at host %s' % (self._zk.client_state, self._local_hostname )
                logging.error(msg)
                
                self._scheduler._history.add_log(job.conf, run_time=datetime_to_utc_timestamp(run_time), output=msg)
                return DispatchCode.FAIL_TO_DISPATCH
            
            else:    
                if self._zk.exists(self._get_job_running_path(job)):
                    data, stat = self._zk.get(self._get_job_running_path(job))
                    logging.info('job ignored cause of exist_strategy=1 and the last running still going on. id=%s name=%s run_time=%s host=%s zk data:%s' % (job.id, job.conf.name, run_time, self._local_hostname, data.decode("utf-8") ))
                    run_time = None
                    return DispatchCode.IGNORED
                else:
                    try:
                        #self._zk.ensure_path(self._zk_root)
                        self._zk.create(self._get_job_running_path(job), value=json.dumps({'job': job.id, 'host': self._local_hostname , 'run_time': run_time.strftime( self._scheduler.config['TIME_FORMAT'])}), ephemeral=True, makepath=True)
                    except Exception, e:
                        logging.info('job ignored cause of fail to create zk ephemeral node %s. id=%s name=%s run_time=%s host=%s msg: %s' % (self._get_job_running_path(job), job.id, job.conf.name, run_time, self._local_hostname, str(e) ))
                        run_time = None
                        return DispatchCode.FAIL_TO_DISPATCH
        
        # local instance
        with self._lock:            
            if job.conf.exist_strategy == 1 and len(self._state[job.id]) > 0:
                logging.info('job ignored cause of exist_strategy=1 and the last running in local host still going on. id=%s name=%s run_time=%s host=%s' % (job.id, job.conf.name, run_time, self._local_hostname ))
                run_time = None 
                    
        def callback(f):
            exc, tb = (f.exception_info() if hasattr(f, 'exception_info') else
                       (f.exception(), getattr(f.exception(), '__traceback__', None)))
            if exc:
                self._run_job_error(job, run_time, exc, tb)
            else:
                self._run_job_success(job, run_time, f.result())
            
            with self._lock:
                self._state[job.id].remove((f, run_time))
                
        #f = self._pool.submit(run_job, job, job._jobstore_alias, run_times, self._logger.name)
        #f = self._pool.submit(self.run_job, job, job._jobstore_alias, run_times, None)
        if run_time:
            with self._lock:
                logging.info('commiting ')
                f = self._executor.submit_job(job, run_time)
                self._state[job.id].append((f, run_time))
                f.add_done_callback(callback)
        return DispatchCode.DONE
        
    def _run_job_success(self, job, run_time, events):
        """Called by the dispatcher with the list of generated events when `run_job` has been successfully called."""
#         with self._lock:
#             #self._state[job.id].remove((f, run_time))
#             indices = [i for i, (f, rt) in enumerate(self._state[job.id]) if rt == run_time]
#             for i in indices:
#                 del self._state[job.id][i]
        
        cost_ms = int ((datetime_to_utc_timestamp(datetime.now( self._scheduler.timezone)) - datetime_to_utc_timestamp(run_time)) * 1000)  
        for event in events:
            self._scheduler._dispatch_event(event)
            
        self._scheduler._history.add_result(job.conf, datetime_to_utc_timestamp(run_time), result=0, output='ok', cost_ms=cost_ms)
 
    def _run_job_error(self, job, run_time, exc, traceback=None):
        """Called by the dispatcher with the exception if there is an error calling `run_job`."""
#         with self._lock:
#             indices = [i for i, (f, rt) in enumerate(self._state[job.id]) if rt == run_time]
#             for i in indices:
#                 del self._state[job.id][i]
 
        exc_info = (exc.__class__, exc, traceback)
        logging.error('Error running job %s run_time %s' % (job.id, run_time), exc_info=exc_info)
        
        cost_ms = int ((datetime_to_utc_timestamp(datetime.now( self._scheduler.timezone)) - datetime_to_utc_timestamp(run_time)) * 1000)
        self._scheduler._history.add_result(job.conf, datetime_to_utc_timestamp(run_time), result=1, output=exc_info, cost_ms=cost_ms)
        
    def _get_job_running_path(self, job):
        if job.conf.host_strategy:
            return self._zk_root + '/task/' + str(job.id) + '/current/once'
        else:
            return self._zk_root + '/task/' + str(job.id) + '/current/everyone/' + self._local_hostname + '/once'

    def _get_job_lock_path(self, job):
        return self._zk_root + '/task/' + str(job.id) + '/current/lock'
        