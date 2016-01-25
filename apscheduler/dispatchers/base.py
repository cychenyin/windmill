'''
Created on Dec 16, 2015

@author: MLS
'''
import six #, json, re
from abc import ABCMeta, abstractmethod

class MaxInstancesReachedError(Exception):
    def __init__(self, job, instance_count):
        super(MaxInstancesReachedError, self).__init__('Job "%s" has already reached its maximum number of instances (%d)' % (job.id, instance_count))

class DispatchCode(object):
    IGNORED = 'ignore'
    FAIL_TO_DISPATCH = 'fail_to_dispatch'
    DONE = 'done'
    
class BaseDispatcher(six.with_metaclass(ABCMeta)):
    '''
    dispatch job to executor 
    '''
    
    def __init__(self, scheduler, executor, zk_hosts='127.0.0.1:2181', zk_root='/windmill'):
        '''
        Constructor
        '''
        self._executor = executor;
        self._scheduler = scheduler
        self._lock = scheduler._create_lock()

    @abstractmethod
    def dispatch(self, job, run_times):
        return DispatchCode.DONE
    
    @abstractmethod
    def _run_job_success(self, job, run_time, events):
        pass
    
    @abstractmethod
    def _run_job_error(self, job, run_time, exception, traceback=None):
        pass
    
    @abstractmethod
    def start(self):
        pass
    
    @abstractmethod
    def shutdown(self):
        pass
    