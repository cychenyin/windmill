# coding: utf-8

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from traceback import format_tb
import logging
import sys
import commands
import subprocess
from pytz import utc
import six

from apscheduler.events import JobExecutionEvent, EVENT_JOB_MISSED, EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

class BaseExecutor(six.with_metaclass(ABCMeta, object)):
    """Abstract base class that defines the interface that every executor must implement."""

    _scheduler = None
    _lock = None
#     _logger = logging.getLogger('apscheduler.executors')

    def __init__(self):
        super(BaseExecutor, self).__init__()

    def start(self, scheduler, alias):
        """
        Called by the scheduler when the scheduler is being started or when the executor is being added to an already
        running scheduler.

        :param apscheduler.schedulers.base.BaseScheduler scheduler: the scheduler that is starting this executor
        :param str|unicode alias: alias of this executor as it was assigned to the scheduler
        """

        self._scheduler = scheduler
        self._lock = scheduler._create_lock()
        return True

    def shutdown(self, wait=True):
        """
        Shuts down this executor.

        :param bool wait: ``True`` to wait until all submitted jobs have been executed
        """

    def submit_job(self, job, run_time):
        """
        Submits job for execution.

        :param Job job: job to execute
        :param datetime, run_time: datetime specifying when the job should have been run
        """

        assert self._lock is not None, 'This executor has not been started yet'
        with self._lock:
            future = self._do_submit_job(job, run_time)
        
        return future
        
    @abstractmethod
    def _do_submit_job(self, job, run_times):
        """Performs the actual task of scheduling `run_job` to be called."""
 
def run_job(job, run_time):
    """Called by executors to run the job. Returns a list of scheduler events to be dispatched by the scheduler."""
 
    events = []
    try:
        #retval = job.func(*job.args, **job.kwargs)
        #buf size采用默认系统缓冲(一般是全缓冲)
        #subproc = subprocess.Popen(job.conf.cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=-1, universal_newlines=True)  
        retval = commands.getstatusoutput(job.conf.cmd)
    except:
        exc, tb = sys.exc_info()[1:]
        formatted_tb = ''.join(format_tb(tb))
        events.append(JobExecutionEvent(EVENT_JOB_ERROR, job.id, run_time, exception=exc,
                                        traceback=formatted_tb))
        logging.exception('Raised an exception, job %s (scheduled at %s)' % (job, run_time))
    else:
        events.append(JobExecutionEvent(EVENT_JOB_EXECUTED, job.id, run_time, retval=retval))
        logging.info('Executed successfully job "%s" (scheduled at %s) result: %s' % (job, run_time, retval))
 
    return events
