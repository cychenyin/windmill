# coding: utf-8
from __future__ import print_function
from abc import ABCMeta, abstractmethod
#from collections import MutableMapping
from threading import RLock
from datetime import datetime
from logging import getLogger
# from pkg_resources import iter_entry_points
from tzlocal import get_localzone
import six, sys, logging

from apscheduler.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError, SchedulerInitError
from apscheduler.dispatchers.base import MaxInstancesReachedError,  DispatchCode
from apscheduler.executors.base import BaseExecutor
from apscheduler.executors.pool import ThreadPoolExecutor 
from apscheduler.jobstores.base import ConflictingIdError, JobLookupError, BaseJobStore
from apscheduler.jobstores.mysqlstore import MysqlJobStore
from apscheduler.job import Job
# from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import asbool, asint, astimezone, maybe_ref, timedelta_seconds, undefined
from apscheduler.events import (
    SchedulerEvent, JobEvent, EVENT_SCHEDULER_START, EVENT_SCHEDULER_SHUTDOWN, EVENT_JOBSTORE_ADDED,
    EVENT_JOBSTORE_REMOVED, EVENT_ALL, EVENT_JOB_MODIFIED, EVENT_JOB_REMOVED, EVENT_JOB_ADDED, EVENT_EXECUTOR_ADDED,
    EVENT_EXECUTOR_REMOVED, EVENT_ALL_JOBS_REMOVED)
from _ast import alias
from tests.test_jobstores import jobstore


class BaseScheduler(six.with_metaclass(ABCMeta)):
    """
    Abstract base class for all schedulers. Takes the following keyword arguments:
    """
    
    _stopped = True

    #
    # Public API
    #

    def __init__(self, gconfig={}, **options):
        super(BaseScheduler, self).__init__()
        self._listeners = []
        self._listeners_lock = self._create_lock()
        self._pending_jobs = []

        self._executor_lock = self._create_lock()
        self._executor = None

        self._jobstore_lock = self._create_lock()
        self._jobstore = None
                
        self._dispatcher_lock = self._create_lock()
        self._dispatcher = None
        
        self.configure(gconfig, **options)

    def configure(self, gconfig={}, prefix='apscheduler.', **options):
        """
        Reconfigures the scheduler with the given options. Can only be done when the scheduler isn't running.

        :param dict gconfig: a "global" configuration dictionary whose values can be overridden by keyword arguments to
                             this method
        :param str|unicode prefix: pick only those keys from ``gconfig`` that are prefixed with this string
                                   (pass an empty string or ``None`` to use all keys)
        :raises SchedulerAlreadyRunningError: if the scheduler is already running
        """

        if self.running:
            raise SchedulerAlreadyRunningError

        # If a non-empty prefix was given, strip it from the keys in the global configuration dict
        if prefix:
            prefixlen = len(prefix)
            gconfig = dict((key[prefixlen:], value) for key, value in six.iteritems(gconfig) if key.startswith(prefix))

        # Create a structure from the dotted options (e.g. "a.b.c = d" -> {'a': {'b': {'c': 'd'}}})
        config = {}
        for key, value in six.iteritems(gconfig):
            parts = key.split('.')
            parent = config
            key = parts.pop(0)
            while parts:
                parent = parent.setdefault(key, {})
                key = parts.pop(0)
            parent[key] = value

        # Override any options with explicit keyword arguments
        config.update(options)
        self._configure(config)

    @abstractmethod
    def start(self):
        """
        Starts the scheduler. The details of this process depend on the implementation.

        :raises SchedulerAlreadyRunningError: if the scheduler is already running
        """

        if self.running:
            raise SchedulerAlreadyRunningError
        
        up = True
        with self._executor_lock:
            if self._executor:
                up =  up and self._executor.start(self, 'default')
                if not up:
                    logging.error('executor fail to start')
        
        with self._dispatcher_lock:
            if self._dispatcher:
                up =  up and self._dispatcher.start()
                if not up:
                    logging.error('_dispatcher fail to start')


        with self._jobstore_lock:
            if self._jobstore:
                up =  up and self._jobstore.start(self, 'default')
            if not up:
                logging.error('_jobstore fail to start')
            
            for job, jobstore_alias, replace_existing in self._pending_jobs:
                self._real_add_job(job, 'default', replace_existing, False)
                
            del self._pending_jobs[:]
        
        if up:
            self._stopped = False
            logging.info('Scheduler started')
            # Notify listeners that the scheduler has been started
            self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_START))
        else:
            raise SchedulerInitError()
        
    @abstractmethod
    def shutdown(self, wait=True):
        """
        Shuts down the scheduler. Does not interrupt any currently running jobs.

        :param bool wait: ``True`` to wait until all currently executing jobs have finished
        :raises SchedulerNotRunningError: if the scheduler has not been started yet
        """

        if not self.running:
            raise SchedulerNotRunningError

        self._stopped = True
        
        self._dispatcher.shut_down()
        self._executor.shutdown(wait)
        self._jobstore.shutdown()

        logging.info('Scheduler has been shut down')
        self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN))

    @property
    def running(self):
        return not self._stopped

    def add_executor(self, executor, alias='default', **executor_opts):
        """
        Adds an executor to this scheduler. Any extra keyword arguments will be passed to the executor plugin's
        constructor, assuming that the first argument is the name of an executor plugin.

        :param str|unicode|apscheduler.executors.base.BaseExecutor executor: either an executor instance or the name of
            an executor plugin
        :param str|unicode alias: alias for the scheduler
        :raises ValueError: if there is already an executor by the given alias
        """
        if not self._executor:
            with self._executor_lock:
                if isinstance(executor, BaseExecutor):
                    self._executor = executor
                elif isinstance(executor, six.string_types):
                    self._executor = executor = self._create_plugin_instance('executor', executor, executor_opts)
                else:
                    raise TypeError('Expected an executor instance or a string, got %s instead' %
                                    executor.__class__.__name__)

            # Start the executor right away if the scheduler is running
            if self.running:
                executor.start(self, 'default')

        self._dispatch_event(SchedulerEvent(EVENT_EXECUTOR_ADDED, alias))

    def remove_executor(self, alias, shutdown=True):
        """
        Removes the executor by the given alias from this scheduler.

        :param str|unicode alias: alias of the executor
        :param bool shutdown: ``True`` to shut down the executor after removing it
        """
        executor = self._executor 
        self._executor = None
        if shutdown and executor:
            executor.shutdown()
            self._dispatch_event(SchedulerEvent(EVENT_EXECUTOR_REMOVED, alias))

    def add_jobstore(self, jobstore, alias='default', **jobstore_opts):
        if not self._jobstore: 
            with self._jobstore_lock:
                if isinstance(jobstore, BaseJobStore):
                    self._jobstore = jobstore
                    logging.info("new jobstore alias=%s type=%s" % (alias, str(type(jobstore))))
                elif isinstance(jobstore, six.string_types):
                    self._jobstore = jobstore = self._create_plugin_instance('jobstore', jobstore, jobstore_opts)
                else:
                    raise TypeError('Expected a job store instance or a string, got %s instead' %
                                    jobstore.__class__.__name__)

            # Start the job store right away if the scheduler is running
            if self.running:
                jobstore.start(self, alias)
        else:
            logging.warn('jobstore exists, init over again')
            
        # Notify listeners that a new job store has been added
        self._dispatch_event(SchedulerEvent(EVENT_JOBSTORE_ADDED, alias))

        # Notify the scheduler so it can scan the new job store for jobs
        if self.running:
            self.wakeup()
        
    def remove_jobstore(self, alias, shutdown=True):
        """
        Removes the job store by the given alias from this scheduler.

        :param str|unicode alias: alias of the job store
        :param bool shutdown: ``True`` to shut down the job store after removing it
        """

        with self._jobstore_lock:
            jobstore = self._jobstore
            self._jobstore = None
            if shutdown and jobstore:
                jobstore.shutdown()
            self._dispatch_event(SchedulerEvent(EVENT_JOBSTORE_REMOVED, alias))

    def add_listener(self, callback, mask=EVENT_ALL):
        """
        add_listener(callback, mask=EVENT_ALL)

        Adds a listener for scheduler events. When a matching event occurs, ``callback`` is executed with the event
        object as its sole argument. If the ``mask`` parameter is not provided, the callback will receive events of all
        types.

        :param callback: any callable that takes one argument
        :param int mask: bitmask that indicates which events should be listened to

        .. seealso:: :mod:`apscheduler.events`
        .. seealso:: :ref:`scheduler-events`
        """

        with self._listeners_lock:
            self._listeners.append((callback, mask))

    def remove_listener(self, callback):
        """Removes a previously added event listener."""

        with self._listeners_lock:
            for i, (cb, _) in enumerate(self._listeners):
                if callback == cb:
                    del self._listeners[i]

    def add_job(self, func, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined,
                coalesce=undefined, max_instances=undefined, next_run_time=undefined, jobstore='default',
                executor='default', replace_existing=False, conf=None, **trigger_args):
        """
        add_job(func, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined, \
            coalesce=undefined, max_instances=undefined, next_run_time=undefined, jobstore='default', \
            executor='default', replace_existing=False, **trigger_args)

        Adds the given job to the job list and wakes up the scheduler if it's already running.

        Any option that defaults to ``undefined`` will be replaced with the corresponding default value when the job is
        scheduled (which happens when the scheduler is started, or immediately if the scheduler is already running).

        The ``func`` argument can be given either as a callable object or a textual reference in the
        ``package.module:some.object`` format, where the first half (separated by ``:``) is an importable module and the
        second half is a reference to the callable object, relative to the module.

        The ``trigger`` argument can either be:
          #. the alias name of the trigger (e.g. ``date``, ``interval`` or ``cron``), in which case any extra keyword
             arguments to this method are passed on to the trigger's constructor
          #. an instance of a trigger class

        :param func: callable (or a textual reference to one) to run at the given time
        :param str|apscheduler.triggers.base.BaseTrigger trigger: trigger that determines when ``func`` is called
        :param list|tuple args: list of positional arguments to call func with
        :param dict kwargs: dict of keyword arguments to call func with
        :param str|unicode id: explicit identifier for the job (for modifying it later)
        :param str|unicode name: textual description of the job
        :param int misfire_grace_time: seconds after the designated run time that the job is still allowed to be run
        :param bool coalesce: run once instead of many times if the scheduler determines that the job should be run more
                              than once in succession
        :param int max_instances: maximum number of concurrently running instances allowed for this job
        :param datetime next_run_time: when to first run the job, regardless of the trigger (pass ``None`` to add the
                                       job as paused)
        :param str|unicode jobstore: alias of the job store to store the job in
        :param str|unicode executor: alias of the executor to run the job with
        :param bool replace_existing: ``True`` to replace an existing job with the same ``id`` (but retain the
                                      number of runs from the existing one)
        :rtype: Job
        """
        job_kwargs = {
            'trigger': trigger,
            'executor': executor,
            'func': func,
            'args': tuple(args) if args is not None else (),
            'kwargs': dict(kwargs) if kwargs is not None else {},
            'id': id,
            'name': name,
            'misfire_grace_time': misfire_grace_time,
            'coalesce': coalesce,
            'max_instances': max_instances,
            'next_run_time': next_run_time
        }
        job_kwargs = dict((key, value) for key, value in six.iteritems(job_kwargs) if value is not undefined)
        job = Job(self, **job_kwargs)
        job.conf = conf
        
        # Don't really add jobs to job store before the scheduler is up and running
        with self._jobstore_lock:
            if not self.running:
                self._pending_jobs.append((job, jobstore, replace_existing))
                logging.info('Adding job tentatively -- it will be properly scheduled when the scheduler starts')
            else:
                logging.info('Adding job really, it will be start as soon as the settings. ')
                self._real_add_job(job, jobstore, replace_existing, True)

        return job

    def scheduled_job(self, trigger, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined,
                      coalesce=undefined, max_instances=undefined, next_run_time=undefined, jobstore='default',
                      executor='default', **trigger_args):
        """
        scheduled_job(trigger, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined, \
            coalesce=undefined, max_instances=undefined, next_run_time=undefined, jobstore='default', \
            executor='default',**trigger_args)

        A decorator version of :meth:`add_job`, except that ``replace_existing`` is always ``True``.

        .. important:: The ``id`` argument must be given if scheduling a job in a persistent job store. The scheduler
           cannot, however, enforce this requirement.
        """
        logging.info("scheduled_job called")
        
        def inner(func):
            self.add_job(func, trigger, args, kwargs, id, name, misfire_grace_time, coalesce, max_instances,
                         next_run_time, jobstore, executor, True, **trigger_args)
            return func
        return inner

    def modify_job(self, job_id, jobstore=None, **changes):
        """
        Modifies the properties of a single job. Modifications are passed to this method as extra keyword arguments.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        """
        
        self._dispatch_event(JobEvent(EVENT_JOB_MODIFIED, job_id, self._jobstore))

        # Wake up the scheduler since the job's next run time may have been changed
        self.wakeup()

    def reschedule_job(self, job_id, jobstore=None, trigger=None, **trigger_args):
        """
        Constructs a new trigger for a job and updates its next run time.
        Extra keyword arguments are passed directly to the trigger's constructor.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        :param trigger: alias of the trigger type or a trigger instance
        """
        jobstore = jobstore if jobstore else self._jobstore
        
        logging.info("rescheduled_job called")
        now = datetime.now(self.timezone)
        next_run_time = trigger.get_next_fire_time(None, now)
        self.modify_job(job_id, jobstore, trigger=trigger, next_run_time=next_run_time)

    def pause_job(self, job_id, jobstore=None):
        """
        Causes the given job not to be executed until it is explicitly resumed.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        """

        self.modify_job(job_id, jobstore, next_run_time=None)

    def resume_job(self, job_id, jobstore=None):
        """
        Resumes the schedule of the given job, or removes the job if its schedule is finished.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        """

        jobstore = jobstore if jobstore else self._jobstore
        with self._jobstore_lock:
            job, jobstore = self._lookup_job(job_id, jobstore)
            now = datetime.now(self.timezone)
            next_run_time = job.trigger.get_next_fire_time(None, now)
            if next_run_time:
                self.modify_job(job_id, jobstore, next_run_time=next_run_time)
            else:
                self.remove_job(job.id, jobstore)

    def get_jobs(self, jobstore=None, pending=None):
        """
        Returns a list of pending jobs (if the scheduler hasn't been started yet) and scheduled jobs, either from a
        specific job store or from all of them.

        :param str|unicode jobstore: alias of the job store
        :param bool pending: ``False`` to leave out pending jobs (jobs that are waiting for the scheduler start to be
                             added to their respective job store), ``True`` to only include pending jobs, anything else
                             to return both
        :rtype: list[Job]
        """
        logging.info("get_jobs called")
        jobstore = jobstore if jobstore else self._jobstore        
        with self._jobstore_lock:
            jobs = []
            if pending is not False:
                for job, alias, replace_existing in self._pending_jobs:
                    jobs.append(job)

            if pending is not True and jobstore:
                jobs.extend(jobstore.get_all_jobs())

            return jobs

    def get_job(self, job_id, jobstore=None):
        """
        Returns the Job that matches the given ``job_id``.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that most likely contains the job
        :return: the Job by the given ID, or ``None`` if it wasn't found
        :rtype: Job
        """
        jobstore = jobstore if jobstore else self._jobstore
        with self._jobstore_lock:
            try:
                return self._lookup_job(job_id, jobstore)[0]
            except JobLookupError:
                return

    def remove_job(self, job_id, jobstore=None):
        """
        Removes a job, preventing it from being run any more.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        :raises JobLookupError: if the job was not found
        """
        jobstore = jobstore if jobstore else self._jobstore
        with self._jobstore_lock:
            # Check if the job is among the pending jobs
            for i, (job, jobstore_alias, replace_existing) in enumerate(self._pending_jobs):
                if job.id == job_id:
                    del self._pending_jobs[i]
                    break
            else:
                # Otherwise, try to remove it from each store until it succeeds or we run out of store to check
                jobstore.remove_job(job_id)

        # Notify listeners that a job has been removed
        event = JobEvent(EVENT_JOB_REMOVED, job_id, jobstore)
        self._dispatch_event(event)

        logging.info('Removed job %s', job_id)

    def remove_all_jobs(self, jobstore=None):
        """
        Removes all jobs from the specified job store, or all job store if none is given.

        :param str|unicode jobstore: alias of the job store
        """

        with self._jobstore_lock:
            self._pending_jobs = []
            self._jobstore.remove_all_jobs()

        self._dispatch_event(SchedulerEvent(EVENT_ALL_JOBS_REMOVED, jobstore))

    def print_jobs(self, jobstore=None, out=None):
        """
        print_jobs(jobstore=None, out=sys.stdout)

        Prints out a textual listing of all jobs currently scheduled on either all job store or just a specific one.

        :param str|unicode jobstore: alias of the job store, ``None`` to list jobs from all store
        :param file out: a file-like object to print to (defaults to **sys.stdout** if nothing is given)
        """

        out = out or sys.stdout
        with self._jobstore_lock:
            if self._pending_jobs:
                print(six.u('Pending jobs:'), file=out)
                for job, jobstore_alias, replace_existing in self._pending_jobs:
                    print(six.u('    %s') % job, file=out)

            print(six.u('Jobstore %s:') % alias, file=out)
            jobs = self._jobstore.get_all_jobs()
            if jobs:
                for job in jobs:
                    print(six.u('    %s') % job, file=out)
            else:
                print(six.u('    No scheduled jobs'), file=out)

    @abstractmethod
    def wakeup(self):
        """
        Notifies the scheduler that there may be jobs due for execution.
        Triggers :meth:`_process_jobs` to be run in an implementation specific manner.
        """

    #
    # Private API
    #

    def _configure(self, config):
        # Set general options
        logging = maybe_ref(config.pop('logger', None)) or getLogger('apscheduler.scheduler')
#         handler = logging.handlers.RotatingFileHandler('/opt/logs/windmill/app.log', maxBytes=104857600, backupCount=10, encoding='utf_8')
#         handler.setLevel(logging.INFO)
#         handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(name)s:%(lineno)d]'))
#         logging.addHandler(handler)
        
        #self.timezone = astimezone(config.pop('timezone', None)) or get_localzone()
        self.timezone = get_localzone()

        # Set the job defaults
        job_defaults = config.get('job_defaults', {})
        self._job_defaults = {
            'misfire_grace_time': asint(job_defaults.get('misfire_grace_time', 1)),
            'coalesce': asbool(job_defaults.get('coalesce', True)),
            'max_instances': asint(job_defaults.get('max_instances', 1))
        }


    def _create_default_executor(self):
        """Creates a default executor store, specific to the particular scheduler type."""
        return ThreadPoolExecutor()

    def _create_default_jobstore(self):
        """Creates a default job store, specific to the particular scheduler type."""
        return MysqlJobStore()
    
    def _lookup_job(self, job_id, jobstore=None):
        """
        Finds a job by its ID.

        :type job_id: str
        :param str jobstore_alias: alias of a job store to look in
        :return tuple[Job, str]: a tuple of job, jobstore alias (jobstore alias is None in case of a pending job)
        :raises JobLookupError: if no job by the given ID is found.
        """
        store = jobstore or self._jobstore
        
        # Check if the job is among the pending jobs
        for job, alias, replace_existing in self._pending_jobs:
            if job.id == job_id:
                return job, None

        # Look in all job store
        job = store.lookup_job(job_id)
        if job is not None:
            return job, 'default'

        raise JobLookupError(job_id)

    def _dispatch_event(self, event):
        """
        Dispatches the given event to interested listeners.

        :param SchedulerEvent event: the event to send
        """

        with self._listeners_lock:
            listeners = tuple(self._listeners)

        for cb, mask in listeners:
            if event.code & mask:
                try:
                    cb(event)
                except:
                    logging.exception('Error notifying listener')

    def _real_add_job(self, job, jobstore_alias, replace_existing, wakeup):
        """
        :param Job job: the job to add
        :param bool replace_existing: ``True`` to use update_job() in case the job already exists in the store
        :param bool wakeup: ``True`` to wake up the scheduler after adding the job
        """
        # Fill in undefined values with defaults
        replacements = {}
        for key, value in six.iteritems(self._job_defaults):
            if not hasattr(job, key):
                replacements[key] = value

        # Calculate the next run time if there is none defined
        if not hasattr(job, 'next_run_time'):
            now = datetime.now(self.timezone)
            replacements['next_run_time'] = job.trigger.get_next_fire_time(None, now)

        # Apply any replacements
        job._modify(**replacements)

        # Add the job to the given job store
        try:
            self._jobstore.add_job(job)
        except ConflictingIdError:
            if replace_existing:
                print("fail to add job into store cause of exists, try to replace it then.")
                logging.error("fail to add job into store cause of exists, try to replace it then.")
                self._jobstore.update_job(job)
            else:
                raise

        # Mark the job as no longer pending
        job._jobstore_alias = jobstore_alias

        # Notify listeners that a new job has been added
        event = JobEvent(EVENT_JOB_ADDED, job.id, jobstore_alias)
        self._dispatch_event(event)

        logging.info('Added job "%s" to job store "%s"', job.name, jobstore_alias)

        # Notify the scheduler about the new job
        if wakeup:
            self.wakeup()

    def _create_plugin_instance(self, type_, alias, constructor_kwargs):
        """Creates an instance of the given plugin type, loading the plugin first if necessary."""
        return type_(**constructor_kwargs)
    
    def _create_trigger(self, trigger, trigger_args):
        return trigger(**trigger_args)

    def _create_lock(self):
        """Creates a reentrant lock object."""

        return RLock()

    def _process_jobs(self):
        """
        Iterates through jobs in every jobstore, starts jobs that are due and figures out how long to wait for the next
        round.
        """
        if not self._executor:
            logging.error('_executor init error') 
            raise SchedulerInitError() 

        if not self._dispatcher:
            logging.error('_dispatcher init error') 
            raise SchedulerInitError() 

        if not self._jobstore:
            logging.error('_jobstore init error') 
            raise SchedulerInitError() 
        
        logging.debug('Looking for jobs to run')
        now = datetime.now(self.timezone)
        
        next_wakeup_time = None
        with self._jobstore_lock:
                for job in self._jobstore.get_due_jobs(now):
                    run_times = job._get_run_times(now)
                    
                    #run_times = run_times[-1:] if run_times and job.coalesce else run_times
                    # 0, restore just one time. 1. restore for every lack
                    run_times = run_times[-1:] if len(run_times) > 1 and job.conf.restore_strategy == 0 else run_times 
                    #logging.info('run_times after store_strategy processed: %s %s' % (run_times, job.conf.restore_strategy))
                    
                    if run_times:
                        code = None
                        try:
                            logging.info("dispatch_job jobs %s cmd=%s run_time=%s" % (job.id, job.conf.cmd, run_times[-1]) )
                            code = self._dispatcher.dispatch(job, run_times)
                        except MaxInstancesReachedError:
                            logging.warning(
                                'Execution of job "%s" skipped: maximum number of running instances reached (%d)',
                                job, job.max_instances)
                        except Exception, e:
                            logging.exception('Error dispatch job "%s" to dispatcher "%s"', job.id, str(e))
                        
                        if code == DispatchCode.DONE:
                            # Update the job if it has a next execution time. Otherwise remove it from the job store.
                            job_next_run = job.trigger.get_next_fire_time(run_times[-1], now)
                            #print('jjjjjjjjjjjjjjjjjjj job_id = %s job_next_run=%s previous_fire_time=%s, now=%s' % (job.id, job_next_run, run_times[-1], now))
                            
                            if job_next_run:
                                job._modify(next_run_time=job_next_run)
                                
                                self._jobstore.update_job(job)
                            else:
                                self._jobstore.remove_job(job.id)
                                from apscheduler.history import add_log
                                add_log(job.conf, output='job will NOT be run any more, so remove it.')
                                logging.warn('job will NOT be run any more, so remove it. job_id=%s' % job.conf.id)

                # Set a new next wakeup time if there isn't one yet or the jobstore has an even earlier one
                jobstore_next_run_time = self._jobstore.get_next_run_time()
                logging.debug("jobstore_next_run_time %s " %  jobstore_next_run_time)
                if jobstore_next_run_time and (next_wakeup_time is None or jobstore_next_run_time < next_wakeup_time):
                    next_wakeup_time = jobstore_next_run_time

        # Determine the delay until this method should be called again
        if next_wakeup_time is not None:
            wait_seconds = max(timedelta_seconds(next_wakeup_time - now), 0)
            logging.debug('Next wakeup is due at %s (in %f seconds)', next_wakeup_time, wait_seconds)
        else:
            wait_seconds = None
            logging.debug('No jobs; waiting until a job is added')

        return wait_seconds
