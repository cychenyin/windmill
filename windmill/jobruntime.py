# coding: utf-8
from collections import Iterable, Mapping
from uuid import uuid4

import six
# from tzlocal import get_localzone

import windmill
from windmill.triggers.base import BaseTrigger
from windmill.triggers.cron import CronTrigger
from windmill.util import (ref_to_obj, obj_to_ref, datetime_repr, repr_escape, get_callable_name,
                              check_callable_args, convert_to_ware_datetime)
from windmill.jobconf import JobConf
import logging;

class JobRuntime(object):
    """
    Contains the options given when scheduling callables and its current schedule and other state.
    This class should never be instantiated by the user.K

    :var str id: the unique identifier of this job
    :var str name: the description of this job
    :var func: the callable to execute
    :var tuple|list args: positional arguments to the callable
    :var dict kwargs: keyword arguments to the callable
    :var bool coalesce: whether to only run the job once when several run times are due
    :var trigger: the trigger object that controls the schedule of this job
    :var str executor: the name of the executor that will run this job
    :var int misfire_grace_time: the time (in seconds) how much this job's execution is allowed to be late
    :var int max_instances: the maximum number of concurrently executing instances allowed for this job
    :var datetime.datetime next_run_time: the next scheduled run time of this job

    .. note::
        The ``misfire_grace_time`` has some non-obvious effects on job execution.
        See the :ref:`missed-job-executions` section in the documentation for an in-depth explanation.
    """

#     __slots__ = ('_scheduler', '_jobstore_alias', 'id', 'trigger', 'executor', 'func', 'func_ref', 'args', 'kwargs',
#                  'name', 'misfire_grace_time', 'coalesce', 'max_instances', 'next_run_time', 'conf')

    def __init__(self, scheduler, id=None, conf=None, **kwargs):
        super(JobRuntime, self).__init__()
        self._scheduler = scheduler
        self._jobstore_alias = 'default'
        self.conf = conf
        self.current_run_time = None
        self.need_update = False
        
        #self._modify(id=id or uuid4().hex, **kwargs)

    def modify(self, **changes):
        """
        Makes the given changes to this job and saves it in the associated job store.
        Accepted keyword arguments are the same as the variables on this class.

        .. seealso:: :meth:`~windmill.schedulers.base.BaseScheduler.modify_job`
        """

        self._scheduler.modify_job(self.id, self._jobstore_alias, **changes)

    def reschedule(self, trigger, **trigger_args):
        """
        Shortcut for switching the trigger on this job.

        .. seealso:: :meth:`~windmill.schedulers.base.BaseScheduler.reschedule_job`
        """

        self._scheduler.reschedule_job(self.id, self._jobstore_alias, trigger, **trigger_args)

    def pause(self):
        """
        Temporarily suspend the execution of this job.

        .. seealso:: :meth:`~windmill.schedulers.base.BaseScheduler.pause_job`
        """

        self._scheduler.pause_job(self.id, self._jobstore_alias)

    def resume(self):
        """
        Resume the schedule of this job if previously paused.

        .. seealso:: :meth:`~windmill.schedulers.base.BaseScheduler.resume_job`
        """

        self._scheduler.resume_job(self.id, self._jobstore_alias)

    def remove(self):
        """
        Unschedules this job and removes it from its associated job store.

        .. seealso:: :meth:`~windmill.schedulers.base.BaseScheduler.remove_job`
        """

        self._scheduler.remove_job(self.id, self._jobstore_alias)

    @property
    def pending(self):
        """Returns ``True`` if the referenced job is still waiting to be added to its designated job store."""

        return self._jobstore_alias is None

    #
    # Private API
    #
    
    # get next_run_time from conf.next_run_time simply
    def _exactly_run_time(self, now):
        self.current_run_time = self.trigger.get_next_fire_time(self.conf_next_run_time, now)
        return self.current_run_time
     
    
    # get lately next_run_time if there have run_times which need to be restore    
    def _lately_run_time(self, now):
        if not self.conf_next_run_time:
            return self.trigger.get_next_fire_time(self.conf_next_run_time, now)
        late = None
        next = self.trigger.get_next_fire_time(self.conf_next_run_time, now)
        while next < now:
            late = next
            next = self.trigger.get_next_fire_time(next, now)
        return late if late and late < now else self.conf_next_run_time 
            
#     def _get_run_times(self, now):
#         """
#         Computes the scheduled run times between ``next_run_time`` and ``now`` (inclusive).
# 
#         :type now: datetime.datetime
#         :rtype: list[datetime.datetime]
#         """
#         
#         run_times = []
#         # if next_run_time have not initialized, job will be commit from now on
#         next_run_time = self.next_run_time if self.next_run_time else now 
#         while next_run_time and next_run_time <= now:
#             run_times.append(next_run_time)
#             next_run_time = self.trigger.get_next_fire_time(next_run_time, now)
#             # logging.info("_get_run_times matched %d ===============next_run_time=%s now=%s" % (len(run_times), next_run_time, now))
#         
#         self.current_run_times = run_times        
#         return run_times

    def _modify(self, **changes):
        """Validates the changes to the Job and makes the modifications if and only if all of them validate."""

        approved = {}

        if 'id' in changes:
            value = changes.pop('id')
            if not isinstance(value, six.integer_types):
                raise TypeError("id must be a integer string")
            if hasattr(self, 'id'):
                raise ValueError('The job ID may not be changed')
            approved['id'] = value

        if 'func' in changes or 'args' in changes or 'kwargs' in changes:
            func = changes.pop('func') if 'func' in changes else self.func
            args = changes.pop('args') if 'args' in changes else self.args
            kwargs = changes.pop('kwargs') if 'kwargs' in changes else self.kwargs

            if isinstance(func, str):
                func_ref = func
                func = ref_to_obj(func)
            elif callable(func):
                try:
                    func_ref = obj_to_ref(func)
                except ValueError:
                    # If this happens, this Job won't be serializable
                    func_ref = None
            else:
                raise TypeError('func must be a callable or a textual reference to one')

            if not hasattr(self, 'name') and changes.get('name', None) is None:
                changes['name'] = get_callable_name(func)

            if isinstance(args, six.string_types) or not isinstance(args, Iterable):
                raise TypeError('args must be a non-string iterable')
            if isinstance(kwargs, six.string_types) or not isinstance(kwargs, Mapping):
                raise TypeError('kwargs must be a dict-like object')

            check_callable_args(func, args, kwargs)

            approved['func'] = func
            approved['func_ref'] = func_ref
            approved['args'] = args
            approved['kwargs'] = kwargs

        if 'name' in changes:
            value = changes.pop('name')
            if not value or not isinstance(value, six.string_types):
                raise TypeError("name must be a nonempty string")
            approved['name'] = value

        if 'misfire_grace_time' in changes:
            value = changes.pop('misfire_grace_time')
            if value is not None and (not isinstance(value, six.integer_types) or value <= 0):
                raise TypeError('misfire_grace_time must be either None or a positive integer')
            approved['misfire_grace_time'] = value

        if 'coalesce' in changes:
            value = bool(changes.pop('coalesce'))
            approved['coalesce'] = value

        if 'max_instances' in changes:
            value = changes.pop('max_instances')
            if not isinstance(value, six.integer_types) or value <= 0:
                raise TypeError('max_instances must be a positive integer')
            approved['max_instances'] = value

        if 'trigger' in changes:
            trigger = changes.pop('trigger')
            if not isinstance(trigger, BaseTrigger):
                raise TypeError('Expected a trigger instance, got %s instead' % trigger.__class__.__name__)

            approved['trigger'] = trigger

        if 'executor' in changes:
            value = changes.pop('executor')
            if not isinstance(value, six.string_types):
                raise TypeError('executor must be a string')
            approved['executor'] = value

        if 'next_run_time' in changes:
            value = changes.pop('next_run_time')
            approved['next_run_time'] = convert_to_ware_datetime(value, self._scheduler.timezone, 'next_run_time')

        if changes:
            raise AttributeError('The following are not modifiable attributes of Job: %s' % ', '.join(changes))
        
        # update job config info
        for key, value in six.iteritems(changes):
            if hasattr(self.conf, key):
                setattr(self.conf,key, value)
        
        # update approved runtime job info
        for key, value in six.iteritems(approved):
            setattr(self, key, value)

    def __getstate__(self):
        # Don't allow this Job to be serialized if the function reference could not be determined
        #if (not hasattr(self, 'conf') and hasattr(self, 'func_ref') and not self.func_ref )  or ( hasattr(self, 'conf') and not self.conf and not self.conf.cmd) :
        if (hasattr(self, 'func_ref') and not self.func_ref) and not self.conf.cmd:
            raise ValueError('This Job cannot be serialized since the reference to its callable (%r) could not be '
                             'determined. Consider giving a textual reference (module:function name) instead.' %
                             (self.func,))

        return {
            'version': 1,
            'id': self.id,
            'func': self.func_ref if hasattr(self, 'func_ref') else None,
            'trigger': self.trigger,
            'executor': self.executor,
            'args': self.args if hasattr(self, 'args') else None,
            'kwargs': self.kwargs if hasattr(self, 'kwargs') else None,
            'name': self.name,
            'misfire_grace_time': self.misfire_grace_time if hasattr(self, 'misfire_grace_time') else None,
            'coalesce': self.coalesce if hasattr(self, 'coalesce') else None,
            'max_instances': self.max_instances if hasattr(self, 'max_instances') else None,
            'next_run_time': self.next_run_time,
            'conf': self.conf,
        }
    def _create_trigger_by_conf(self):
        '''
            refer: http://crontab.org/
            day of week    0-7 (0 or 7 is Sun, or use names)
        '''
        if not self.conf or not self.conf.cron_str:
            raise ValueError("job.conf is None or job.conf.cron_str is None")
        
        ary = self.conf.cron_str.split(' ')
        second = None
        minute = None
        hour = None
        day_of_the_month = None
        month_of_the_year = None
        day_of_the_week = None
        year = None
        
        if len(ary) == 5: # classics
            minute, hour, day_of_the_month, month_of_the_year, day_of_the_week   = ary[0], ary[1], ary[2], ary[3], ary[4]
        else:
            if len(ary) == 6: # with year
                minute, hour, day_of_the_month, month_of_the_year, day_of_the_week, year = ary[0], ary[1], ary[2], ary[3], ary[4], ary[5]            
            else:
                if len(ary) == 7: # with second extended
                    second, minute, hour, day_of_the_month, month_of_the_year, day_of_the_week, year = ary[0], ary[1], ary[2], ary[3], ary[4], ary[5], ary[6]            
                else:
                    raise ValueError("job %s has sth. wrong with format of cron_str %s" % (self.id, self.conf.cron_str))
        
        trigger = CronTrigger(second=second, minute=minute, hour=hour, day=day_of_the_month, month=month_of_the_year, day_of_week=day_of_the_week, year=year
                              , start_date = windmill.util.convert_to_ware_datetime(windmill.util.local_timestamp_to_datetime(self.conf.start_date), self._scheduler.timezone, 'start_date') if self.conf.start_date  > 0 else None
                              , end_date = windmill.util.convert_to_ware_datetime(windmill.util.local_timestamp_to_datetime(self.conf.end_date), self._scheduler.timezone, 'end_date') if self.conf.end_date  > 0 else None
         )
#                               , start_date = windmill.util.convert_to_ware_datetime(windmill.util.local_timestamp_to_datetime(job.conf.start_date), self.timezone, 'start_date') if job.conf.start_date  > 0 else None
#                               , end_date = windmill.util.convert_to_ware_datetime(windmill.util.local_timestamp_to_datetime(job.conf.end_date), self.timezone, 'end_date') if job.conf.end_date  > 0 else None
        return trigger
            
    def _setconf(self, conf):
        self.id = conf.id
        self.name = conf.name
        self.conf = conf
        self.trigger = self._create_trigger_by_conf() 
        
        t = windmill.util.local_timestamp_to_datetime(conf.next_run_time) if conf.next_run_time > 0 else None
        self.conf_next_run_time = self.next_run_time = windmill.util.convert_to_ware_datetime(t, self._scheduler.timezone, 'conf.next_run_time' );
        
#     def __setstate__(self, state):
#         if state.get('version', 1) > 1:
#             raise ValueError('Job has version %s, but only version 1 can be handled' % state['version'])
# 
# #         if 'id' in state:
# #             self.id = state['id']
# 
# #         try:
# #             if 'func' in state and state['func']:
# #                 self.func_ref = state['func']
# #                 self.func = ref_to_obj(self.func_ref)
# #             if 'trigger' in state:
# #                 self.trigger = state['trigger']
# #             if 'executor' in state:
# #                 self.executor = state['executor']
# #             else:
# #                 self.executor = 'default'
# #             if 'args' in state:
# #                 self.args = state['args']
# #             if 'kwargs' in state:
# #                 self.kwargs = state['kwargs']
# #         except Exception,e:
# #             #self._scheduler._logger.info("reason: %s\n" % str(e))
# #             raise 
#         
#         if 'name' in state:
#             self.name = state['name']
#         if 'misfire_grace_time' in state:
#             self.misfire_grace_time = state['misfire_grace_time']
#         if 'coalesce' in state:
#             self.coalesce = state['coalesce']
#         if 'max_instances' in state:
#             self.max_instances = state['max_instances']
#         else:
#             self.max_instances = 1
#             
#         if 'next_run_time' in state:
#             self.next_run_time = state['next_run_time']
#         
#         if 'conf' in state:
#             self.conf = state['conf'] 

    def __eq__(self, other):
        if isinstance(other, JobRuntime):
            return self.id == other.id
        return NotImplemented

    def __repr__(self):
        return '<Job (id=%s name=%s)>' % (repr_escape(self.id), repr_escape(self.name))

    def __str__(self):
        return repr_escape(self.__unicode__())

    def __unicode__(self):
        if hasattr(self, 'next_run_time'):
            status = 'next run at: ' + datetime_repr(self.next_run_time) if self.next_run_time else 'paused'
        else:
            status = 'pending'

        #return six.u('%s (trigger: %s, %s)') % (self.name, self.trigger, status)
        return six.u('%s (trigger: %s, %s)') % (self.name, self.conf.cron_str, status)




