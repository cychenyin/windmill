# coding: utf-8
from __future__ import absolute_import

import logging
from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, local_timestamp_to_datetime
# utc_timestamp_to_datetime
from apscheduler.triggers.cron import CronTrigger
from apscheduler.job import Job
from apscheduler.jobconf import JobConf
import apscheduler
from tzlocal import get_localzone
from datetime import datetime

#from .cmdjob import CmdJob
# from .runtimejob import RuntimeJob


try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from sqlalchemy import create_engine, Table, Column, MetaData, Unicode, String, Integer, Float, LargeBinary, select, text
    from sqlalchemy.exc import IntegrityError
except ImportError:  # pragma: nocover
    raise ImportError('MysqlJobStore requires SQLAlchemy installed')

class MysqlJobStore(BaseJobStore):
    """
    Stores jobs in a database table using SQLAlchemy. The table will be created if it doesn't exist in the database.

    Plugin alias: ``mysqljobstore``

    :param str url: connection string (see `SQLAlchemy documentation
                    <http://docs.sqlalchemy.org/en/latest/core/engines.html?highlight=create_engine#database-urls>`_
                    on this)
    :param engine: an SQLAlchemy Engine to use instead of creating a new one based on ``url``
    :param str tablename: name of the table to store jobs in
    :param metadata: a :class:`~mysqljobstore.MetaData` instance to use instead of creating a new one
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the highest available
    """

    def __init__(self, url=None, engine=None, tablename='apscheduler_jobs', metadata=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL):
        super(MysqlJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol
        metadata = maybe_ref(metadata) or MetaData()
        self.timezone = get_localzone()
        if engine:
            self.engine = maybe_ref(engine)
        elif url:
            self.engine = create_engine(url, encoding='utf-8', echo=False)
        else:
            raise ValueError('Need either "engine" or "url" defined')
                
        # 191 = max key length in MySQL for InnoDB/utf8mb4 tables, 25 = precision that translates to an 8-byte float
#         self.jobs_t = Table(
#             tablename, metadata,
#             Column('id', Unicode(191, _warn_on_bytestring=False), primary_key=True),
#             Column('next_run_time', Float(25), index=True),
#             Column('job_state', LargeBinary, nullable=False)
#         )

#         self.jobs_t.create(self.engine, True)
    
        self.__ini_schema(metadata)
        
    def __ini_schema(self, metadata=None):
        tablename = 'wm_jobs'
        self.wm_jobs_t = Table(
            tablename, metadata,
            Column('id', Integer, primary_key=True),
            Column('cmd', String(512), nullable=False, server_default=''),
            Column('cron_str', String(512), nullable=False, server_default=''),
            Column('name', String(50), nullable=False, server_default='', index=True),
            Column('desc', String(1000), nullable=False, server_default=''),
            Column('mails', String(200), nullable=False, server_default=''),
            Column('phones', String(200), nullable=False, server_default=''),
            Column('team', String(50), nullable=False, server_default=''),
            Column('owner', String(50), nullable=False, server_default='', index=True),
            Column('hosts', String(1000), nullable=False, server_default=''),
            Column('host_strategy', Integer, nullable=False, server_default='0', ),  # 0, every host; 1, one of these 
            Column('restore_strategy', Integer, nullable=False, server_default='0'), # 0, restore just one time. 1. restore for every lack 
            Column('retry_strategy', Integer, nullable=False, server_default='0'),   # 0, no retry; N>0, retry N times 
            Column('error_strategy', Integer, nullable=False, server_default='0'),   # 0, do nothing; 1, stop job & alarm through both mail & phones
            Column('exist_strategy', Integer, nullable=False, server_default='0'), # 0, whatever; 1, skip this period; 2, wait for stop until end of this period 
            Column('running_timeout_s', Integer, nullable=False, server_default='0'), # 0, without endless 1, kill job when timeout
            Column('status', Integer, nullable=False, server_default='0', index=True), # 0, stopped or new; 1, running normally; 2. suspend by runtime  
            Column('modify_time', Integer, nullable=False, server_default='0'),
            Column('modify_user', String(50), nullable=False, server_default=''),
            Column('create_time', Integer, nullable=False, server_default='0'),
            Column('create_user', String(50), nullable=False, server_default=''),
            Column('start_date', Integer, nullable=False, server_default='0', index=True),
            Column('end_date', Integer, nullable=False, server_default='0', index=True),
            Column('oupput_match_reg', String(100), nullable=False, server_default=''),  # do nothing when empty, otherwise, alarm when match Regex expression specified         
            Column('next_run_time', Float(25), nullable=False, server_default='0', index=True),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )
        # by the way, next_run_time / and last_run_time / and running status should be saved in zookeeper 
        self.wm_jobs_t.create(self.engine, True)
    
    def start(self,scheduler, alias):
        
        return self.engine and BaseJobStore.start(self, scheduler, alias) 
        
#     def lookup_job(self, job_id):
#         selectable = select([self.jobs_t.c.job_state]).where(self.jobs_t.c.id == job_id)
#         job_state = self.engine.execute(selectable).scalar()
#         return self._reconstitute_job(job_state) if job_state else None
    def lookup_job(self, job_id):
        selectable = select([ x for x in self.wm_jobs_t.c]).where(self.wm_jobs_t.c.id == job_id)
        row = self.engine.execute(selectable).one()
        return self._reconstitute_job(row) if row else None
        
#     def get_due_jobs(self, now):
#         timestamp = datetime_to_utc_timestamp(now)
#         return self._get_jobs(self.jobs_t.c.next_run_time <= timestamp)
    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs(self.wm_jobs_t.c.next_run_time <= timestamp)
        
#     def get_next_run_time(self):
#         selectable = select([self.jobs_t.c.next_run_time]).where(self.jobs_t.c.next_run_time != None).\
#             order_by(self.jobs_t.c.next_run_time).limit(1)
#         next_run_time = self.engine.execute(selectable).scalar()
#         return utc_timestamp_to_datetime(next_run_time)
    def get_next_run_time(self):
        # TDODO ... should access zookeeper to get the next_run_time
        selectable = select([self.wm_jobs_t.c.next_run_time]).where(self.wm_jobs_t.c.next_run_time != None).\
            order_by(self.wm_jobs_t.c.next_run_time).limit(1)
        next_run_time = self.engine.execute(selectable).scalar()
        ret = local_timestamp_to_datetime(next_run_time)
        return ret 

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

#     def add_job(self, job):
#         insert = self.jobs_t.insert().values(**{
#             'id': job.id,
#             'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
#             'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)
#         })
#         try:
#             self.engine.execute(insert)
#         except IntegrityError:
#             raise ConflictingIdError(job.id)
    def add_job(self, job):
        insert = self.wm_jobs_t.insert().values(**{
            'id': job.conf.id,
            'cmd': job.conf.cmd,
            'cron_str': job.conf.cron_str,
            'name': job.conf.name,
            'desc': job.conf.desc,
            'mails': job.conf.mails,
            'phones': job.conf.phones,
            'team': job.conf.team,
            'owner': job.conf.owner,
            'hosts': job.conf.hosts,
            'host_strategy': job.conf.host_strategy,
            'restore_strategy': job.conf.restore_strategy,
            'retry_strategy': job.conf.retry_strategy,
            'error_strategy': job.conf.error_strategy,
            'exist_strategy': job.conf.exist_strategy,
            'running_timeout_s': job.conf.running_timeout_s,
            'status': job.conf.status,
            'modify_time': job.conf.modify_time,
            'modify_user': job.conf.modify_user,
            'create_time': job.conf.create_time,
            'create_user': job.conf.create_user,
            'start_date': job.conf.start_date,
            'end_date': job.conf.end_date,
            'oupput_match_reg': job.conf.oupput_match_reg,
            'next_run_time': job.conf.next_run_time,
            
        })
        try:
            self.engine.execute(insert)
        except IntegrityError:
            raise ConflictingIdError(job.id)

#     def update_job(self, job):
#         update = self.jobs_t.update().values(**{
#             'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
#             'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)
#         }).where(self.jobs_t.c.id == job.id)
#         result = self.engine.execute(update)
#         if result.rowcount == 0:
#             raise JobLookupError(id)
    def update_job(self, job):
        job.conf.next_run_time = datetime_to_utc_timestamp(job.next_run_time)
        logging.debug('job %s update next_run_time to %s %s cmd=%s' % (job.conf.id, job.conf.next_run_time, job.next_run_time, job.conf.cmd))
        
        update = self.wm_jobs_t.update().values(**{
            'cmd': job.conf.cmd,
            'cron_str': job.conf.cron_str,
            'name': job.conf.name,
            'desc': job.conf.desc,
            'mails': job.conf.mails,
            'phones': job.conf.phones,
            'team': job.conf.team,
            'owner': job.conf.owner,
            'hosts': job.conf.hosts,
            'host_strategy': job.conf.host_strategy,
            'restore_strategy': job.conf.restore_strategy,
            'retry_strategy': job.conf.retry_strategy,
            'error_strategy': job.conf.error_strategy,
            'exist_strategy': job.conf.exist_strategy,
            'running_timeout_s': job.conf.running_timeout_s,
            'status': job.conf.status,
            'modify_time': job.conf.modify_time,
            'modify_user': job.conf.modify_user,
            'create_time': job.conf.create_time,
            'create_user': job.conf.create_user,
            'start_date': job.conf.start_date,
            'end_date': job.conf.end_date,
            'oupput_match_reg': job.conf.oupput_match_reg,
            'next_run_time': job.conf.next_run_time
        }).where(self.wm_jobs_t.c.id == job.id)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise JobLookupError(id)

#     def remove_job(self, job_id):
#         delete = self.jobs_t.delete().where(self.jobs_t.c.id == job_id)
#         result = self.engine.execute(delete)
#         if result.rowcount == 0:
#             raise JobLookupError(job_id)
    def remove_job(self, job_id):
        #delete = self.wm_jobs_t.delete().where(self.wm_jobs_t.c.id == job_id)
        update = self.wm_jobs_t.update().where(self.wm_jobs_t.c.id == job_id).values(status='2')
        # TODO ... add history here, operation cause of job should be stopped as scheduled settings.
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise JobLookupError(job_id)
    
#     def remove_all_jobs(self):
#         delete = self.jobs_t.delete()
#         self.engine.execute(delete)
    def remove_all_jobs(self):
        delete = self.wm_jobs_t.delete()
        self.engine.execute(delete)

    def shutdown(self):
        self.engine.dispose()

#     def _reconstitute_job(self, job_state):
#         job_state = pickle.loads(job_state)
#         job_state['jobstore'] = self
#         job = Job.__new__(Job)
#         job.__setstate__(job_state)
#         job._scheduler = self._scheduler
#         job._jobstore_alias = self._alias
#         return job
    def _reconstitute_job(self, row):
        '''
            code gen by shell cmd: cat a | awk -F '=' '{print $1}' | cut -c5- | awk '{ print "job."$1" = row."$1}'
            what in file a is the wm_jobs_t create statement which can be found in the current source code file
        '''
                
        conf = JobConf()
        conf.id = row.id
        conf.cmd = row.cmd
        conf.cron_str = row.cron_str
        conf.name = row.name
        conf.desc = row.desc
        conf.mails = row.mails
        conf.phones = row.phones
        conf.team = row.team
        conf.owner = row.owner
        conf.hosts = row.hosts
        conf.host_strategy = row.host_strategy
        conf.restore_strategy = row.restore_strategy
        conf.retry_strategy = row.retry_strategy
        conf.error_strategy = row.error_strategy
        conf.exist_strategy = row.exist_strategy
        conf.running_timeout_s = row.running_timeout_s
        conf.status = row.status
        conf.modify_time = row.modify_time
        conf.modify_user = row.modify_user
        conf.create_time = row.create_time
        conf.create_user = row.create_user
        conf.start_date = row.start_date
        conf.end_date = row.end_date
        conf.oupput_match_reg = row.oupput_match_reg 
        conf.next_run_time = row.next_run_time 
        
        job = Job.__new__(Job)
        job.conf = conf
        job.id = job.conf.id
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        job.trigger = self._create_trigger_by_conf(job) 
        t = apscheduler.util.local_timestamp_to_datetime(conf.next_run_time) if conf.next_run_time > 0 else None
        t = apscheduler.util.convert_to_ware_datetime(t, get_localzone(), 'conf.next_run_time' )
        state = {
             'version': 1,
             'conf': conf, 
             'id': conf.id, 
             'name': conf.name, 
             'next_run_time': t, 
        }
        
        job.__setstate__(state)
        
        return job
    
    def _create_trigger_by_conf(self, job):
        '''
            refer: http://crontab.org/
            day of week    0-7 (0 or 7 is Sun, or use names)
        '''
        if not job.conf or not job.conf.cron_str:
            raise ValueError("job.conf is None or job.conf.cron_str is None")
        
        ary = job.conf.cron_str.split(' ')
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
                              , start_date = apscheduler.util.convert_to_ware_datetime(apscheduler.util.local_timestamp_to_datetime(job.conf.start_date), self.timezone, 'start_date') if job.conf.start_date  > 0 else None
                              , end_date = apscheduler.util.convert_to_ware_datetime(apscheduler.util.local_timestamp_to_datetime(job.conf.end_date), self.timezone, 'end_date') if job.conf.end_date  > 0 else None
         )
#                               , start_date = apscheduler.util.convert_to_ware_datetime(apscheduler.util.local_timestamp_to_datetime(job.conf.start_date), self.timezone, 'start_date') if job.conf.start_date  > 0 else None
#                               , end_date = apscheduler.util.convert_to_ware_datetime(apscheduler.util.local_timestamp_to_datetime(job.conf.end_date), self.timezone, 'end_date') if job.conf.end_date  > 0 else None
        return trigger
            
#     def _get_jobs(self, *conditions):
#         jobs = []
#         selectable = select([self.jobs_t.c.id, self.jobs_t.c.job_state]).order_by(self.jobs_t.c.next_run_time)
#         selectable = selectable.where(*conditions) if conditions else selectable
#         failed_job_ids = set()
#         for row in self.engine.execute(selectable):
#             try:
#                 jobs.append(self._reconstitute_job(row.job_state))
#             except:
#                 logging.exception('Unable to restore job "%s" -- removing it', row.id)
#                 failed_job_ids.add(row.id)
# 
#         # Remove all the jobs we failed to restore
#         if failed_job_ids:
#             delete = self.jobs_t.delete().where(self.jobs_t.c.id.in_(failed_job_ids))
#             self.engine.execute(delete)
# 
#         return jobs
    def _get_jobs(self, *conditions):
        jobs = []
        selectable = select([x for x in self.wm_jobs_t.c])
#       selectable = selectable.order_by(self.wm_jobs_t.c.next_run_time)
        selectable = selectable.where(*conditions).where(self.wm_jobs_t.c.status == 1) if conditions else selectable
        failed_job_ids = set()
        for row in self.engine.execute(selectable):
            try:
                jobs.append(self._reconstitute_job(row))
            except:
                logging.exception('Unable to restore job "%s" -- removing it', row.id)
                failed_job_ids.add(row.id)

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            # delete = self.jobs_t.delete().where(self.jobs_t.c.id.in_(failed_job_ids))
            # logic delete
            msg = 'job %s update status to 2 cause of failing to _reconstitute_job' % ','.join(list(failed_job_ids))
            logging.error(msg)
            update = self.wm_jobs_t.update().where(self.wm_jobs_t.c.id.in_(failed_job_ids)).values(status='2')
            self.engine.execute(update)
            
            # TODO ... add history here
            from apscheduler.history import add_log
            conf = JobConf()
            conf.id = 0
            conf.cmd = ' '
            add_log(conf, output=msg)

        return jobs    
    
    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.engine.url)
