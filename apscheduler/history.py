# coding: utf-8
from __future__ import absolute_import

import logging
import socket
import six
from abc import ABCMeta
from apscheduler.util import maybe_ref,datetime_to_utc_timestamp # local_timestamp_to_datetime, 
# ,utc_timestamp_to_datetime
#from flask import current_app
from tzlocal import get_localzone
from datetime import datetime
from apscheduler.jobconf import JobConf
from runner.config import Config

try:
    from sqlalchemy import create_engine, Table, Column, MetaData, String, Integer
#     , Unicode, Float, LargeBinary, text
    from sqlalchemy import desc, select, and_
    from sqlalchemy.exc import IntegrityError
except ImportError:  # pragma: nocover
    raise ImportError('MysqlJobStore requires SQLAlchemy installed')

class HistoryStore(six.with_metaclass(ABCMeta)):
    """
    Stores jobs running history.

    Plugin alias: ``mysqljobstore``
    """

    def __init__(self, url=None, engine=None, metadata=None):
        super(HistoryStore, self).__init__()
        metadata = maybe_ref(metadata) or MetaData()

        url = url if url else Config.DATABASE_URL
        if engine:
            self.engine = maybe_ref(engine)
        elif url:
            self.engine = create_engine(url, encoding='utf-8', echo=False)
        else:
            raise ValueError('Need either "engine" or "url" defined')

        self.__ini_schema(metadata)
        
    def __ini_schema(self, metadata=None):
        tablename = 'wm_jobs_history'
        self.history_t = Table(
            tablename, metadata,
            Column('id', Integer, primary_key=True),
            Column('job_id', String(512), nullable=False, server_default='', index=True),
            Column('cmd', String(512), nullable=False, server_default=''),
            Column('host', String(100), nullable=False, server_default=''),
            Column('run_time', Integer, nullable=False, server_default='0'),
            Column('info_type', Integer, nullable=False, server_default='0'),  # 0, 框架相关， 不记录result, alarm; 1， job运行结果  2. 其他类型
            Column('output', String(8000), nullable=False, server_default=''),
            Column('result', Integer, nullable=False, server_default='0'),     # 0 success, 1, error
            Column('cost_ms', Integer, nullable=False, server_default='0'),
            Column('alarm_phones', String(500), nullable=False, server_default=''),
            Column('alarm_mails', String(500), nullable=False, server_default=''),
            Column('create_time', Integer, nullable=False, server_default='0', index=True),
            mysql_engine='InnoDB',
            mysql_charset='utf8',
        )
        self.history_t.create(self.engine, True)
    
    def get(self, job_id):
        selectable = select([ x for x in self.history_t.c]).where(self.history_t.c.id == job_id)
        row = self.engine.execute(selectable).one()
        return self._reconstitute_job(row) if row else None
    def __now(self):
        return int(datetime_to_utc_timestamp(datetime.now( get_localzone())))
    
    def __fix_val(self, val_dict):
        d = dict(val_dict)
        if 'create_time' not in d:
            d['create_time'] = self.__now()
        return d
    
    def createMessage(self, jobconf, run_time=0):
        d = {}
        d['job_id'] = jobconf.id
        d['cmd'] = jobconf.cmd
        d['host'] = socket.gethostname()
        d['run_time'] = run_time
        d['create_time'] = self.__now()
        
        return d
        
    def add(self, val_dict):
        d = self.__fix_val(val_dict)
        #insert = self.history_t.insert().values(**val_dict)
        insert = self.history_t.insert().values(d)
        try:
            self.engine.execute(insert)
        except IntegrityError:
            logging.error('history id exists %s', val_dict['id'])
            raise
    
    def look_up(self, job_id, run_time):
        s = select().where(and_(self.history_t.c.job_id == job_id, self.history_t.c.run_time == run_time ))
        return self.engine.execute(s).one_or_none()
        
#     def update(self, id_, **val_dict):
#         update = self.wm_jobs_t.update().values(**val_dict).where(self.history_t.c.id == id_)
#         result = self.engine.execute(update)
#         if result.rowcount == 0:
#             logging.error("fail to update history cause of id %s not found" % id)
            
    def remove_job(self, job_id, remain_rows = 100):
        select = self.history_t.select([self.history_t.c.id]).where(self.history_t.c.job_id == job_id).order_by(desc(self.history_t.c.id )).limit(1).offset(remain_rows + 1)
        one = self.engine.execute(select).one_or_none()
        if one:
            delete = self.history_t.delete().where(and_(self.history_t.c.id < one.id, self.history_t.c.job_id == job_id))
            self.engine.execute(delete)
    
    def shrink(self):
        s = select(self.history_t.c.job_id).distinct()
        for row in self.engine.execute(s).fetchall():
            self.remove_job(row.job_id)
        
    def get_all(self, page=0, size=20, job_id=None):
        selectable = select([x for x in self.history_t.c])
        if job_id:
            selectable = selectable.where(self.history_t.c.job_id == job_id)
        selectable = selectable.order_by(desc(self.history_t.c.create_time)).limit(size).offset(page * size)
        
        return self.engine.execute(selectable).fetchall()
    
    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.engine.url)


def add_log(jobconf, run_time=0, output=None, alarm_phones='', alarm_mails=''):
    store = HistoryStore()
    run_time = int(run_time)
    msg = store.createMessage(jobconf, run_time) 
    msg['info_type'] = 0
    if output:
        msg['output'] = output
    if alarm_phones:
        msg['alarm_phones'] = alarm_phones
    if alarm_mails:
        msg['alarm_mails'] = alarm_mails
    msg['result'] = 1
    
    store.add(msg)

def add_result(jobconf, run_time=0, result=1, output=None, cost_ms=-1, alarm_phones='', alarm_mails=''):
    store = HistoryStore()
    run_time = int(run_time)
    msg = store.createMessage(jobconf, run_time) 
    msg['info_type'] = 1
    if output:
        if len(output) > 8000:
            output = output[:7996] + '...'   
        msg['output'] = output
    if cost_ms > 0:
        msg['cost_ms'] = cost_ms
    if alarm_phones:
        msg['alarm_phones'] = alarm_phones
    if alarm_mails:
        msg['alarm_mails'] = alarm_mails
    msg['result'] = int(result)
    
    store.add(msg)
    
