# coding: utf-8

import windmill
from windmill.schedulers.background import BackgroundScheduler
from windmill.stores.mysqlstore import MysqlJobStore
from windmill.dispatchers.defaultimpl import DefaultDispather
from flask import Flask
from windmill.stores.historystore import HistoryStore
from windmill.jobruntime import JobRuntime, JobConf
import logging;
from runner.config import Config

def createjob(scheduler):
    conf = JobConf()
    conf.id = 1
    conf.cron_str = '*/5 * * * * * *'
    conf.cmd = 'ls -al .'
    conf.name = 'test'
    return conf

def createjob2(scheduler):
    conf = JobConf()
    conf.id = 1
    conf.cron_str = '*/5 * * * * * *'
    conf.cmd = 'ls -al .'
    conf.name = 'test'
    conf.next_run_time = 1452767330
    return conf

def createjob1(scheduler):
    conf = JobConf()
    conf.id = 1
    conf.cron_str = '*/5 * * * * * *'
    conf.cmd = 'ls -al .'
    conf.name = 'test'
    conf.next_run_time = 1452760330
    return conf

def createjob3(scheduler):
    conf = JobConf()
    conf.id = 1
    conf.cron_str = '*/5 * * * * * *'
    conf.cmd = 'ls -al .'
    conf.name = 'test'
    import datetime
    now = datetime.datetime.now(scheduler.timezone)
    conf.next_run_time = windmill.util.datetime_to_utc_timestamp(now)
    return conf


def test():
    print '===== start....'
    from windmill.schedulers.blocking import BlockingScheduler
    from datetime import datetime 
    scheduler = BlockingScheduler(Config())
    
    conf = createjob3(scheduler)
    job = JobRuntime(conf=conf, scheduler=scheduler) 
    job._setconf(conf)
    now = datetime.now(scheduler.timezone)
    print now
    print 'exact: ',  job._exactly_run_time(now)
    print 'late : ', job._lately_run_time(now)

    print '===== done.'
    
    
if __name__ == '__main__':
    test()