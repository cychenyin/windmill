# coding: utf-8
'''
Created on Dec 1, 2015

@author: MLS
'''
import logging

from datetime import date, timedelta
from logging.handlers import RotatingFileHandler
from tzlocal import get_localzone

class Config(object):
    
    LOGGER_NAME = 'windmill'
    LOG_LEVEL = logging.INFO
    LOG_FORMATER = logging.Formatter('%(asctime)s %(levelname)7s: %(message)s [in %(pathname)s:%(lineno)d] {%(process)d-%(processName)s-%(thread)d-%(threadName)s}')
    LOG_HANDLER = logging.handlers.RotatingFileHandler('/opt/logs/windmill/app.log', maxBytes=104857600, backupCount=10, encoding='utf_8')
    LOG_HANDLERS = [LOG_HANDLER, logging.StreamHandler() ]

    ADMIN = 'admin@server.com'
    ADMIN_PHONES = '13800000000'
    ALARM_MAIL = 'user@serer.com' 
    ALARM_PASSWD = 'passwd'
    
    
    DEBUG = True
    CSRF_ENABLED = True
    THREADS_PER_PAGE = 8
    #os.urandom(24)
    SECRET_KEY = 'k^S%1*2s3W*4*d6$≈1#w@2'
    TIMEZONE = get_localzone() 
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    PERMANENT_SESSION_LIFETIME = timedelta(minutes=20)
    
    #DATABASE_URL = 'postgres://postgres:test@localhost/nosypm'
    #JOB_STORE_URL = "postgres://postgres:test@localhost/scheduler"
    #swrapper.scheduler.add_jobstore('sqlalchemy', url='sqlite:///example.sqlite3')
    #conn_str = 'mysql+pymysql://work:1qazxsw2@172.17.30.110/windmill?charset=utf8'
    # conn_str = 'mysql://work:1qazxsw2@172.17.30.110/windmill?charset=utf8'

    JOB_STORE_URL = 'mysql://work:1qazxsw2@172.17.30.110/windmill?charset=utf8'
    DATABASE_URL = JOB_STORE_URL
     
    ZK_URI = '127.0.0.1:2181'
    ZK_ROOT = '/windmill'
    ZOOKEEPER_PATH = "zk://localhost:2181/windmill"
    
    LOG_TO = "/tmp/logs"
    
    TMP_DIR = "/tmp"
    
    '''
    SCHEDULER_JOBSTORES = {
        'default': SQLAlchemyJobStore(url='sqlite://')
    }
    
    SCHEDULER_EXECUTORS = {
        'default': {'type': 'threadpool', 'max_workers': 20}
    }

    SCHEDULER_JOB_DEFAULTS = {
        'coalesce': False,
        'max_instances': 3
    }
    '''
    
    
    

    # LOGGER = {level=logging.DEBUG, name="SimpleScheduler", file="log_{date:%Y-%m-%d}.log".format(date=date.today()), formatter=logging.Formatter("%(asctime)s [%(thread)d:%(threadName)s] [%(levelname)s] - %(name)s:%(message)s"}

