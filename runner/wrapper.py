# coding: utf-8
'''
Created on Dec 1, 2015

@author: MLS
'''
import logging
import socket
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.jobstores.mysqlstore import MysqlJobStore
from apscheduler.dispatchers.local import ZkDispather
from flask import Flask
from config import Config

#@allowed_hosts('0.0.0.0')
class SchedulerWrapper(object):
    def __init__(self,app=None,scheduler=None):
        self.__app = None
        self.__set_app(app)
        self.__init_scheduler(scheduler)
        
    ################################## properties    
    @property
    def scheduler(self):
        return self.__scheduler

    @property
    def host_name(self):
        pass
    
    @property
    def running(self):
        """Gets true if the scheduler is running."""
        return self.__scheduler.running
    
    ################################## init
    def __init_scheduler(self, scheduler):
        conf = {}
        c = Config()
        for key in dir(c):
            if key.isupper():
                conf[key] = getattr(c, key)
                
        # TODO ... edit config stuff, eg, daemon, thread counter, etc 
        #conn_str = 'mysql://work:1qazxsw2@172.17.30.110/windmill?charset=utf8'
        conn_str = self.__app.config['JOB_STORE_URL']
        self.__scheduler = scheduler or BackgroundScheduler(conf);
        self.__load_config()
        # had better do NOT change the following aliases
        self.jobstore_alias = 'default'
        self.executor_alias = 'default'
        #self.__executor = ProcessPoolExecutor();
        self.__scheduler._jobstore = self.__jobstore = MysqlJobStore(url=conn_str)
        self.__scheduler._executor = self.__executor = ThreadPoolExecutor(max_workers=20);
        self.__scheduler._dispatcher = self.__dispatcher = ZkDispather(self.__scheduler, self.__executor, self.__app.config['ZK_URI'])
        
#         self.__jobstore._logger = self.__app.logger
#         self.__executor._logger = self.__app.logger
#         self.__scheduler._logger = self.__app.logger
        
        self.__load_jobs()
        
    def __set_app(self, app):
        """Initializes the APScheduler with a Flask application instance."""
        if not app:
            return
        
        if not isinstance(app, Flask):
            raise TypeError('app must be a Flask application')

        self.__app = app
        #self.__app.apscheduler = self
    
    def __load_config(self): 
        options = dict()

#         job_stores = self.__app.config.get('SCHEDULER_JOBSTORES')
#         if job_stores:
#             options['jobstores'] = job_stores
# 
#         executors = self.__app.config.get('SCHEDULER_EXECUTORS')
#         if executors:
#             options['executors'] = executors
# 
#         job_defaults = self.__app.config.get('SCHEDULER_JOB_DEFAULTS')
#         if job_defaults:
#             options['job_defaults'] = job_defaults

        timezone = self.__app.config.get('SCHEDULER_TIMEZONE')
        
        if timezone:
            options['timezone'] = timezone

        self.__scheduler.configure(**options)

    def __load_jobs(self):
        jobs = self.__app.config.get('SCHEDULER_JOBS')

        if not jobs:
            jobs = self.__app.config.get('JOBS')

        if jobs:
            for job in jobs:
                self.add_job(**job)
    

        
    ################################## job stuff methods
    
    #method which add one job into scheduler use for testing proposer
    def add_job(self, id, func, **args):
        if not id:
            raise Exception('Argument id cannot be None.')

        if not func:
            raise Exception('Argument func cannot be None.')
        
        job_def = dict(args)
        job_def['id'] = id
        job_def['func'] = func
        job_def['name'] = job_def.get('name') or id
        job = self.__scheduler.add_job(**job_def)
        return job
    
    def delete_job(self, id, jobstore=None):
        """
        Removes a job, preventing it from being run any more.
        :param str id: the identifier of the job
        :param str jobstore: alias of the job store that contains the job
        """
        self.__scheduler.remove_job(id, jobstore)

    
    def modify_job(self, id, jobstore=None, **changes):
        """
        Modifies the properties of a single job. Modifications are passed to this method as extra keyword arguments.
        :param str id: the identifier of the job
        :param str jobstore: alias of the job store that contains the job
        """

        if not id:
            raise Exception('Argument id cannot be None or empty.')
        
        self.__scheduler.modify_job(id, jobstore, **changes)

        job = self.__scheduler.get_job(id, jobstore)

        return job
    def pause_job(self, id, jobstore=None):
        if not id:
            raise Exception('Argument id cannot be None or empty.')
        self.__scheduler.pause_job(id, jobstore)

    def resume_job(self, id, jobstore=None):
        if not id:
            raise Exception('Argument id cannot be None or empty.')        
        self.__scheduler.resume_job(id, jobstore)
    
    def run_job(self, id, jobstore=None):
        job = self.__scheduler.get_job(id, jobstore)
        if not job:
            raise LookupError(id)
        job.func(*job.args, **job.kwargs)
    

    def get_jobs(self):
        pass
    
    ######################################## private methods
   
    def reload(self):
        pass
    
    def start(self):
        self.__scheduler.start();
            
    def shutdown(self, wait=True):
        self.__scheduler.shutdown(wait)

    def prints(self):
        self.__scheduler.print_jobs()
    
