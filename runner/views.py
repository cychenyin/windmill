# coding: utf-8
'''
Created on Dec 1, 2015

@author: MLS

dependencies: Jinja2-2.8 MarkupSafe-0.23 Werkzeug-0.11.2 flask-0.10.1 itsdangerous-0.24 
    
'''

import logging, json, socket
from flask import Flask
from flask import render_template, flash, redirect, session, url_for, request, g
from flask import current_app, request, Response
from flask.views import View
from windmill.stores.base import ConflictingIdError, JobLookupError
from windmill.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError
from collections import OrderedDict
#from cmdjob import CmdJob
#from application import app

__all__ = [
           'start', 
           'stop',            
           'get_scheduler_info', 
           'add_job',
           'delete_job',
           'get_job',
           'get_jobs',
           'update_job',
           'pause_job',
           'resume_job',
           'run_job',
           'register' ,
           ]

# @app.route('/start', methods=['GET', 'POST'])
def start():
    try:
        current_app.swrapper.start();
    except (SchedulerAlreadyRunningError, SchedulerNotRunningError), e:
        return __jsonify({'status': current_app.swrapper.running, 'code': 0})
    else:
        return __jsonify({'status': current_app.swrapper.running, 'code': 0})
    
# @app.route('/stop', methods=['GET', 'POST'])
def stop():
    try:
        current_app.swrapper.stop();
    except (SchedulerAlreadyRunningError, SchedulerNotRunningError), e:
        return __jsonify({'status': current_app.swrapper.running, 'code': 0})
    else:
        return __jsonify({'status': current_app.swrapper.running, 'code': 0})


# @app.route('/', methods=['GET', 'POST'])
# @app.route('/index', methods=['GET', 'POST'])
def get_scheduler_info():
    #scheduler = current_app.swrapper.scheduler
    wrapper = current_app.swrapper
    current_app.swrapper.prints()
    d = OrderedDict({'code': 0, 'data': [
        ('current_host', wrapper.host_name),
        ('running', wrapper.running),
    ]})

    return __jsonify(d)

def tick():
    from datetime import datetime
    from random import random
    try:
        print('Tick! The time is: %s __ ' % (datetime.now() ))
    except Exception as e:
        print str(e)

def cmdfunc():        
    result = runcmd('date +%s; echo ------------------------------------------')
    print result
    return result

def runcmd(cmd):
    import commands
    return commands.getstatusoutput(cmd)

# @app.route('/add', methods=['GET', 'POST'])
def add_job():
    """Adds a new job."""
    try:
        from windmill.jobconf import JobConf
        conf = JobConf()
        conf.id = 1
        conf.cmd = 'date +%s; echo ----------------------------------------'
        conf.name = 'job1'
        conf.status = 1
        #from windmill.triggers.interval import IntervalTrigger 
#         current_app.swrapper.scheduler.add_job(cmdfunc, 'interval', seconds=100, id='tick1', name='tick1 x', replace_existing=True, conf=conf)
        from windmill.triggers.cron import CronTrigger
        trigger = CronTrigger(second='*/5')
        current_app.swrapper.scheduler.add_job(cmdfunc, trigger, second=5, id=conf.id, name=conf.name, replace_existing=True, conf=conf, jobstore=current_app.swrapper.jobstore_alias)
#         data = request.get_json(force=True)
#         if data:
#             job = current_app.swrapper.scheduler.add_job(**data)
#             return __jsonify(job.conf.to_dict())
#         else:
#             return __jsonify({'message': 'data is empty', 'code':0})
        return __jsonify({'message': 'add done', 'code':0})

    except ConflictingIdError:
        return __jsonify(dict(error_message='Job %s already exists.' % conf.id), status=409)
    except LookupError as e:
        return __jsonify(dict(error_message=str(e)), status=400)
    except Exception as e:
        return __jsonify(dict(error_message=str(e)), status=500)

# @app.route('/delete', methods=['GET', 'POST'])
# @app.route('/delete/<job_id>', methods=['GET', 'POST'])
def delete_job(job_id):
    """Deletes a job."""
    try:
        current_app.swrapper.scheduler.remove_job(job_id)
        return Response(status=204)
    except JobLookupError:
        return __jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return __jsonify(dict(error_message=str(e)), status=500)

# @app.route('/get', methods=['GET', 'POST'])
def get_job(job_id):
    """Gets a job."""

    job = current_app.swrapper.scheduler.get_job(job_id)

    if not job:
        return __jsonify(dict(error_message='Job %s not found' % job_id), status=404)

    return __jsonify(job.conf.to_dict())

# @app.route('/list', methods=['GET', 'POST'])
def get_jobs():
    """Gets all scheduled jobs."""

    jobs = current_app.swrapper.scheduler.get_jobs()
    print "========================="
    print jobs
    job_states = []

    for job in jobs:
        job_states.append(job.conf.to_dict())

    return __jsonify(job_states)


# @app.route('/update', methods=['GET', 'POST'])
def update_job(job_id):
    """Updates a job."""

    data = request.get_json(force=True)

    try:
        job = current_app.swrapper.scheduler.modify_job(job_id, **data)
        if job:
            cmd = CmdJob(job)
            return __jsonify(cmd.to_dict())
        else:
            return __jsonify({'code': 1, 'message': 'job not found by id [%s]' % job_id})
    except JobLookupError:
        return __jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return __jsonify(dict(error_message=str(e)), status=500)


# @app.route('/pause', methods=['GET', 'POST'])
def pause_job(job_id):
    """Pauses a job."""

    try:
        current_app.swrapper.scheduler.pause_job(job_id)
        job = current_app.swrapper.scheduler.get_job(job_id)
        return __jsonify(job.conf.to_dict())
    except JobLookupError:
        return __jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return __jsonify(dict(error_message=str(e)), status=500)

# @app.route('/resume', methods=['GET', 'POST'])
def resume_job(job_id):
    """Resumes a job."""

    try:
        current_app.swrapper.scheduler.resume_job(job_id)
        job = current_app.swrapper.scheduler.get_job(job_id)
        return __jsonify(job.conf.to_dict())
    except JobLookupError:
        return __jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return __jsonify(dict(error_message=str(e)), status=500)


# @app.route('/run', methods=['GET', 'POST'])
def run_job(job_id):
    """Executes a job."""

    try:
        current_app.swrapper.scheduler.run_job(job_id)
        job = current_app.swrapper.scheduler.get_job(job_id)
        return __jsonify(job.conf.to_dict())
    except LookupError:
        return __jsonify(dict(error_message='Job %s not found' % job_id), status=404)
    except Exception as e:
        return __jsonify(dict(error_message=str(e)), status=500)

class Counter(object):
    def __init__(self, seed):
        self.__c = seed if seed else 0
    
    def count(self):
        self.__c += 1 
        return self.__c
    
    def get(self):
        return self.__c

def __jsonify(data, status=None):
#     from json import JSONEncoder
#     class MyEncoder(JSONEncoder):
#         def default(self, o):
#             if hasattr(o, '__dict__'):
#                 return o.__dict__
#             else: 
#                 return o
    return Response(json.dumps(data, indent=2), status=status, mimetype='application/json')
