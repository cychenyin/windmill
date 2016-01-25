# coding: utf-8
'''
Created on Dec 1, 2015

@author: MLS

dependencies: Jinja2-2.8 MarkupSafe-0.23 Werkzeug-0.11.2 flask-0.10.1 
    itsdangerous-0.24 
    hoi 3.0.4 
    python-dateutil==2.4.2
    
inspired by  https://github.com/imwilsonxu/fbone/tree/master/fbone

'''

import logging, json, socket
from flask import Flask
from flask import render_template, Response
from runner.skeleton import WindmillAssembly
from config import Config as Config
from encodings import utf_8
from array import array
from cgitb import handler

def config_app(app, conf=None):    
    app.config.from_object(Config())
    if conf:
        app.config.from_object(conf)
        
    app.debug = app.config.get('DEBUG')
    app.secret_key = app.config.get('SECRET_KEY')

def config_logging(app, config=None):
    # 100M pre file
    # NOTSET < DEBUG < INFO < WARNING < ERROR < CRITICAL
    def inner_init(loggers):
        handlers = app.config['LOG_HANDLERS']
        formatter = app.config['LOG_FORMATER']
        level = app.config['LOG_LEVEL']
        for logger in loggers:
            del logger.handlers[:]
            
            for handler in handlers:
                #handler.setLevel(logging.INFO)
                handler.setLevel(level)
                handler.setFormatter(formatter)
#                 print (handler.level)
#                 print (handler.formatter._fmt)
                logger.addHandler(handler)
                logger.setLevel(level)
        
    logger_names = ['windmill', 'windmill.stores', 'windmill.stores.default', 'windmill.scheduler', 'windmill.executors', 'windmill.executors.default' ]
    loggers = [ logging.root ] + [logging.getLogger(name) for name in logger_names]
    inner_init(loggers)
    
    
def config_hook(app):
    @app.before_request
    def before_request():
        pass

def config_blueprints(app, blueprints):
    if blueprints:
        for blueprint in blueprints:
            app.register_blueprint(blueprint)

def config_extensions(app):
    pass

def config_scheduler(app):
    wm = WindmillAssembly(app=app, conf=Config())
    app.scheduler = wm.scheduler
    app.windmill = wm
    app.windmill.start()
    logging.warn('config_scheduler executed')
    
def config_error_handlers(app):
    def _jsonify(message, code, status=500):
        return Response(json.dumps({'code': code, 'message': message }, indent=2), status=status, mimetype='application/json')
    
    @app.errorhandler(403)
    def forbidden_page(error):
#         return render_template("errors/forbidden_page.html"), 403
        return _jsonify(str(error), 1, 403)

    @app.errorhandler(404)
    def page_not_found(error):
#         return render_template("errors/page_not_found.html"), 404
        return _jsonify(str(error), 1, 404)

    @app.errorhandler(500)
    def server_error_page(error):
#         return render_template("errors/server_error.html"), 500
        return _jsonify(str(error), 1, 500)

def config_template_filters(app):
    @app.template_filter()
    def pretty_date(value):
        return pretty_date(value)

    @app.template_filter()
    def format_date(value, format='%Y-%m-%d'):
        return value.strftime(format)


def config_views(app):
    import views
    funcs = [ 
            ['/'        , '/',          views.get_scheduler_info],
            ['/index'   , '/index',     views.get_scheduler_info],
            ['/stop'    , '/stop',      views.stop],
            ['/start'   , '/start',     views.start],
            ['/add'     , '/add',       views.add_job],
            ['/delete'  , 'delete1',    views.delete_job],
            ['/get'     , 'get',        views.get_job],
            ['/list'    , 'list',       views.get_jobs],
            ['/update'  , 'update',     views.update_job],
            ['/delete/<job_id>', 'delet2', views.delete_job],
         ]
    for f in funcs:
        app.add_url_rule(f[0], f[1], f[2] )
    
def create_app(config=None, app_name=None, blueprints=None):
    app = Flask(__name__)
    
    """Create a Flask app."""
    #app = Flask(app_name, instance_path=INSTANCE_FOLDER_PATH, instance_relative_config=True)
    config_app(app, config)
    config_logging(app)
    config_hook(app)
    config_blueprints(app, blueprints)
    config_extensions(app)
    config_template_filters(app)
    config_error_handlers(app)
    config_scheduler(app)
    
    config_views(app)
    
    logging.warning('Flask app created')
    
    
    return app
