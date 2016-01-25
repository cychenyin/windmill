#!/usr/bin/env python
# coding: utf-8

#from runner.application import create_app
from runner import create_app
    #from runner.views import *

def main():
    app = create_app()
    if app.scheduler.running:
        app.run(host='0.0.0.0', port=5000, use_reloader=False, reloader_type='watchdog')

if __name__ == '__main__':
    main()