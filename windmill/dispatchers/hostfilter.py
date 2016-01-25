# coding: utf-8

'''
Created on Dec 1, 2015

@author: MLS
'''
from windmill.dispatchers.filterbase import FilterBase
import re, logging, socket

class HostFilter(FilterBase):
    def _configure(self):
        self._local_hostname = socket.gethostname() 
    
    def _filter(self, job):
        # 2 chinese symbol included: chinese comma and chinese semicolon
        hosts = [ x.strip() for x in re.split('[，；,; \\r\\n]', job.conf.hosts) if x and x.strip()] 
        if self._local_hostname not in hosts:
            logging.info('jog ignored. local ip %s not in hosts %s' % (self._local_hostname, job.conf.hosts ))
            return False
        return True
    
    def __before_process(self, job):
        pass

    def _after_process(self, job):
        pass
