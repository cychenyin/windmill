# coding: utf-8

'''
Created on Dec 1, 2015

@author: MLS
'''

# coding: utf-8

'''
Created on Dec 1, 2015

@author: MLS
'''
from windmill.dispatchers.filterbase import FilterBase
from datetime import datetime
import logging

# deal max instance scenario
class ExistsFilter(FilterBase):
    def _configure(self):
        #self._state = defaultdict(lambda: [])
        pass
    
    def _filter(self, job):
#         if job.conf.exist_strategy == 1 and len(self._state[job.id]) > 0:
#             logging.info('job ignored cause of exist_strategy=1 and the last running in local host still going on. id=%s name=%s run_time=%s host=%s' % (job.id, job.conf.name, run_time, self._local_hostname ))
#             return False 
        return True
    
    def __before_process(self, job):
        pass

    def _after_process(self, job):
#         self._state[job.id].remove((f, run_time))
        pass
