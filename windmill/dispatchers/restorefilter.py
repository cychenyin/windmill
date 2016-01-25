# coding: utf-8

'''
Created on Dec 1, 2015

@author: MLS
'''
from windmill.dispatchers.filterbase import FilterBase
from datetime import datetime

class RestoreFilter(FilterBase):
    def _configure(self):
        pass
    
    def _filter(self, job):
        now = datetime.now(self._scheduler.timezone)
        
        if job.conf_next_run_time == None:
            # set first next_run_time
            job.current_run_time = None
            job.next_run_time = self.trigger.get_next_fire_time(None, now)
            job.need_update = True
            return False
        else:
            if job.conf.restore_strategy == 0: 
                # do not restore
                job.current_run_time = job._lately_run_time()
                job.next_run_time = self.trigger.get_next_fire_time(job.current_run_time, now)
                job.need_update = True
            else:
                # restore every run_time
                job.current_run_time = job.conf_next_run_time
                job.next_run_time = self.trigger.get_next_fire_time(job.current_run_time, now)
                job.need_update = True
            return True
    
    def __before_process(self, job):
        pass

    def _after_process(self, job):
        pass
