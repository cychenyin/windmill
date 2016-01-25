# coding: utf-8
'''
Created on Dec 1, 2015

@author: MLS
'''

from abc import ABCMeta, abstractmethod
from six import with_metaclass

class FilterBase(with_metaclass(ABCMeta)):
    def __init__(self, prev=None, scheduler=None):
        self.__prev = prev;
        self.__next = None;
        self.__scheduler
        self.__name = 'base';
        
        self._configure()    

    @abstractmethod
    def _configure(self):
        pass
    
    @abstractmethod
    def _filter(self, job):
        return True

    
    @abstractmethod
    def __before_process(self, job):
        pass

    @abstractmethod
    def _after_process(self, job):
        pass
    
    
    # state check
    def filter(self, job):
        ret = self._filter(job) and ( self.__next == None or (self.__next != None and self.__next._filter(job)))
        if ret == True and self.__prev == None:
            self._before_process(job)
        return ret

    # set state  before running  
    def _before_process(self, job):
        self.__before_process(job)
        if(self.__next):
            self.__next._before_process(job)
    # clear state  before running  
    def after_process(self, job):
        self._after_process(job)
        if(self.__next):
            self.__next.after_process(job)
    
    def first(self):
        f = self
        while(f.__prev):
            f = f.__prev
        return f
        
    def get(self, name):
        f= self.first()
        while(f):
            if f.__name == name:
                return f
            else:
                f = f.__next
    