# coding: utf-8
'''
Created on Dec 1, 2015

@author: MLS
'''

from sqlalchemy import Sequence, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import PrimaryKeyConstraint


Base = declarative_base()

class JobConf(Base):
    __tablename__ = 'wm_jobs'
    __table_args__ = (
        PrimaryKeyConstraint('id'),
    )
    id = Column('id', Integer, nullable=False)
    cmd = Column('cmd', String(512), nullable=False, server_default='')
    cron_str = Column('cron_str', String(512), nullable=False, server_default='')
    name = Column('name', String(50), nullable=False, server_default='', index=True)
    desc = Column('desc', String(1000), nullable=False, server_default='')
    mails = Column('mails', String(200), nullable=False, server_default='')
    phones = Column('phones', String(200), nullable=False, server_default='')
    team = Column('team', String(50), nullable=False, server_default='')
    owner = Column('owner', String(50), nullable=False, server_default='', index=True)
    hosts = Column('hosts', String(1000), nullable=False, server_default='')
    host_strategy = Column('host_strategy', Integer, nullable=False, server_default='0', )  # 0, every host; 1, one of these 
    restore_strategy = Column('restore_strategy', Integer, nullable=False, server_default='0') # 0, do not restore. 1. restore once 
    retry_strategy = Column('retry_strategy', Integer, nullable=False, server_default='0')   # 0, no retry; N>0, retry N times 
    error_strategy = Column('error_strategy', Integer, nullable=False, server_default='0')   # 0, do nothing; 1, stop job & alarm through both mail & phones
    exist_strategy = Column('exist_strategy', Integer, nullable=False, server_default='0') # 0, whatever; 1, skip this period; 2 (not support now), wait for stop until end of this period 
    running_timeout_s = Column('running_timeout_s', Integer, nullable=False, server_default='0') # 0, without endless 1, kill job when timeout
    status = Column('status', Integer, nullable=False, server_default='0', index=True) # 0, stopped or new; 1, running normally; 2. suspend by runtime  
    modify_time = Column('modify_time', Integer, nullable=False, server_default='0')
    modify_user = Column('modify_user', String(50), nullable=False, server_default='')
    create_time = Column('create_time', Integer, nullable=False, server_default='0')
    create_user = Column('create_user', String(50), nullable=False, server_default='')
    start_date = Column('start_date', Integer, nullable=False, server_default='0', index=True)
    end_date = Column('end_date', Integer, nullable=False, server_default='0', index=True)
    oupput_match_reg = Column('oupput_match_reg', String(100), nullable=False, server_default='')  # do nothing when empty, otherwise, alarm when match Regex expression specified
    next_run_time = Column('next_run_time', Float(25), nullable=False, server_default='0', index=True)
    
    ######################### methods
    def __init__(self):
        self.id = 0
        self.cmd = ''
        self.cron_str = ''
        self.name = ''
        self.desc = ''
        self.mails = ''
        self.phones = ''
        self.team = ''
        self.owner = ''
        self.hosts = ''
        self.host_strategy = 0
        self.restore_strategy = 0
        self.retry_strategy = 0
        self.error_strategy = 0
        self.exist_strategy = 0
        self.running_timeout_s = 0
        self.status = 0
        self.modify_time = 0
        self.modify_user = 0
        self.create_time = 0
        self.create_user = 0
        self.start_date = 0
        self.end_date = 0
        self.oupput_match_reg = ''
        self.next_run_time = 0.0 
        
    def __repr__(self):
        return "<CmdJob(id='%s', name='%s', cmd='%s')>" % (
                                self.id, self.name, self.cmd)
        
    def __str__(self):
        return "CmdJob(id='%s', name='%s', cmd='%s')" % (
                                self.id, self.name, self.cmd)
    
    
    def get_hosts_list(self):
        return  [ x.strip() for x in self.hosts.split(',') if x and x.strip()]
    
    # put attributes into file a, and then use following shell command line to gen to_dict code
    # cat a.bak | awk '{print "'\''"$1"'\'': self."$1"," }' 
    def to_dict(self):
        return {
                'id': self.id,
                'cmd': self.cmd,
                'cron_str': self.cron_str,
                'name': self.name,
                'desc': self.desc,
                'mails': self.mails,
                'phones': self.phones,
                'team': self.team,
                'owner': self.owner,
                'hosts': self.hosts,
                'host_strategy': self.host_strategy,
                'restore_strategy': self.restore_strategy,
                'retry_strategy': self.retry_strategy,
                'error_strategy': self.error_strategy,
                'exist_strategy': self.exist_strategy,
                'running_timeout_s': self.running_timeout_s,
                'status': self.status,
                'modify_time': self.modify_time,
                'modify_user': self.modify_user,
                'create_time': self.create_time,
                'create_user': self.create_user,
                'start_date': self.start_date,
                'end_date': self.end_date,
                'oupput_match_reg': self.oupput_match_reg,
                'next_run_time': self.next_run_time,
                }
    
if __name__ == '__main__':
    job = JobConf()
#     print job
    job.cmd = 'date +%s'
    
    print job.cron_str
    
    
#     print job.cmd
#     import commands
#     print commands.getstatusoutput(job.cmd)
    
#     conn_str = 'mysql://work:1qazxsw2@172.17.30.110/windmill?charset=utf8'
#     from sqlalchemy import create_engine, Table, Column, MetaData, Unicode, String, Integer, Float, LargeBinary, select, text
#     engine = create_engine(conn_str, encoding='utf-8', echo=True)
#     from sqlalchemy.orm import sessionmaker
#     Session = sessionmaker(bind=engine)
# #     Session.configure(bind=engine)
#     trans = Session()
#     
#     new_job = JobConf(cmd="ls -al ~")
#     trans.add(new_job)
#     trans.commit()
#     
#     o = trans.query(JobConf).order_by('id desc').limit(1).one()
#     print o
# 
#     o = trans.query(JobConf).filter_by(id=1).one()    
#     print o
#     o = trans.query(JobConf).filter_by(name='tese').filter_by(id=4).one()    
#     print o
#     trans.close_all()