#!/usr/bin/env python
#encoding:utf8
'''
统计用公共函数
'''
import os
import re
import sys
import MySQLdb
import urllib

from optparse import OptionParser 
from statutil import get_config_db
from mailman import sendmail

ADMIN="lizhang@server.com"

chr = re.compile('[\x80-\xff]+')

reload(sys)
sys.setdefaultencoding('utf8')

def get_opts():
    parser = OptionParser(usage="")
    
    parser.add_option("-t","--send-to",dest="name",default=ADMIN,help="the mail name prefix, who the sms is sent to")
    parser.add_option("-c","--content",dest="msg",default="",help="the sms content")

    options = parser.parse_args()[0]

    if options.msg == "":
        parser.print_help()
        print "[ERROR]the msg is null!"
        sys.exit(1)

    return [options.msg, options.name]


def get_chlen(str):
    cLen = 0
    m = chr.findall(str)
    for chiness in m:
        cLen+=len(chiness)
    
    return int(len(str)-cLen*2/3)

def get_substr(str,idx,slen):
    cnt = 0
    substr = ''
    i = 0

    while i < len(str):
        if cnt >= slen:
            break
        ch = str[i]
        if chr.search(ch):
            substr += str[i]+str[i+1]+str[i+2]
            i += 3
        else:
            substr += str[i]
            i += 1    
        cnt += 1
    
    return substr

def sendSMS(msg,recipients,send=1):
    """ 将msg内容发送到recipients, 收件人以","分割, 支持姓名和邮箱前缀 """
    
    # 收件人列表
    recipients = recipients.replace('@server.com','')
    names = recipients.split(",")
    if len(names) < 1:
        print "recipients is NULL!"
        return

    # 收件人滤重
    names = list(set(names))

    #处理msg内容，包括编码和截断
    msg_ori = msg
    max_len = 132
    if get_chlen(msg) >= max_len:
        msg = "%s.." % get_substr(msg, 0, max_len)

    msg = urllib.quote_plus(str(msg))

    # 当前主机名和用户名
    host = os.popen("hostname").readlines()[0].strip()
    user = os.popen("whoami").readlines()[0].strip()

    print "msg:%s, send to:%s" % (msg, names)

    # 发送短信报警
    for name in names:
        if len(name) < 1:
            continue

        cmd = """/usr/bin/wget -q "http://smsapi.server.com/smssys/interface/smsapi.php?smsKey=1407399202710&type=both&name=%s&phone=&smscontent=%s&mailsubject=msg_from_%s&mailcontent=%s" -O /dev/null """ % (name, msg, host, msg)
        print cmd
        if send == 1:
            os.system(cmd)
        else:
            subject = 'msg_from_%s' % host
            sendmail(name, subject, '', msg_ori)
            print "no need to send this sms"

    # 短信发送记录存档
    try:
        statdb = get_config_db('statdb')
        msg_ori = msg_ori.replace("'","\\'")
        msg_ori = msg_ori.replace("\"","\\\"")

        cursor = statdb.cursor()
        path = sys.path[0]
        sql = """ replace into t_dolphin_stat_sms(`content`,`recipients`,`host`,`user`,`path`,`send`) \
            value('%s','%s','%s','%s','%s',%s) """ % (msg_ori, recipients, host, user, path, send)
        cursor.execute(sql)
        cursor.close()
        statdb.close()
    except Exception, e:
        print e
        print "[WARN]insert the msg record into db fail!"
        pass
    
if __name__ == '__main__':

    [msg, name]= get_opts()
    
    if msg == "":
        print "\nthe msg is null, please use -h for help!\n"
        sys.exit(1)

    # 发送短信
    sendSMS(msg, name, 1)


