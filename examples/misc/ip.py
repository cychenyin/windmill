# coding: utf-8

import socket
localIP = socket.gethostbyname(socket.gethostname())#得到本地ip

print "1 local ip:%s "%localIP
 
ipList = socket.gethostbyname_ex(socket.gethostname())
for i in ipList:
#     if i != localIP:
    print "2 external IP:%s" % i
       

# myname = socket.getfqdn(socket.gethostname())
# print (3, myname)
# myaddr = socket.gethostbyname(myname)
# print (3, myname, myaddr)


def get_ip_address_1(ifname='eth0'):
    '''
    Source:
    http://code.activestate.com/recipes/439094/
    '''
    import socket
    import fcntl
    import struct
    ipaddr = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ipaddr = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])
    except:
        pass
    return ipaddr

def get_ip_address_2():
    '''
    Source:
    http://commandline.org.uk/python/how-to-find-out-ip-address-in-python/
    '''
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('baidu.com', 0))
        ipaddr=s.getsockname()[0]
        return ipaddr
    except:
        pass

def get_ip_address_3():
    import socket
    ipaddr = socket.gethostbyname(socket.gethostname())
    return ipaddr

def get_ip_address_4(netdev='eth0'):
    # Use ip addr show
    import subprocess
    arg='ip addr show ' + netdev    
    try:
        p=subprocess.Popen(arg,shell=True,stdout=subprocess.PIPE)
        data = p.communicate()
        sdata = data[0].split('\n')
        macaddr = sdata[1].strip().split(' ')[1]
        ipaddr = sdata[2].strip().split(' ')[1].split('/')[0]
        return (ipaddr,macaddr)
    except:
        pass
    
def get_ip_address_5():
    #Use ip route list
    import subprocess
    arg='ip route list'    
    p=subprocess.Popen(arg,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    data = p.communicate()
    print "get_ip_address_5"
    print data
    sdata = data[0].split()
    try:
        ipaddr = sdata[ sdata.index('src')+1 ]
        netdev = sdata[ sdata.index('dev')+1 ]
        return (ipaddr,netdev)
    except:
        pass
    
print "================================="
print(get_ip_address_1())
print(get_ip_address_2())
print(get_ip_address_3())
print(get_ip_address_4())
print(get_ip_address_5())