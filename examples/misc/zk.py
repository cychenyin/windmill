from kazoo.client import KazooClient, KazooState ,KeeperState

import os, sys
import logging
import threading
from threading import Event 

def xy_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        logging.info("session of zk lost")
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        logging.info("conn disconnected to zk")
    else:
        # Handle being connected/reconnected to Zookeeper
        logging.info("conn / reconn to zk")


if __name__ == '__main__':
    logging.basicConfig(format='-------------- %(asctime)s %(levelname)7s: %(message)s [%(lineno)d]')
    logging.root.setLevel(logging.INFO) 
    logging.info("test")

    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.add_listener(xy_listener)
    zk.start()
    
    @zk.add_listener
    def watch_for_ro(state):
        if state == KazooState.CONNECTED:
            if zk.client_state == KeeperState.CONNECTED_RO:
                print("Read only mode!")
            else:
                print("Read/Write mode!")


    # Determine if a node exists
    # kazoo.protocol.states.ZnodeStat 
    # ZnodeStat(czxid=7807, mzxid=7809, ctime=1450246467993, mtime=1450246468015, version=1, cversion=1, aversion=0, ephemeralOwner=0, dataLength=9, numChildren=1, pzxid=7808)
    result = zk.exists("/xy/test")
    if result:
        print('exists /xy/test reuslt=%s' % str(result) )

    # Ensure a path, create if necessary
    result = zk.ensure_path("/xy/test")
    print('ensure_path type of reuslt=%s' % type(result) )
    print('ensure_path reuslt=%s' % result )
    
    # Determine if a node exists
    if zk.exists("/xy/test"):
        print('exists type of reuslt=%s' % type(result) )
    # Create a node with data
    try:
        result = zk.create("/xy/test/node", b"a value", acl=None)
    except Exception, e:
        print('=========== exception when create node, %s' % e)
    else:
        print('=========== create /xy/test/node reuslt=%s' % result )
    
    #reading
    # Determine if a node exists
    print('exists /xy/test/node reuslt=%s' % str(zk.exists("/xy/test/node")))
    data, stat = zk.get("/xy/test/node")
    print("//////////////////////////// /xy/test/node Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
    
    print("")
    print("")
    print("")
    
#     # Print the version of a node and its data
#     data, stat = zk.get("/xy/test")
#     print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
#     
#     # List the children
#     children = zk.get_children("/xy/test")
#     print("There are %s children with names %s" % (len(children), children))

    #update
    try:
        result = None
        result = zk.set("/xy/test", b"some data")
    except Exception, e:
        print('exception when zk.set, %s' % e)
    else:
        print("zk.set /xy/test result %s" % str(result))
    
    
    # del
    result = zk.delete("/xy/test/node", recursive=True)
    print("zk.delete /xy/test/node result %s" % (result))
    
    # action
    try:
        result = zk.retry(zk.get, "/xy/test/nodex")
    except Exception, e:
        print('exception when zk.retry, %s' % e)
    else:
        print("zk.retry /xy/test/nodex result %s" % str(result))
    
    from kazoo.retry import KazooRetry
    kr = KazooRetry(max_tries=3, ignore_expire=False)
    try:
        result = kr(zk.get, "/xy/test/nodex")
    except Exception, e:
        print('exception when KazooRetry, %s' % e)
    else:
        print("KazooRetry zk.get /xy/test/nodex result %s" % (result))
        
    
    
    #watcher
    def xy_func(event):
        # check to see what the children are now
        print("xy_func watcher event:%s" % event)

    # Call xy_func when the children change
    try:
        children = zk.get_children("/xy/test/node", watch=xy_func)
    except Exception, e:
        print("exception when get_childeren %s" % str(e))
    else:
        print("zk.get_children /xy/test/node result %s" % (children))
    
    @zk.ChildrenWatch("/xy/test")
    def watch_children(children):
        #print("watch_children of /xy/test, Children are now: %s" % str(children))
        #print("watch_children of /xy/test, Children count: %d" % len(children))
        pass
    # Above function called immediately, and from then on
    
    @zk.DataWatch("/xy/test")
    def watch_node(data, stat):
        #print("watch_node, Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
        pass
        
    #trans, great!!!
    transaction = zk.transaction()
    transaction.check('/xy/test/node2', version=3)
    transaction.create('/xy/test/node2', b"a value")
    result = transaction.commit()
    print("transaction result %s" % str(result))
    
    print ("----------------------------")

#     for i in range(1,100):    
#         try:
#             result = zk.create("/xy/test/node", b"a value", acl=None, sequence=True, ephemeral=True)
#         except Exception, e:
#             print('=========== exception when create node, %s' % e)
#         else:
#             #print('=========== create /xy/test/node reuslt=%s' % result )
#             pass


    if zk.exists("/xy/test/node"):
        data, stat = zk.get("/xy/test/node")
        print("/xy/test/node Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
    else:
        print("/xy/test/node not exists")
        
    print ("----------------------------0")
    zk.create("/xy/test/node", b"a value", acl=None)
    data, stat = zk.get("/xy/test/node")
    print("/xy/test/node Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
    print ("----------------------------1")
    zk.delete("/xy/test/node")
    zk.create('/xy/test/node', b"abc")
    data, stat = zk.get("/xy/test/node")
    print("/xy/test/node Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
    print ("----------------------------2")
    zk.delete("/xy/test/node")
    zk.create('/xy/test/node', b"def", acl=None)
    data, stat = zk.get('/xy/test/node')
    print("/xy/test/node Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
    print ("----------------------------3")
    
#     ev = Event()
#     ev.set()
#     wait_seconds = 1
#     threading.sleep(10)
#     while(True):
#         ev.wait(wait_seconds)
#         ev.clear()
    
    input = None
    while(input != 'quit' and input != 'exit'):
        print "print quit or exit to QUIT"
        input = raw_input()
        input = input.lower()

    print "quitting "    
    zk.stop()
    