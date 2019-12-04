from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse
from threading import Timer
import os
import random
import xmlrpc.client
import threading




class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True

# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")

    blockData = bytes(4)
    return blockData

# Puts a block
def putblock(b):
    """Puts a block"""
    print("PutBlock()")

    return True

# Given a list of hashes, return the subset that are on this server
def hasblocks(hashlist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")

    return hashlist

# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    print("GetFileInfoMap()")

    return fileinfomap

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile("+filename+")")

    fileinfomap[filename] = [version, hashlist]

    return True

# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("is leader?"+"  "+currentstate)
    return currentstate=="leader"

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    global crashstate
    print("Crash()")
    crashstate=True
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    global crashstate
    """Restores this metadata store"""
    print("Restore()")

    crashstate=False
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    
    return crashstate

def getterm():
    return currentterm
# Requests vote from this server to become the leader
def requestVote():
    """Requests vote to be the leader"""
    print(node+" start request vote...")
    global currentstate,count,timer,mutex,currentterm,votefor
    
    terms=[]
    terms.append(currentterm)
    for i in  clientlist:
        try:
           if not i.surfstore.isCrashed():
              terms.append(i.surfstore.getterm())
        except:
            print("one node carsh")
    max_term=max(terms)
    if max_term>currentterm:
        candidate_to_follower(max_term,"")
        return False
    for i in clientlist: 
        try:
          if i.surfstore.isLeader() and not i.surfstore.isCrashed():
             candidate_to_follower(max_term,"")
             return False
        except:
            print("one node crash")

    
    mutex=True     
    currentterm+=1
    count=1
    currentstate="candidate"
    if timer is not None:
        timer.cancel()
    timer=None

    mutex=False
    for i in clientlist:
        try:
           if i.surfstore.answervote(node,currentterm):
              count+=1
        except:
            print("one vote fail")

    print("vote count:"+str(count))
    if count>int(total_server_num/2):
            mutex=True
            currentstate="leader"
            if timer is not None:
                timer.cancel()
            timer=None
            mutex=False
    return True
        




def requestanotherVote():
    global currentstate
    if currentstate=="candidate":
        requestVote()
        

# Updates fileinfomap
def appendEntries(serverid, term, fileinfomap):
    """Updates fileinfomap to match that of the leader"""
    return True

def tester_getversion(filename):
    return fileinfomap[filename][0]

def candidate_to_follower(term,vote):
    global currentstate,timer,mutex,currentterm,votefor
    mutex=True
    if timer is not None:
        timer.cancel()
    timer=None
    currentterm=term
    currentstate="follower"
    votefor=vote
    mutex=False

def answervote(serverid,term):
    global currentstate,timer,mutex,currentterm,votefor
    if term>currentterm:
        mutex=True
        candidate_to_follower(term,serverid)
        mutex=False
        return True
    elif term==currentterm and votefor=="":
        
            votefor=serverid
            return True
    else:
        return False

       

# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)


    return maxnum, host, port


def leaderaction():
    return True








if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        config = args.config
        servernum = args.servernum
        mutex=False
        # server list has list of other servers
        serverlist = []
        currentterm=0
        currentstate="follower"
        votefor=False
        count=0
        timer=None
        crashstate=False
        # maxnum is maximum number of servers
        maxnum, host, port = readconfig(config, servernum)

        total_server_num=len(serverlist)+1

        hashmap = dict();

        fileinfomap = dict()

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        print(serverlist)
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(appendEntries,"surfstore.appendEntries")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        server.register_function(getterm,"surfstore.getterm") 
        server.register_function(answervote,"surfstore.answervote") 
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")

        clientlist=[xmlrpc.client.ServerProxy("http://"+i) for i in serverlist]

        




        mainThread = threading.Thread(target = server.serve_forever)
        mainThread.start()
        clientlist=[xmlrpc.client.ServerProxy("http://"+i) for i in serverlist]
        

        
        node=host+":"+str(port)   
        while True:
             if mutex or crashstate:continue
             if currentstate=="leader":
                 
                 if timer is  None:
                     print(host,port,currentstate,str(currentterm))
                     # timer=Timer(0.1,leaderaction)
                     # timer.start()
                     
             elif currentstate=="follower": 
                 
                 if timer is None:
                     print(host,port,currentstate,str(currentterm)) 
                     timer = Timer(random.randint(10000, 30000)/float(10000),requestVote)
                     timer.start()
                     
             elif currentstate=="candidate":
        
                  if timer is None:
                      print(host,port,currentstate,str(currentterm))
                      timer = Timer(random.randint(10000, 30000)/float(10000),requestanotherVote) 
                      timer.start()
    except Exception as e:
        print("Server: " + str(e))