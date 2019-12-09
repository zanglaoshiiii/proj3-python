from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from MyTimer import MyTimer
import argparse
import hashlib
import xmlrpc.client
import random
import time
import threading


fileinfomap = {}

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

# Retrieves the server's fileinfomap
def getfileinfomap():
    """Gets the fileinfo map"""

    
    return fileinfomap

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    if isCrash or status != 0:
        raise Exception('Exception')

    global log

    
    log.append([currentTerm, [filename, version, hashlist]])
    lastIndex = len(log) - 1

    # block
    while commitIndex < lastIndex:
        pass

    return True


def apply(index):
    filename, version, hashlist = log[index][1]
    if filename in fileinfomap.keys():
        lastestVersion = fileinfomap[filename]
        if (version == lastestVersion[0] + 1):
            fileinfomap[filename] = [version, hashlist]
        else:
            return False
    else:
        fileinfomap[filename] = [version, hashlist]
    return True

# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    if status == 0:
        return True
    else:
        return False

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    global isCrash
    print("Crash()")
    isCrash = True
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    global isCrash
    print("Restore()")
    isCrash = False
    timer.reset()
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return isCrash

def tester_getVersion(filename):
    "gets version number of file from server"
    
    return fileinfomap[filename][0]
    

def requestVote(sv):
    global voteCount
    global currentTerm
    global status

    if isCrash:
        raise Exception('Crashed')
    
    lastLogIndex = len(log) - 1
    lastLogTerm = log[-1][0]

    try:
        voteResponse = sv.voteHandler(currentTerm, servernum, lastLogIndex, lastLogTerm )

        if voteResponse[0]: 
            voteCount +=1
        else:
            if voteResponse[1]>currentTerm:
                currentTerm = voteResponse[1]
                status = 2
            pass
            
    except Exception as e:

        pass

def voteHandler(cand_term, cand_id, cand_lastLogIndex, cand_lastLogTerm):
    if isCrash:
        raise Exception('Crashed')
    global timer
    timer.reset()

    def castVote():
        global voteFor
        global currentTerm
        global status
        voteFor = cand_id
        currentTerm = cand_term
        status = 2
        print("casting vote to", cand_id)
        return [True, currentTerm]

    if cand_term <= currentTerm:
        return [False, currentTerm]
    else:
        lastLogIndex = len(log)-1
        lastLogTerm = log[-1][0]
        print("lastLogTerm", lastLogTerm)

        if cand_lastLogIndex > lastLogIndex:
            return castVote()
        elif cand_lastLogIndex == lastLogIndex \
                and cand_lastLogTerm == lastLogTerm:
            return castVote()
        else:
            return [False, currentTerm]    


def appendEntries(sv):
    global nextIndex
    global matchIndex
    global prevLogIndex
    global status
    global currentTerm
    global success
    global prevLogTerm

    try:
        if newLeader:
            nextIndex[sv] = len(log)
        prevLogIndex[sv] = nextIndex[sv]-1
        prevLogTerm[sv] = log[prevLogIndex[sv]][0]
            
        if success[sv] and prevLogIndex[sv]== len(log)-1:

            entries =[]
        elif success[sv] and prevLogIndex[sv] != len(log)-1 :
            entries = log[prevLogIndex[sv]:len(log)]
        else:
            entries =[]


        leader_commit = commitIndex
        follower_term, success[sv] = sv.appendEntryHandler(currentTerm, servernum, prevLogIndex[sv],\
                                prevLogTerm[sv], entries, leader_commit)

        if follower_term > currentTerm:
            status = 2
            currentTerm = follower_term

        if success[sv]:
            if len(entries)>0: prevLogIndex[sv] += len(entries)-1
            matchIndex[sv] = prevLogIndex[sv]
            nextIndex[sv] = prevLogIndex[sv]+1
        else:
            if nextIndex[sv] > 0:
                nextIndex[sv] -= 1

    except Exception as e: 

        pass

def appendEntryHandler(leader_term, leader_id, prevLogIndex,\
                        prevLogTerm, entries, leader_commit):

    if isCrash:
        raise Exception('Crashed')

    global timer
    global currentTerm
    global status
    global commitIndex

    if leader_term < currentTerm:
        return currentTerm, False

    def appendLog():
        print("in appendLog")
        print(prevLogIndex, prevLogIndex + len(entries))
        for i,j in enumerate(range(prevLogIndex, prevLogIndex + len(entries))):
            if j < len(log):
                log[j] = entries[i]
            else:
                log.append(entries[i])

    status = 2
    currentTerm = leader_term
    timer.reset()
    print("hearbeat from "+ str(leader_id)+" in term: "+str(currentTerm))
    print("received entries:", entries)

    if len(log) - 1 < prevLogIndex or log[prevLogIndex][0] != prevLogTerm:
        return currentTerm, False

    if entries != []:
        appendLog()

    if leader_commit > commitIndex:
        commitIndex = min(leader_commit, len(log) - 1)

        
    return currentTerm, True 

def lifecycle():
    global currentTerm
    global status
    global voteCount
    global timer
    global newLeader
    global nextIndex
    global matchIndex
    global success
    global prevLogIndex
    global prevLogTerm
    global commitIndex
    global lastApplied

    timer = MyTimer()
    timer.reset()
    while True:
        while isCrash:
            status = 2
            pass
        if commitIndex > lastApplied:
            if apply(lastApplied+1):
                lastApplied += 1
        if status !=0:
            if timer.currentTime() > timer.timeout:
                status = 1  
                currentTerm +=1
                voteCount = 0 
                voteCount += 1 
                timer.reset()
                notleader_th_list = []
                for sv in serverList:
                    notleader_th_list.append(threading.Thread(target = requestVote, args=(sv, )))
                    notleader_th_list[-1].start()
                for t in notleader_th_list:
                    t.join()

                if voteCount > (numServers/2):
                    status = 0 
                    newLeader = True
                    print("I am the leader in term: " + str(currentTerm) +", votes: " + str(voteCount))
                    print(log)

        else: 
            timer.setTimeout(100)
            if timer.currentTime() > timer.timeout:
                timer.reset()
                leader_th_list= []
                if newLeader:
                    nextIndex ={}
                    matchIndex = {}
                    success={}
                    prevLogIndex = {}
                    prevLogTerm = {}
                    for sv in serverList:
                        nextIndex[sv] = len(log) 
                        success[sv] = False


                for sv in serverList:
                    leader_th_list.append(threading.Thread(target = appendEntries, args=(sv, )))
                    leader_th_list[-1].start()
                for t in leader_th_list:
                    t.join()

                newLeader = False

                if len(log)-1 > commitIndex:
                    commitCount = 1
                    for sv in serverList:
                        if sv in matchIndex.keys() and matchIndex[sv]>commitIndex:
                            commitCount += 1
                    if commitCount > (numServers/2) and log[commitIndex+1][0]==currentTerm:
                        commitIndex += 1
                    print("Leader commit index: ", commitIndex)

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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="SurfStore server")
    parser.add_argument('config', help='path to config file')
    parser.add_argument('servernum', type=int, help='server number')

    args = parser.parse_args()

    config = args.config
    servernum = args.servernum
    
    # server list has list of other servers
    serverlist = []
    
    # maxnum is maximum number of servers
    maxnum, host, port = readconfig(config, servernum)

    serverList = []
    for s in serverlist:
        if s != servernum:
            serverList.append(xmlrpc.client.ServerProxy("http://" + s))


    numServers = len(serverList) + 1
    status = 2  
    isCrash = False
    currentTerm = 1
    voteFor = None
    log = [[0,0]]
    newLeader = False
    commitIndex = 0
    lastApplied = 0
    matchIndex = {}


    server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)



    server.register_introspection_functions()
    server.register_function(ping,"surfstore.ping")
    server.register_function(getblock,"surfstore.getblock")
    server.register_function(putblock,"surfstore.putblock")
    server.register_function(hasblocks,"surfstore.hasblocks")
    server.register_function(getfileinfomap,"surfstore.getfileinfomap")
    server.register_function(updatefile,"surfstore.updatefile")

    server.register_function(isLeader,"surfstore.isLeader")
    server.register_function(crash,"surfstore.crash")
    server.register_function(restore,"surfstore.restore")
    server.register_function(isCrashed,"surfstore.isCrashed")
    server.register_function(tester_getVersion, "surfstore.tester_getversion")

    server.register_function(voteHandler,"voteHandler")
    server.register_function(appendEntryHandler, "appendEntryHandler")


    print("Started successfully.")
    print("Accepting requests. (Halt program to stop.)")

    th1 = threading.Thread(target = lifecycle, )
    th1.start()

    server.serve_forever()