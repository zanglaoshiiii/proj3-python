from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse
import xmlrpc.client
import random
import time
import threading
from MyTimer import MyTimer

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

fileinfomap = {}

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
    if isCrashed == True:
        raise Exception('Crash')
    if status != 0:
        raise Exception("I am not leader")

    global log
    log.append([currentTerm, [filename, version, hashlist]])
    lastIndex = len(log)

    ### block
    while commitIndex < lastIndex:
        pass

    print("UpdateFile("+filename+")")

    # fileinfomap[filename] = [version, hashlist]

    return True

def apply_log(idx):
    filename, version, hashlist = log[idx][1]

    if filename in fileinfomap.keys():
        latestVersion = fileinfomap[filename][0]
        if version == (latestVersion + 1):
            fileinfomap[filename] = tuple((version, hashlist))
            return True
        else:
            return False
    else:
        fileinfomap[filename] = tuple((version, hashlist))
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
    print("Crash()")
    global isCrash

    isCrash = True
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    print("Restore()")
    global isCrash

    isCrash = False
    timer.reset()

    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return isCrash

# Requests vote from this server to become the leader
def requestVote(server):
    """Requests vote to be the leader"""
    if isCrashed:
        raise Exception('Crash')

    global status
    global currentTerm
    global voteCount

    lastLogIndex = len(log) - 1
    lastLogTerm = log[lastLogIndex][0]

    try:
        # response [T/F, their currentTerm]
        response = server.requestVoteHandler(currentTerm, servernum, lastLogIndex, lastLogTerm)

        if response[0] == True: # vote yes
            voteCount += 1
        else:
            serverTerm = response[1]
            if serverTerm > currentTerm:
                currentTerm = serverTerm
                status = 2
            pass
    except Exception as e:
        pass

def requestVoteHandler(c_term, c_id, c_lastLogIndex, c_lastLogTerm):
    if isCrash:
        raise Exception('Crash')

    global timer
    global currentTerm
    global status
    global votedFor
    timer.reset()

    response = []

    if c_term <= currentTerm:
        response = [False, currentTerm]
        return response
    else:
        my_lastLogIndex = len(log) - 1
        my_lastLogTerm = log[my_lastLogIndex][0]

        if ((c_lastLogIndex > my_lastLogIndex) or (c_lastLogIndex == my_lastLogIndex and c_lastLogTerm == my_lastLogTerm)):
            votedFor = c_id
            currentTerm = c_term
            status = 2 # become follower
            response = [True, currentTerm]
            return response
        else:
            response = [False, currentTerm]
            return response

# Updates fileinfomap
def appendEntries(server):
    """Updates fileinfomap to match that of the leader"""
    global status
    global currentTerm
    global success
    global nextIndex
    global matchIndex
    global prevLogIndex

    try:
        if newLeader:
            nextIndex[server] = len(log)
        prevLogIndex[server] = nextIndex[server]-1
        prevLogTerm[server] = log[prevLogIndex[server]][0]
            
        if success[server] and prevLogIndex[server]== len(log) - 1:
            entries =[]
        elif success[server] and prevLogIndex[server] != len(log) - 1:
            entries = log[prevLogIndex[server]:len(log)]
        else: 
            entries =[]


        leaderCommit = commitIndex
        followerTerm, success[server] = server.appendEntryHandler(currentTerm, servernum, prevLogIndex[server],\
                                prevLogTerm[server], entries, leaderCommit)

        if followerTerm > currentTerm:
            state = 2
            current_term = followerTerm

        if success[server]:
            if len(entries)>0: prevLogIndex[server] += len(entries)-1
            matchIndex[server] = prevLogIndex[server]
            nextIndex[server] = prevLogIndex[server] + 1
        else:
            if nextIndex[server] > 0:
                nextIndex[server] -= 1
    
    except Exception:
        pass


def appendEntriesHandler(l_term, l_id, l_prevLogIndex, l_prevLogTerm, entries, leaderCommit):
    if isCrash:
        raise Exception('Crash')

    global timer
    global status
    global currentTerm
    global commitIndex


    if leaderCommit < currentTerm:
        return currentTerm, False

    timer.reset()
    status = 2
    currentTerm = l_term

    if (len(log) - 1 < l_prevLogIndex) or (log[l_prevLogIndex][0] != l_prevLogTerm):
        return currentTerm, False
    
    if entries != []:
        for i,j in enumerate(range(l_prevLogIndex, l_prevLogIndex + len(entries))):
            if j >= len(log):
                log.append(entries[i])
            else:
                log[j] = entries[i]

    if leaderCommit > commitIndex:
        commitIndex = min(len(log) - 1, leaderCommit)

    
    #success
    return currentTerm, True
    




def tester_getversion(filename):
    return fileinfomap[filename][0]

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

def lifecycle():
    global status
    global currentTerm
    global timer
    global voteCount
    global success
    global newLeader
    global nextIndex
    global matchIndex
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
        
        if commitIndex > lastApplied and apply_log(lastApplied + 1):
            lastApplied += 1

        if status != 0: # not leader
            if timer.currentTime() > timer.timeout:
                timer.reset()
                status = 1
                currentTerm += 1
                voteCount = 1 # vote for myself
                th_list = []
                for sv in serverList:
                    th_list.append(threading.Thread(target = requestVote, args=(sv, )))
                    th_list[len(th_list) - 1].start()
                for th in th_list:
                    th.join()
                
                if voteCount > (numServers / 2):
                    status = 0
                    newLeader = True

        else: # is leader
            timer.setTimeout(101)
            if timer.currentTime() > timer.timeout:
                timer.reset()
                th_list = []

                if newLeader:
                    nextIndex = {}
                    matchIndex = {}
                    success = {}
                    prevLogIndex = {}
                    prevLogTerm = {}

                    for sv in serverList:
                        nextIndex[sv] = len(log)
                        matchIndex[sv] = 0
                        success[sv] = False


                for sv in serverlist:
                    th_list.append(threading.Thread(target = appendEntries, args=(sv, )))
                    th_list[len(th_list) - 1].start()
                for th in th_list:
                    th.join()

                newLeader = False

                if len(log) - 1 > commitIndex:
                    commitIndex = 1
                    for sv in serverList:
                        if sv in matchIndex.keys() and matchIndex[sv] > commitIndex:
                            commitIndex += 1
                        if commitIndex > (numServers/2) and log[commitIndex + 1][0] == currentTerm:
                            commitIndex += 1
                            




if __name__ == "__main__":
    try:
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

        numServers = len(serverList)
        status = 2
        isCrash = False
        currentTerm = 1
        votedFor = None
        log = [[0, []]]
        newLeader = False
        commitIndex = 0
        lastApplied = 0
        matchIndex = {}


        fileinfomap = dict()

        print("Attempting to start XML-RPC Server...")
        print(host, port)
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
        server.register_function(requestVoteHandler,"surfstore.requestVoteHandler")
        server.register_function(appendEntries,"surfstore.appendEntries")
        server.register_function(appendEntriesHandler,"surfstore.appendEntriesHandler")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")

        th1 = threading.Thread(target = lifecycle, )
        th1.start()

        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
