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
    # print("Getfileinfomap()")

    return fileinfomap

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    if isCrash or status != 0:
        raise Exception('exception')
    global log

    log.append([currentTerm, [filename, version, hashlist]])
    lastIndex = len(log) - 1

    # block
    while commitIndex < lastIndex:
        continue

    return True


def apply(index):
    filename, version, hashlist = log[index][1]
    if filename in fileinfomap.keys():
        latest_version = fileinfomap[filename][0]
        if (version == latest_version + 1):
            fileinfomap[filename] = [version, hashlist]
            return True
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

def tester_getversion(filename):
    "gets version number of file from server"
    
    return fileinfomap[filename][0]
    

def requestVote(cl):
    if isCrash:
        raise Exception('Crashed')
    
    global vote_counter
    global currentTerm
    global status


    
    last_log_index = len(log) - 1
    last_log_term = log[last_log_index][0]
    try:
        vote_response = cl.voteHandler(currentTerm, servernum, last_log_index, last_log_term )

        if vote_response[0]: # [true, current term]
            vote_counter +=1
        else:
            if vote_response[1]>currentTerm:
                currentTerm = vote_response[1]
                status = 2
            pass
            
    except Exception:
        pass

def voteHandler(cand_term, cand_id, cand_last_log_index, cand_last_log_term):
    if isCrash:
        raise Exception('Crashed')

    global timer
    global voteFor
    global currentTerm
    global status

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
        last_log_index = len(log)-1
        last_log_term = log[-1][0]
        print("last_log_term", last_log_term)

        if cand_last_log_index > last_log_index:
            voteFor = cand_id
            currentTerm = cand_term
            status = 2
            return [True, currentTerm]
        elif cand_last_log_index == last_log_index \
                and cand_last_log_term == last_log_term:
            voteFor = cand_id
            currentTerm = cand_term
            status = 2
            return [True, currentTerm]
        else:
            return [False, currentTerm]    


def appendEntries(cl):
    global next_index
    global matchIndex
    global prev_log_index
    global status
    global currentTerm
    global success
    global prev_log_term

    try:
        if newLeader:
            next_index[cl] = len(log)
        prev_log_index[cl] = next_index[cl]-1
        prev_log_term[cl] = log[prev_log_index[cl]][0]
            
        if success[cl] and prev_log_index[cl]== len(log)-1:
            #print("it's a success")
            entries =[]
        elif success[cl] and prev_log_index[cl] != len(log)-1 :
            entries = log[prev_log_index[cl]:len(log)]
        else: # not success, last entries didn't match until next entry reaches at a point
            entries =[]
            #print("try again, next index: ",next_index[cl])

        leader_commit = commitIndex
        follower_term, success[cl] = cl.appendEntryHandler(currentTerm, servernum, prev_log_index[cl],\
                                prev_log_term[cl], entries, leader_commit)
        #success True if follower[next_index] matches any entry in leader or it has just been appended

        if follower_term > currentTerm:
            status = 2
            currentTerm = follower_term

        if success[cl]:
            if len(entries)>0: prev_log_index[cl] += len(entries)-1
            matchIndex[cl] = prev_log_index[cl]
            next_index[cl] = prev_log_index[cl]+1
        else:
            if next_index[cl] > 0:
                next_index[cl] -= 1

    except Exception as e: 
        # print("Exception in appendEntries: ", e)
        pass

def appendEntryHandler(leader_term, leader_id, prev_log_index,\
                        prev_log_term, entries, leader_commit):
    #print("log",log)
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
        print(prev_log_index, prev_log_index + len(entries))
        for i,j in enumerate(range(prev_log_index, prev_log_index + len(entries))):
            if j < len(log):
                log[j] = entries[i]
            else:
                log.append(entries[i])

    status = 2  #*** maybe
    currentTerm = leader_term
    timer.reset()
    print("hearbeat from "+ str(leader_id)+" in term: "+str(currentTerm))
    print("received entries:", entries)

    if len(log)-1< prev_log_index or log[prev_log_index][0] != prev_log_term:  #*** maybe
        return currentTerm, False

    if entries != []:
        appendLog()

    if leader_commit > commitIndex:
        commitIndex = min(leader_commit, len(log)-1)

        
    return currentTerm, True 

def raftHandler():
    global currentTerm
    global status
    global vote_counter
    global timer
    global newLeader
    global next_index
    global matchIndex
    global success
    global prev_log_index
    global prev_log_term
    global commitIndex
    global last_applied

    timer = MyTimer()
    timer.reset()
    while True:
        while isCrash:
            status = 2
            pass
        if commitIndex > last_applied:
            if apply(last_applied+1):
                last_applied += 1
        if status !=0:
            if timer.currentTime() > timer.timeout:
                status = 1  # candidate
                currentTerm +=1
                vote_counter = 0 #initialized
                vote_counter += 1 # vote for self
                timer.reset()
                th11_list = []
                for cl in serverList:
                    th11_list.append(threading.Thread(target = requestVote, args=(cl, )))
                    th11_list[-1].start()
                for t in th11_list:
                    t.join()
                #print(vote_counter)
                if vote_counter > (numServers/2):
                    status = 0 #leader elected
                    newLeader = True
                    print("I am the leader in term: " + str(currentTerm) +", votes: " + str(vote_counter))
                    print(log)
                    # immediately send hearbeat here somehow
        else: # leader
            timer.setTimeout(100)
            if timer.currentTime() > timer.timeout:
                timer.reset()
                th12_list= []
                if newLeader:
                    next_index ={}
                    matchIndex = {}
                    success={}
                    prev_log_index = {}
                    prev_log_term = {}
                    for cl in serverList:
                        next_index[cl] = len(log) # [initialize]
                        matchIndex[cl] = 0
                        success[cl] = False


                for cl in serverList:
                    th12_list.append(threading.Thread(target = appendEntries, args=(cl, )))
                    th12_list[-1].start()
                for t in th12_list:
                    t.join()
                #commitIndex = min([matchIndex[cl] for cl in serverList]) #*** to be implemented
                newLeader = False

                if len(log)-1 > commitIndex:
                    commit_count = 1
                    for cl in serverList:
                        if cl in matchIndex.keys() and matchIndex[cl]>commitIndex:
                            commit_count += 1
                    if commit_count > (numServers/2) and log[commitIndex+1][0]==currentTerm:
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
    # parser.add_argument('config_file', help='path to config file')
    # parser.add_argument('servernum', help='server id')
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
    status = 2   # 0: Leader; 1: Candidate; 2: Follower
    isCrash = False
    currentTerm = 1
    voteFor = None
    log = [] # [[term,data]]
    newLeader = False
    commitIndex = 0
    last_applied = 0
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
    server.register_function(tester_getversion, "surfstore.tester_getversion")

    server.register_function(voteHandler,"voteHandler")
    server.register_function(appendEntryHandler, "appendEntryHandler")
    # server.register_function()

    print("Started successfully.")
    print("Accepting requests. (Halt program to stop.)")

    th1 = threading.Thread(target = raftHandler, )
    th1.start()

    server.serve_forever()