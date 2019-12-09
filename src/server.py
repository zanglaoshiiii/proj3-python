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

    
    log.append([current_term, [filename, version, hashlist]])
    last_index = len(log) - 1

    # block
    while commit_index <last_index:
        pass

    return True


def apply(index):
    filename, version, hashlist = log[index][1]
    if filename in fileinfomap.keys():
        last_version = fileinfomap[filename]
        if (version == last_version[0] + 1):
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
    

def requestVote(cl):
    global vote_counter
    global current_term
    global status

    if isCrash:
        raise Exception('Crashed')
    
    last_log_index = len(log) - 1
    last_log_term = log[-1][0]

    try:
        vote_response = cl.voteHandler(current_term, servernum, last_log_index, last_log_term )

        if vote_response[0]: 
            vote_counter +=1
        else:
            if vote_response[1]>current_term:
                current_term = vote_response[1]
                status = 2
            pass
            
    except Exception as e:
        #print(e)
        pass

def voteHandler(cand_term, cand_id, cand_last_log_index, cand_last_log_term):
    if isCrash:
        raise Exception('Crashed')
    global timer
    timer.reset()

    def castVote():
        global voted_for
        global current_term
        global status
        voted_for = cand_id
        current_term = cand_term
        status = 2
        print("casting vote to", cand_id)
        return [True, current_term]

    if cand_term <= current_term:
        return [False, current_term]
    else:
        last_log_index = len(log)-1
        last_log_term = log[-1][0]
        print("last_log_term", last_log_term)

        if cand_last_log_index > last_log_index:
            return castVote()
        elif cand_last_log_index == last_log_index \
                and cand_last_log_term == last_log_term:
            return castVote()
        else:
            return [False, current_term]    


def appendEntries(cl):
    global next_index
    global match_index
    global prev_log_index
    global status
    global current_term
    global success
    global prev_log_term

    try:
        if new_leader:
            next_index[cl] = len(log)
        prev_log_index[cl] = next_index[cl]-1
        prev_log_term[cl] = log[prev_log_index[cl]][0]
            
        if success[cl] and prev_log_index[cl]== len(log)-1:

            entries =[]
        elif success[cl] and prev_log_index[cl] != len(log)-1 :
            entries = log[prev_log_index[cl]:len(log)]
        else:
            entries =[]


        leader_commit = commit_index
        follower_term, success[cl] = cl.appendEntryHandler(current_term, servernum, prev_log_index[cl],\
                                prev_log_term[cl], entries, leader_commit)

        if follower_term > current_term:
            status = 2
            current_term = follower_term

        if success[cl]:
            if len(entries)>0: prev_log_index[cl] += len(entries)-1
            match_index[cl] = prev_log_index[cl]
            next_index[cl] = prev_log_index[cl]+1
        else:
            if next_index[cl] > 0:
                next_index[cl] -= 1

    except Exception as e: 

        pass

def appendEntryHandler(leader_term, leader_id, prev_log_index,\
                        prev_log_term, entries, leader_commit):

    if isCrash:
        raise Exception('Crashed')

    global timer
    global current_term
    global status
    global commit_index

    if leader_term < current_term:
        return current_term, False

    def appendLog():
        print("in appendLog")
        print(prev_log_index, prev_log_index + len(entries))
        for i,j in enumerate(range(prev_log_index, prev_log_index + len(entries))):
            if j < len(log):
                log[j] = entries[i]
            else:
                log.append(entries[i])

    status = 2
    current_term = leader_term
    timer.reset()
    print("hearbeat from "+ str(leader_id)+" in term: "+str(current_term))
    print("received entries:", entries)

    if len(log)-1< prev_log_index or log[prev_log_index][0] != prev_log_term:
        return current_term, False

    if entries != []:
        appendLog()

    if leader_commit > commit_index:
        commit_index = min(leader_commit, len(log)-1)

        
    return current_term, True 

def raftHandler():
    global current_term
    global status
    global vote_counter
    global timer
    global new_leader
    global next_index
    global match_index
    global success
    global prev_log_index
    global prev_log_term
    global commit_index
    global last_applied

    timer = MyTimer()
    timer.reset()
    while True:
        while isCrash:
            status = 2
            pass
        if commit_index > last_applied:
            if apply(last_applied+1):
                last_applied += 1
        if status !=0:
            if timer.currentTime() > timer.timeout:
                status = 1  
                current_term +=1
                vote_counter = 0 
                vote_counter += 1 
                timer.reset()
                th11_list = []
                for cl in serverList:
                    th11_list.append(threading.Thread(target = requestVote, args=(cl, )))
                    th11_list[-1].start()
                for t in th11_list:
                    t.join()

                if vote_counter > (num_servers/2):
                    status = 0 
                    new_leader = True
                    print("I am the leader in term: " + str(current_term) +", votes: " + str(vote_counter))
                    print(log)

        else: 
            timer.setTimeout(100)
            if timer.currentTime() > timer.timeout:
                timer.reset()
                th12_list= []
                if new_leader:
                    next_index ={}
                    match_index = {}
                    success={}
                    prev_log_index = {}
                    prev_log_term = {}
                    for cl in serverList:
                        next_index[cl] = len(log) 
                        success[cl] = False


                for cl in serverList:
                    th12_list.append(threading.Thread(target = appendEntries, args=(cl, )))
                    th12_list[-1].start()
                for t in th12_list:
                    t.join()

                new_leader = False

                if len(log)-1 > commit_index:
                    commit_count = 1
                    for cl in serverList:
                        if cl in match_index.keys() and match_index[cl]>commit_index:
                            commit_count += 1
                    if commit_count > (num_servers/2) and log[commit_index+1][0]==current_term:
                        commit_index += 1
                    print("Leader commit index: ", commit_index)

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


    num_servers = len(serverList) + 1
    status = 2  
    isCrash = False
    current_term = 1
    voted_for = None
    log = [[0,0]]
    new_leader = False
    commit_index = 0
    last_applied = 0
    match_index = {}


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

    th1 = threading.Thread(target = raftHandler, )
    th1.start()

    server.serve_forever()