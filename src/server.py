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
    if isCrash or state != 0:
        raise Exception('exception')
    global log
    
    log.append([current_term, [filename, version, hashlist]])
    lastIndex = len(log) - 1

    # block
    while commitIndex < lastIndex:
        pass

    return True


def apply(log_index):
    filename, version, hashlist = log[log_index][1]
    if filename in fileinfomap.keys():
        last_version = fileinfomap[filename]
        if (version == last_version[0]+1):
            fileinfomap[filename] = tuple((version, hashlist))
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
    if state == 0:
        return True
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

def getVersion(filename):
    "gets version number of file from server"
    
    return fileinfomap[filename][0]
    

def requestVote(cl):
    global vote_counter
    global current_term
    global state

    if isCrash:
        raise Exception('Crashed')
    
    last_log_index = len(log) - 1
    last_log_term = log[-1][0]
    # print("current_term",current_term)
    try:
        vote_response = cl.voteHandler(current_term, servernum, last_log_index, last_log_term )
        # print("vote_response",vote_response)
        # print("vote requested from: ", cl)
        if vote_response[0]: # [true, current term]
            vote_counter +=1
        else:
            if vote_response[1]>current_term:
                current_term = vote_response[1]
                state = 2
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
        global state
        voted_for = cand_id
        current_term = cand_term
        state = 2
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
    global state
    global current_term
    global success
    global prev_log_term

    try:
        if new_leader:
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
        follower_term, success[cl] = cl.appendEntryHandler(current_term, servernum, prev_log_index[cl],\
                                prev_log_term[cl], entries, leader_commit)
        #success True if follower[next_index] matches any entry in leader or it has just been appended

        if follower_term > current_term:
            state = 2
            current_term = follower_term

        if success[cl]:
            if len(entries)>0: prev_log_index[cl] += len(entries)-1
            match_index[cl] = prev_log_index[cl]
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
    global current_term
    global state
    global commitIndex

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

    state = 2  #*** maybe
    current_term = leader_term
    timer.reset()
    print("hearbeat from "+ str(leader_id)+" in term: "+str(current_term))
    print("received entries:", entries)

    if len(log)-1< prev_log_index or log[prev_log_index][0] != prev_log_term:  #*** maybe
        return current_term, False

    if entries != []:
        appendLog()

    if leader_commit > commitIndex:
        commitIndex = min(leader_commit, len(log)-1)

        
    return current_term, True 

def raftHandler():
    global current_term
    global state
    global vote_counter
    global timer
    global new_leader
    global next_index
    global match_index
    global success
    global prev_log_index
    global prev_log_term
    global commitIndex
    global last_applied

    timer = MyTimer()
    timer.reset()
    while True:
        while isCrash:
            state = 2
            pass
        if commitIndex > last_applied:
            if apply(last_applied+1):
                last_applied += 1
        if state !=0:
            if timer.currentTime() > timer.timeout:
                state = 1  # candidate
                current_term +=1
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
                if vote_counter > (num_servers/2):
                    state = 0 #leader elected
                    new_leader = True
                    print("I am the leader in term: " + str(current_term) +", votes: " + str(vote_counter))
                    print(log)
                    # immediately send hearbeat here somehow
        else: # leader
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
                        next_index[cl] = len(log) # [initialize]
                        match_index[cl] = 0
                        success[cl] = False


                for cl in serverList:
                    th12_list.append(threading.Thread(target = appendEntries, args=(cl, )))
                    th12_list[-1].start()
                for t in th12_list:
                    t.join()
                #commitIndex = min([match_index[cl] for cl in serverList]) #*** to be implemented
                new_leader = False

                if len(log)-1 > commitIndex:
                    commit_count = 1
                    for cl in serverList:
                        if cl in match_index.keys() and match_index[cl]>commitIndex:
                            commit_count += 1
                    if commit_count > (num_servers/2) and log[commitIndex+1][0]==current_term:
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

    # config_file = args.config_file
    # servernum = int(args.servernum)

    # server_info = {}

    # with open(config_file,'r') as file:
    #     next(file)
    #     for line in file:
    #         server_info[int(line.split(' ')[0][-2])] = line.strip().split(' ')[1]

    # address, port = server_info[servernum].split(':')
    # port = int(port)
    # print(port)

    num_servers = len(serverList) + 1
    state = 2   # 0: Leader; 1: Candidate; 2: Follower
    isCrash = False
    current_term = 1
    voted_for = None
    log = [[0,0]] # [[term,data]]
    new_leader = False
    commitIndex = 0
    last_applied = 0
    match_index = {}

    # print("Attempting to start XML-RPC Server at "+ address+":"+str(port))
    server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
    # th1 = threading.Thread(target = raftThread)
    
    # serverList = []
    # for i in server_info.keys():
    #     if i!=servernum:
    #         cl = xmlrpc.client.ServerProxy("http://"+server_info[i])
    #         serverList.append(cl)

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
    server.register_function(getVersion, "surfstore.tester_getversion")

    server.register_function(voteHandler,"voteHandler")
    server.register_function(appendEntryHandler, "appendEntryHandler")
    # server.register_function()

    print("Started successfully.")
    print("Accepting requests. (Halt program to stop.)")

    th1 = threading.Thread(target = raftHandler, )
    th1.start()

    server.serve_forever()