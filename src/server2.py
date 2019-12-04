from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn

import argparse
import hashlib
import xmlrpc.client
import random
import time
import threading

BlockStore = {}
FileInfoMap = {}

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

class timerClass():
    '''Timer'''
    def __init__(self):
        self.t_a = 1500
        self.t_b = 2000
        self.start = int(time.time()*1000)
        self.timeout = random.randint(self.t_a,self.t_b)

    def now(self):
        return int(time.time()*1000) - self.start

    def reset(self):
        self.start = int(time.time()*1000)
        self.timeout = random.randint(self.t_a,self.t_b)

    def setTimeout(self, reset_time = None):
        if reset_time != None:
            self.timeout = reset_time
        else:
            self.timeout = random.randint(self.t_a,self.t_b)



# A simple ping, returns true
def ping():
    """A simple ping method"""
    #print("Ping()")
    return True

# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    # print("GetBlock(" + h + ")")

    #blockData = bytes(4)
    blockData = BlockStore[h]
    #print(BlockStore)
    return blockData

# Puts a block
def putblock(b):
    """Puts a block"""
    # print("PutBlock()", b)

    h = hashlib.sha256(b.data).hexdigest()
    # print("index: ", h)
    BlockStore[h] = b.data
    return True

# Given a list of hashes, return the subset that are on this server
def hasblocks(hashlist):
    """Determines which blocks are on this server"""
    # print("HasBlocks()")

    haslist = []
    haslist = [hashes for hashes in hashlist if hashes in BlockStore.keys()]

    return haslist

# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    # print("GetFileInfoMap()")
    

    result = FileInfoMap
    return result

# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    # print("UpdateFile()")
    global log
    # ******* add log entries
    # log.append([current_term, ]) # check with others for their commits
    log.append([current_term, [filename, version, hashlist]])

    # if filename in FileInfoMap.keys():
    #     #file already exist in cloud
    #     last_version = FileInfoMap[filename]
    #     if (version == last_version[0]+1):
    #         FileInfoMap[filename] = tuple((version, hashlist))
    #     else:
    #         "send error"
    #         return False
    # else:
    #     #new file (version should be 1)
    #     FileInfoMap[filename] = tuple((version, hashlist))
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
    global is_crashed
    print("Crash()")
    is_crashed = True
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    global is_crashed
    print("Restore()")
    is_crashed = False
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return is_crashed


def requestVote(client):
    global vote_counter
    global current_term
    global state
    # try:
    if not log:
        last_log_index = 0
        last_log_term = 0
    else:
        last_log_index = len(log)
        last_log_term = log[-1][0]
    try:
        vote_response = client.voteHandler(current_term, idx, last_log_index, last_log_term )
        print("vote requeted")
        if vote_response[0]: # [true, current term]
            vote_counter +=1
        else:
            if vote_response[1]>current_term:
                current_term = vote_response[1]
                state = 2
            
    except (ConnectionRefusedError):
        pass
        # print("ConnectionRefusedError")

def voteHandler(cand_term, cand_id, cand_last_log_index, cand_last_log_term):
    global timer
    timer.reset()

    def castVote():
        global voted_for
        global current_term
        global state
        voted_for = cand_id
        current_term = cand_term
        state = 2
        print("casting vote")
        return [True, current_term]

    if not log:
        last_log_index = 0
        last_log_term = 0
    else:
        last_log_index = len(log)
        last_log_term = log[-1][0]
    
    print("last_log_term", last_log_term)

    if current_term < cand_term:
        if cand_last_log_index > last_log_index:
            return castVote()
        elif cand_last_log_index == last_log_index \
                and cand_last_log_term == last_log_term:
            return castVote()
        else:
            return [False, current_term] 
    else:
        print("Sorry no voting")
        return [False, current_term]    


def appendEntries(cl):
    # global next_index
    global match_index
    # global prev_log_index
    global state
    global current_term

    # print("@@@@@@@@@@@@@@@@@")
    # print(next_index)
    # print("@@@@@@@@@@@@@@@@@@")
    try:
        entries =[]
        # prev_log_index = 0
        if new_leader:
            if log:
                entries = log[:len(log)]
            else:
                entries = []
                match_index[cl] = -1
        else:
            if len(log) > match_index[cl] + 1:
                entries = log[match_index[cl]+1: len(log)]

        # leader_commit = commit_index
        follower_term, success = cl.appendEntryHandler(current_term, idx, match_index[cl], entries)
        #success True if follower[next_index] matches any entry in leader
        if follower_term > current_term:
            state = 2
            current_term = follower_term
        if success:
            match_index[cl] = len(log)-1

        print(log)
    except (ConnectionRefusedError):
    # Exception as e: # if some server not alive
        # print("in except for " + str(client))       
        pass

    # self.timer.reset()
    # client = xmlrpc.client.ServerProxy("http://" + server_info[voter_id])
    # client.heartbeatHandler(self.id, self.currentTerm)

def appendEntryHandler(leader_term, leader_id, from_index, entries):
    print(log)

    global timer
    global current_term
    global state

    def appendLog(from_index):
        print("in appendLog")
        # print(prev_log_index, prev_log_index + len(entries))
        for i,j in enumerate(range(from_index, from_index + len(entries))):
            if j < len(log):
                log[j] = entries[i]
            else:
                log.append(entries[i])

    state = 2
    success = False
    print("received heartbeat by: " + str(leader_id)+" in term " + str(leader_term))
    if current_term > leader_term:
        return current_term, success
    else:
        current_term = leader_term
    
    
    # if from_index > len(log):
    #     request_all

    print("my term: ", current_term)
    timer.reset()
    if entries != []:
        appendLog(from_index)
        success = True
        
    # if leader_commit > commit_index:
    #         commit_index = min(leader_commit, len(log))
        
    return current_term, success 

def raftHandler():
    global current_term
    global state
    global vote_counter
    global timer
    global new_leader
    # global next_index
    global match_index

    timer = timerClass()
    timer.reset()
    while True:
        if state !=0:
            if timer.now() > timer.timeout:
                state = 1  # candidate
                current_term +=1
                vote_counter = 0 #initialized
                vote_counter += 1 # vote for self
                timer.reset()
                th11_list = []
                for cl in client_list:
                    th11_list.append(threading.Thread(target = requestVote, args=(cl, )))
                    th11_list[-1].start()
                for t in th11_list:
                    t.join()
                #print(vote_counter)
                if vote_counter > (num_servers/2):
                    state = 0 #leader elected
                    new_leader = True
                    print("I am the leader in term: " + str(current_term) +", votes: " + str(vote_counter))
                    # immediately send hearbeat here somehow
        else: # leader
            timer.setTimeout(700)  #*** can so timer.set_timeout(1000)
            if timer.now() > timer.timeout:
                timer.reset()
                th12_list= []
                if new_leader:
                    # next_index ={}
                    match_index = {}
                    for cl in client_list:
                    # print("before appendEntries")
                        # next_index[cl] = [len(log), False] # [initialize, success]
                        match_index[cl] = 0

                for cl in client_list:
                    th12_list.append(threading.Thread(target = appendEntries, args=(cl, )))
                    th12_list[-1].start()
                for t in th12_list:
                    t.join()
                new_leader = False


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="SurfStore server")
    parser.add_argument('config_file', help='path to config file')
    parser.add_argument('idx', help='server id')
    args = parser.parse_args()

    config_file = args.config_file
    idx = int(args.idx)

    server_info = {}

    with open(config_file,'r') as file:
        next(file)
        for line in file:
            server_info[int(line.split(' ')[0][-2])] = line.split(' ')[1][:-1]

    address, port = server_info[idx].split(':')
    port = int(port)

    num_servers = len(server_info)
    state = 2   # 0: Leader; 1: Candidate; 2: Follower
    is_crashed = False
    current_term = 1
    voted_for = None
    log = [] # [[term,data]]
    # prev_log_index = 0
    new_leader = False
    commit_index = 0
    # last_applied = 0

    print("Attempting to start XML-RPC Server at "+ address+":"+str(port))
    server = threadedXMLRPCServer((address, port), requestHandler=RequestHandler)
    # th1 = threading.Thread(target = raftThread)
    
    client_list = []
    for i in server_info.keys():
        if i!=idx:
            cl = xmlrpc.client.ServerProxy("http://"+server_info[i])
            client_list.append(cl)

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

    server.register_function(voteHandler,"voteHandler")
    server.register_function(appendEntryHandler, "appendEntryHandler")
    # server.register_function()

    print("Started successfully.")
    print("Accepting requests. (Halt program to stop.)")

    th1 = threading.Thread(target = raftHandler, )
    th1.start()

    server.serve_forever()
    # except Exception as e:
    #     print("Server: " + str(e))


        # idx from config.ini

        # server = serverClass(localhost,8080, idx)

        # for idx in all_ids:
        #     server.requestVote(idx)
