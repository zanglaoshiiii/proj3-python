import argparse
import xmlrpc.client
import os
import hashlib
import os

def get_blocks(f_name, b_size):
    """reads a file and returns a list of blocks of size b_size"""
    block_list = []
    with open(f_name, "rb") as file:
        while True:
            block =  file.read(b_size)
            if block == b"":
                break
            block_list.append(block)

    file.close()
    return block_list        

def compute_hash(block_list):
    """returns a list of hashes corresponding to the block"""
    return [hashlib.sha256(b).hexdigest() for b in block_list]

def listDiff(a, b):
    """ function will return list a - b. Order will not be preserved in output """
    return list(set(a) - set(b))

def listIntersection(a, b):
    """ function will return list a Intersection b. Order will not be preserved in output """
    return list(set(a) & set(b))

def writeFile(file_name, block_list):
    """ writes file_name in basedirectory with the data in block_list """
    with open(args.basedir + '/'+ file_name, "wb") as f:
        for b in block_list:
            f.write(b.data)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument('hostport', help='host:port of the server')
    parser.add_argument('basedir', help='The base directory')
    parser.add_argument('blocksize', type=int, help='Block size')
    args = parser.parse_args()
    
    local_list = []

    path = args.basedir
    exclude_dir = ['.git']
    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if d not in exclude_dir]  #exclude .git folder
        for f in files:
            if f[0]!='.' and f!="index.txt":
                local_list.append(f)
    #print("files:", local_list)

    local_index = {}
    with open(path + "/index.txt", "a+") as index_file:
        index_file.seek(0)
        for line in index_file:
            k = line.split()
            local_index[k[0]] = tuple((int(k[1]), k[2:len(k)]))

    index_file.close()
                   
    try:
        client  = xmlrpc.client.ServerProxy("http://" + args.hostport)
        
    ##    # Test ping
    ##        if client.surfstore.ping():
    ##            print("Ping() successful")

        remote_index = client.surfstore.getfileinfomap() # xml rpc return array as list(not tuple)
        remote_list=[]
        for f in remote_index.keys():
            remote_index[f] = tuple(remote_index[f])
            remote_list.append(f)

        del_down_files = listDiff(remote_list,local_list)     #file deleted or to be downloaded
        up_files = listDiff (local_list, remote_list)     #file to be uploaded
        exist_mod_files = listIntersection(remote_list, local_list)  #file existing but to be checked for modification

    ##        print("del_down_files: ", del_down_files)
    ##        print("up_files: ", up_files)
    ##        print("exist_mod_files: ", exist_mod_files)

        # For deleted file or downloaded file. (when file is in remote_index but not in localdirectory)
        # if deleted file, update index.txt and send update to server
        # download block, write and then update index.txt with same version
        for f in del_down_files:
            if f in local_index.keys(): # file present in index.txt but not in directory
                if local_index[f][1] != ['0'] : # file is deleted after the last sync 
                    #print("Deleted: ", f)
                    old_version =  local_index[f][0]
                    new_version = old_version + 1
                    hash_list = ["0"]
                    local_index[f] = (new_version,hash_list)

                    chekc=client.surfstore.updatefile(f, new_version, hash_list)

                    with open(path + "/index.txt", "w") as fwrite:
                        for key in local_index.keys():
                            version = local_index[key][0]
                            hash_list = local_index[key][1]
                            fwrite.write(key+" "+str(version))
                            for h in hash_list:
                                fwrite.write(" "+h)
                            fwrite.write("\n")
                else:
                    if local_index[f][0] < remote_index[f][0]:
                        version, hash_list = remote_index[f][0], remote_index[f][1]
                        block_list = []
                        for h in hash_list:
                            block_list.append(client.surfstore.getblock(h))
                        writeFile(f, block_list)

                        local_list.append(f)
                        local_index[f] = tuple((version, hash_list))
                        with open(path + "/index.txt", "w") as fwrite:
                            for key in local_index.keys():
                                version = local_index[key][0]
                                hash_list = local_index[key][1]
                                fwrite.write(key+" "+str(version))
                                for h in hash_list:
                                    fwrite.write(" "+h)
                                fwrite.write("\n")

            else: #not tombstoned 
                block_list =[]
                version = remote_index[f][0]
                hash_list = remote_index[f][1]
                if hash_list != ['0']: # write only the undeleted files
                    for h in hash_list:
                            block_list.append(client.surfstore.getblock(h))
                    writeFile(f, block_list)
                #print("downloaded: ", f)

                local_list.append(f)
                local_index[f] = tuple((version, hash_list))
            
                with open(path + "/index.txt", "a") as fwrite:
                    fwrite.write(f+" "+str(version))
                    for h in hash_list:
                        fwrite.write(" "+h)
                    fwrite.write("\n")

        # upload file. (base directory has files not in remote index or local index)
        # upload block and then update remote_index and index.txt 
        # include version conflict during uploading. surfstore.uploadfile()
        # will fail with version error
        for f in up_files:
            block_list = get_blocks(path+'/'+f, args.blocksize)
            hash_list = compute_hash(block_list)
            version = 1
            put_check = True
            for block in block_list:
                put_check = client.surfstore.putblock(block)
                if put_check==False:
                    break
            if put_check==True:
                update_check = client.surfstore.updatefile(f, version, hash_list)
            if update_check==True:
                #print("Uploaded: ", f)
                local_index[f] = tuple((version, hash_list))
                with open(path + "/index.txt", "a") as fwrite:
                    fwrite.write(f+" "+str(version))
                    for h in hash_list:
                        fwrite.write(" "+h)
                    fwrite.write("\n")
            
            else: # when other client uploads file
                # version conflict: get and download new version on server
                local_index[f] = tuple(client.surfstore.getfileinfomap()[f])
                version, hash_list = local_index[f][0], local_index[f][1]
                block_list = []
                for h in hash_list:
                    block_list.append(client.surfstore.getblock(h))
                writeFile(f, block_list)
                with open(path + "/index.txt", "w") as fwrite:
                    for key in local_index.keys():
                        version = local_index[key][0]
                        hash_list = local_index[key][1]
                        fwrite.write(key+" "+str(version))
                        for h in hash_list:
                            fwrite.write(" "+h)
                        fwrite.write("\n")
                # add file to mod_exist_files for version conflict handling later
                

        # check for file modification. This includes delete and version conflict                
        # download or upload only the modified block
        for f in exist_mod_files:
            # New client when file of same name as remote
            if f in remote_index and f in local_list and f not in local_index:
                 # when file exists in remote
                block_list =[]
                version = remote_index[f][0]
                hash_list = remote_index[f][1]
                if hash_list == ["0"]: # # When other client deleted the file and it's not synced locally. 
                    os.system("rm "+path+"/"+f)
                else:
                    for h in hash_list:
                        block_list.append(client.surfstore.getblock(h))
                    writeFile(f, block_list)
                    #print("downloaded: ", f)
                    local_list.append(f)

                local_index[f] = tuple((version, hash_list))
            
                with open(path + "/index.txt", "a") as fwrite:
                    fwrite.write(f+" "+str(version))
                    for h in hash_list:
                        fwrite.write(" "+h)
                    fwrite.write("\n")
            else:
                if remote_index[f] == local_index[f]: # Case 1: When no modification
                    block_list = get_blocks(path+'/'+f,args.blocksize)
                    hash_list = compute_hash(block_list)
                    if local_index[f][1]!=hash_list:
                          # checking for blocks server already has
                        oldblock_hashes = client.surfstore.hasblocks(hash_list)
                        put_check = True
                        for block in block_list:
                            if hashlib.sha256(block).hexdigest() not in oldblock_hashes:
                                # uploading modified blocks to server
                                put_check = client.surfstore.putblock(block)
                                if put_check==False:
                                    break
                        if put_check==True:
                            update_check = client.surfstore.updatefile(f, remote_index[f][0]+1, hash_list)
                        if update_check==True:
                            #print("Uploaded :", f)
                            local_index[f] = tuple((local_index[f][0]+1, hash_list))
                            # overwriting entire index.txt for now, use seek?
                            with open(path + "/index.txt", "w") as index_file:
                                for key in local_index.keys():
                                    line = key +" "+ str(local_index[key][0])
                                    for h in local_index[key][1]:
                                        line = line +" "+ h
                                    print(line, file = index_file)
                    
                        else: # download when version conflict
                            ''' download '''
                            remote_index[f] = tuple(client.surfstore.getfileinfomap()[f])
                            local_index[f] = remote_index[f]
                            version, hash_list = local_index[f][0], local_index[f][1]
                            block_list = []
                            for h in hash_list:
                                block_list.append(client.surfstore.getblock(h))
                            writeFile(f, block_list)
                            with open(path + "/index.txt", "w") as fwrite:
                                for key in local_index.keys():
                                    version = local_index[key][0]
                                    hash_list = local_index[key][1]
                                    fwrite.write(key+" "+str(version))
                                    for h in hash_list:
                                        fwrite.write(" "+h)
                                    fwrite.write("\n")

                    else:
                        '''No modification'''
                        pass

                else: # ( have to include consider case when file was deleted earlier)
                    version, hash_list = remote_index[f][0], remote_index[f][1]
                    #print(hash_list)
                    if hash_list == ["0"]: # # Case 2: When other client deleted the file and it's not synced locally. 
                        os.system("rm "+path+"/"+f)

                        local_index[f] = (version,hash_list)

                        with open(path + "/index.txt", "w") as fwrite:
                            for key in local_index.keys():
                                version = local_index[key][0]
                                hash_list = local_index[key][1]
                                fwrite.write(key+" "+str(version))
                                for h in hash_list:
                                    fwrite.write(" "+h)
                                fwrite.write("\n")

                    else: # Case 3: if remote has higher version of file
                        remote_version = remote_index[f][0]
                        local_version = local_index[f][0]
                        if remote_version > local_version:
                            hash_list = remote_index[f][1]

                            block_list=[]
                            for h in hash_list:
                                block_list.append(client.surfstore.getblock(h))
                            local_index[f] = (remote_version,hash_list)

                            #write file
                            writeFile(f, block_list)
                            #print("Saved: ", f)
                            # write index.txt        
                            with open(path + "/index.txt", "w") as fwrite:
                                for key in local_index.keys():
                                    version = local_index[key][0]
                                    hash_list = local_index[key][1]
                                    fwrite.write(key+" "+str(version))
                                    for h in hash_list:
                                        fwrite.write(" "+h)
                                    fwrite.write("\n")

    except Exception as e:
        print("Client: " + str(e))

        
