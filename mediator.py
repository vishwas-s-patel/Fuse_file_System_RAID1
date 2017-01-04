import xmlrpclib, unittest, getopt, sys
from datetime import datetime, timedelta
import SimpleXMLRPCServer, pickle
from xmlrpclib import Binary
from sys import argv
from multiprocessing import Pool
import socket
import time

Qr = 0
Qw = 0
Nreplicas = 0
server_ports = []
meta_port = []
data_ports = []
repair_restarted_server_list = []

class mediator_server:
    def __init(self):
        global Qr, Qw
        Qr = 0
        Qw = 0

    def repair_server(self):
        global repair_restarted_server_list
        global data_ports
       
        print "In repair_server"
        for i in range(len(repair_restarted_server_list)):
            retry = 0
            max_repair_retry = 6 
            while True:
                try:
                    retry += 1
                    if (max_repair_retry+1) != retry:
                        running_one = xmlrpclib.Server(repair_restarted_server_list[i])

                        key_list = running_one.list_contents()
                        
                        for j in range(len(key_list)):
                            res = self.get(key_list[j])
                            if "value" in res:
                                res1 =  pickle.loads(res["value"].data)
                            else:
                                res1 = None

                            correct_server = xmlrpclib.Server(repair_restarted_server_list[i])
                            correct_server.put(key, Binary(pickle.dumps(res1)), 6000)

                        repair_restarted_server_list.pop(i)
                        break
                    else:
                        break
                    
                except socket.error:
                    print "retrying for restarted server to repair"

    # Quorum voter, if data from multiple servers is same and the count of same data is 
    # equal to or greater than Qr(read Quorum), then read of data is successful
    def Quorum_voter(self, key, data_multi_servers):
        global Qr
        Count = []
        different_data = []
        Found = 0
        NFound = 0
        repair_list = []
        different_data.append(data_multi_servers[0])
        
        print "inside Quorum"
        # If different data are received from the servers and do not match, 
        # unmatched data are appended to form a list
        for i in range(1, len(data_multi_servers)):
            Found = 0
            NFound = 0
            for j in range(len(different_data)):
                if (different_data[j] == data_multi_servers[i]):
                    Found = 1
                else:
                    NFound = 1
            if (NFound == 1 and Found == 0):
                different_data.append(data_multi_servers[i])
     
        # Count number of matched data and form a list
        for i in range(len(different_data)):
            Count.append(0)
            for j in range(len(data_multi_servers)):
                if (different_data[i] == data_multi_servers[j]):
                    Count[i] += 1                        # Increment the count if same data is received from different server

        #init pos to None
        pos = None
        
        # Compare the Count with minimum read Quorum servers required
        # If we couldn't find the position which is equal or greater than Qr, then pos will be None
        for i in range(len(Count)):
            if int(Count[i]) >= int(Qr):
                pos = i
                
        if pos == None:
            print "read failed"
            return None
        else:
            # find the position in data_multi_servers which is the correct data 
            for i in range(len(data_multi_servers)):
                if (different_data[int(pos)] == data_multi_servers[i]):
                    pos_in_data_multi_servers = i
                    break
            
            # find the list of servers which are corrupted
            for i in range(len(data_multi_servers)):
                if (different_data[pos] != data_multi_servers[i]):
                    repair_list.append(i)

            return pos_in_data_multi_servers
    
    def put(self,key,value,ttl):
        global meta_port, data_ports, repair_restarted_server_list
        max_get_retry = 6
        retry = 0

        if (key.data[-4:] == 'data'):
            for i in range(len(data_ports)):
                retry = 0
                while True:
                    try:
                        retry += 1
                        if (max_get_retry+1) != retry:
                            file_data_to_server = xmlrpclib.Server(data_ports[i])
                            file_data_to_server.put(key, value, ttl)
                            break
                        else:
                            if repair_restarted_server_list.count(data_ports[i]) == 0:
                                repair_restarted_server_list.append(data_ports[i])
                            print "repair list in put"
                            print repair_restarted_server_list
                            break
                
                    except socket.error:
                        time.sleep(5)
           
            # call repair_server everytime, if there are any servers which needs
            # to be repaired will have in the global list 'repair_restarted_server_list'
            self.repair_server()
        else:
            meta_data_to_server = xmlrpclib.Server(meta_port)
            meta_data_to_server.put(key, value, ttl)

        return True

    def get(self,key):
        global meta_port, data_ports
        global Qr
        
        data_multi_servers = []
        unmarshaled_data_multi_servers = []
        max_get_retry = 3
        retry = 0
        if (key.data[-4:] == 'data'):
            for i in range(len(data_ports)):
                retry = 0
                while True:
                    try:
                        retry += 1
                        if (max_get_retry+1) != retry:
                            file_data_from_server = xmlrpclib.Server(data_ports[i])
                            temp = file_data_from_server.get(key)
                            if temp != {}:
                                unmarshaled_data_multi_servers.append(temp)
                                temp1 = pickle.loads(temp["value"].data)
                                data_multi_servers.append(temp1)
                            break
                        else:
                            temp1 = "server is down, need quorom to know tat it is down"
                            data_multi_servers.append(temp1)
                            if repair_restarted_server_list.count(data_ports[i]) == 0:
                                repair_restarted_server_list.append(data_ports[i])
                            print "repair list in get"
                            print repair_restarted_server_list
                            break

                    except socket.error:
                        time.sleep(1)
           
            pos = self.Quorum_voter(key, data_multi_servers)
            self.repair_server()

            return unmarshaled_data_multi_servers[pos]
        else:
            meta_data_from_server = xmlrpclib.Server(meta_port)
            meta_data = meta_data_from_server.get(key)

            return meta_data

# main() function which reads meta and data ports from the command line 
# start the mediator server waiting for client
def main():
    global Qr, Qw, Nreplicas, server_ports, meta_port, data_ports

    Qr = argv[1]
    Qw = argv[2]

    # mediator_port hard-coded to 8000
    mediator_port = 8000
    
    # meta data server port
    meta_port = 'http://127.0.0.1:' + str(argv[3])
    print meta_port
   
    # file data server ports
    for i in range(4, len(argv)):
        print i
        print argv[i]
        data_ports.append('http://127.0.0.1:' + str(argv[i]))
        print data_ports
        server_ports.append(int(argv[i]))
    
    serve(8000)
  
# Start the xmlrpc server
def serve(port):
    med_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
    med_server.register_introspection_functions()
    sht = mediator_server()
    med_server.register_function(sht.put, "put")
    med_server.register_function(sht.get, "get")
    print "listening to port: " + str(port)
    med_server.serve_forever()

# begin executing from main() function
if __name__ == "__main__":
    main()
