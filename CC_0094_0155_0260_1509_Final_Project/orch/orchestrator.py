#!/usr/bin/env python
from flask import Flask,render_template,jsonify,request,abort,Response
from flask_cors import CORS
import threading
import os
import time
import sqlite3
import requests
import re
import math
import csv
import docker
import datetime
import json 
import pika
import sys
import uuid
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging




logging.basicConfig()


# Intialisation of Flask "app"
app = Flask(__name__)


# To support Cross Origin Resorce Sharing 
CORS(app)




# Establishing Connection with the Server
zk = KazooClient(hosts='zoo:2181')
zk.start()
zk.ensure_path("/orchestrator")




# master_container_detail and slave_container_detail stores the "PID" of the worker as the key and the respective docker object as the value.
master_container_detail={}
slave_container_detail={}

# first_request flag is used to ensure that the request count timer starts immediately after the first_request is received as well as to initialise a master
# and a slave. first_request is set to "False" after this is done.
first_request = True

# Since the zookeeper event gets triggered right after running the python file, we use this variable to check for this event to ensure that no changes are made
# as this is not exactly an event that should be considered
first_zoo_event_req = True


# crash_pid_flag is used to check if a worker was killed voluntarily by the user.
crash_pid_flag = 0


# The zookeeper watcher event which watches over all the znodes present in the "/orchestrator" path.
@zk.ChildrenWatch("/orchestrator")
def f(ch):
    print()
    print("Orchestrator:(f()) Event Just got Triggered!")
    global first_zoo_event_req
    global crash_pid_flag

    # If it is the "first_zoo_event_req", it is useless and has to be ignored.
    if(first_zoo_event_req):
        first_zoo_event_req = False


    else:
        print(ch)
        
        # Check if "/api/v1/crash/slave" was to kill the slave (or) if it was because of scaling up or down.
        # As we do not have to add a new slave when they are killed because of scaling down.
        if(crash_pid_flag):
            crash_pid_flag=0
            print("Orchestrator:(f()) Adding a Slave as the previous one crashed!")
            requests.post("http://localhost:80/api/v1/create/slave")

        # This case is reached when a worker is created (or) deleted not according to the above case (Line 79)
        else:
            
            m=0
            lowest=-1
            corres_c=""

            # We look at active znodes present in the list "ch" to check if a "master" exists.
            # When creating a worker, everything is initially made into a "slave" and its znode-data is set to "slave"
            # Here we compare the znode-data to check if any of the worker is the "master", if not, we make the slave with
            # the lowest PID to run as the master.
            for c in ch:
                print("Orchestrator:(f()) Iteration at 'c':",c," & type of 'c':",type(c))
                
                # We create the znode as "/orchestrator/worker,<PID of the worker>", hence we retrieve
                # it the same way here. 'm_or_s' stores the znode-data to determine if it is a "master" or "slave"
                d,s = zk.get("/orchestrator/"+c)
                m_or_s = d.decode("utf-8").split(",")[0]
                pid = int(d.decode("utf-8").split(",")[1])

                # Checking whether a "master" exists
                if(m_or_s == "master"):
                        m=1
                        print("Orchestrator:(f()) Master Exists!")
                
                # If "slave", finding to lowest PID and keeping track of it for "master" election
                else:
                    if(lowest==-1):
                        lowest=pid
                    if(pid<=lowest):
                        lowest=pid
                        corres_c=c
                            
            # If "master" isn't present, making the lowest PID worker(slave) as the "master" and creating another "slave"
            # at the end as the slave count is getting reduced by 1.
            if(m==0):
                print("Orchestrator:(f()) As Master wasn't found, changing Slave to Master")
                strin="master,"+str(lowest)
                # We set the znode-data as "master" now (It was "slave" before). This triggers a znode event in the "worker"
                # directly and makes it convert from a master to a slave. We also add the new master to the master_container_detail
                # dictionary and remove the respective slave from slave_container_detail dictionary to keep track of everything.
                zk.set("/orchestrator/"+corres_c,strin.encode('utf-8'))
                print("Orchestrator:(f()) /orchestrator/"+corres_c+" is the New Master!")
                master_container_detail[lowest] = slave_container_detail[lowest]
                slave_container_detail.pop(lowest)
                requests.post("http://localhost:80/api/v1/create/slave")



# This function is called by the "scaler", if it has to reduce the number of slaves as the request count is low.
def crash_slave_scaler():
    global slave_container_detail
        
    m=-1
    for pid in slave_container_detail:
        if(m<pid):
            m=pid
    if(m>-1):
        resp=slave_delete_con(m)
        if(resp==200):
            return "Scaled-down by 1!"
    
    return "Couldn't Scale Down!"
        


# This function is called after every 2 minuters since the first request is received and scales it appropriately.
# The number of requests received (count of only the necessary requests) is stored in a file called "request_count.json"
def scaler():
    n=1
    with open("request_count.json","r") as file:
        j = json.load(file)

    total_requests = j["total_requests"]
        
    if(total_requests!=0):
        n = math.ceil(total_requests/20)
        
    j["total_requests"]=0
    
    with open("request_count.json","w") as file:
        json.dump(j,file)
    
    timer = threading.Timer(120.0, scaler)
    timer.start()

    run=1
    while(run):
        print()
        print("Orchestrator:(scaler()) master_container_detail:",master_container_detail)
        print("Orchestrator:(scaler()) slave_container_detail:",slave_container_detail)

        if(len(slave_container_detail)==n):
            run=0

        if(len(slave_container_detail)<n):
            x=requests.post("http://localhost:80/api/v1/create/slave")
            if(x):
                print("Slave Added")
        if(len(slave_container_detail)>n):
            x=crash_slave_scaler()
            if(x):
                print("Slave Killed")
                
        

# The write API makes an object of this class for every write. This establishes a connection with RabbitMQ Server, declares
# (if not already created) and connects to the "writeQ", "writeResponseQ".
# "writeQ" holds every write request as a message, while "writeResponseQ" holds the status code after attempting the write call.
class writeResponseObject(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))

        self.channel = self.connection.channel()

        req = self.channel.queue_declare(queue='writeQ',durable = True)
        self.request_queue = req.method.queue

        result = self.channel.queue_declare(queue='writeResponseQ', durable = True)
        self.callback_queue = result.method.queue

        
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
        

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=self.request_queue,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(n))
        while self.response is None:
            self.connection.process_data_events()

        self.connection.close()
        print(self.response)
        return self.response



# The read API makes an object of this class for every read. This establishes a connection with RabbitMQ Server, declares
# (if not already created) and connects to the "readQ", "responseQ".
# "readQ" holds every read request as a message, while "responseQ" holds the response of the read call.
class ResponseObject(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))

        self.channel = self.connection.channel()

        req = self.channel.queue_declare(queue='readQ',durable = True)
        self.read_queue = req.method.queue

        result = self.channel.queue_declare(queue='responseQ', durable = True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
        

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=self.read_queue,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(n))
        while self.response is None:
            self.connection.process_data_events()

        self.connection.close()
        print(self.response)
        return self.response





# Any write request made is serviced here. Incase the first_request is a write request, it automatically starts the timer
# and also creates one slave and one master(converted after being spawned as a slave first). [Initial Slaves are created only after the first request is received]
# It returns the status code of the write operation back.
@app.route("/api/v1/db/write",methods=["POST"])
def write_db():
    global first_request
    global slave_container_detail
    global master_container_detail

    if(first_request):
        timer = threading.Timer(120.0, scaler)
        timer.start()
        first_request = False
        with open("request_count.json","w") as file:
            count={}
            count["total_requests"]=0
            json.dump(count,file)

    # As there will be no workers running until the first request is received
    if(len(master_container_detail)==0 and len(slave_container_detail)==0):
        # This creates only one slave, but this triggers a zookeeper event which makes this the master and spawns another slave.
        # Hence we get an initial "master" and a "slave".
        print("Orchestrator:(write_db()) First Request Received, Spawning a Slave!")
        y = requests.post("http://localhost:80/api/v1/create/slave")
        if(y):
            print()
            print("Orchestrator:(write_db()) Created Initial Slave")

    message = request.get_json()
    print("Orchestrator:(write_db()) Write Request is:",message)
    writeRespObj = writeResponseObject()
    wresp = writeRespObj.call(message).decode()
    res = json.loads(wresp)
    with open("write_request.json","a") as file:
        json.dump(message,file)

    del(writeRespObj)
    return res["status"]


# Any read request made is serviced here. Incase the first_request is a read request, it automatically starts the timer
# and also creates one slave and one master(converted after being spawned as a slave first). [Initial Slaves are created only after the first request is received]
# It returns the appropriate response of the read operation back.
@app.route("/api/v1/db/read",methods=["POST"])
def read_db():
    global first_request
    global slave_container_detail
    global master_container_detail

    if(first_request):
        timer = threading.Timer(120.0, scaler)
        timer.start()
        first_request = False
        with open("request_count.json","w") as file:
            count={}
            count["total_requests"]=1
            json.dump(count,file)

        return Response(status=400)

    # As there will be no workers running until the first request is received
    if(len(master_container_detail)==0 and len(slave_container_detail)==0):
        # This creates only one slave, but this triggers a zookeeper event which makes this the master and spawns another slave.
        # Hence we get an initial "master" and a "slave".
        y = requests.post("http://localhost:80/api/v1/create/slave")
        if(y):
            print()
            print("Orchestrator:(read_db()) Created Initial Slave")
            
    if(request.get_json()["dual_request_flag"]==1):
        with open("request_count.json","r") as file:
            j = json.load(file)
            
        total_requests = j["total_requests"]
        j["total_requests"] = total_requests + 1
            
        with open("request_count.json","w") as file:
            json.dump(j,file)


    message = request.get_json()
    print("Read Call JSON Object:",message)
    respObj = ResponseObject()

    response = respObj.call(message).decode()
    with open("write_request.json","a") as file :
        json.dump(message,file)
    del(respObj)
    print()
    print(" Orchestrator:(read_db()) Got Response for READ:%r and it's type is:%r" % (response,type(response)))
    return response


# The API "/api/v1/crash/master" makes a call to this function to delete the "master". "ppid" is the PID of the existing "master" container.
def master_delete_con(ppid):
    v=master_container_detail[ppid]
    v.remove(force= True)
    print()
    print("Orchestrator:(master_delete_con()) Removed Master Container with "+str(ppid)+" Pid!")
    master_container_detail.pop(ppid)
    return 200

# The API "/api/v1/crash/slave" makes a call to this function to delete the "slave". "ppid" is the lowest PID "slave" container
def slave_delete_con(ppid):
    global crash_pid_flag
    v=slave_container_detail[ppid]
    
    v.remove(force= True)
    print()
    print("Orchestrator:(slave_delete_con()) Removed Slave Container with "+str(ppid)+" Pid!")
    slave_container_detail.pop(ppid)
    return 200


# This API is used to create a "slave" container. We have used Docker SDK to achieve this by going into the docker environment
# (where all the containers i.e Zookeeper, RabbitMQ, Orchestrator, Workers are present) from the orchestrator environment to create them.
@app.route("/api/v1/create/slave",methods=["POST"])
def create_con_slave():
    global slave_container_detail
    client = docker.from_env()
    client.images.build(path=".", tag="slave")    
    client.containers.create("slave",detach=True)

    cont_id = os.popen("hostname").read().strip()
    print()
    print("Orchestrator:(create_con_slave()) Orchestrator's Container ID:",cont_id)
    v=client.containers.run("slave",command="python worker.py",network='orch_default',links={'rmq':'rmq'},detach=True,volumes_from=[cont_id])
    print ("Orchestrator:(create_con_slave()) New Slave created!")
    ppid = int(v.top()['Processes'][0][2])
    slave_container_detail[ppid]=v
    print("Orchestrator:(create_con_slave()) PPID is ",ppid)
    return "Created"


'''
PLEASE NOTE THAT THERE IS NO API TO CREATE A "MASTER" AS THE SLAVES THEMSELVES ARE MADE INTO THE MASTER IF NEEDED BY THE ZOOKEEPER EVENT.
'''


# This API is used to crash the "master" container. This API makes a call to the function "master_delete_con" along with the master's
# PID to achieve this.
@app.route("/api/v1/crash/master",methods=["POST"])
def crash_master():
    global master_container_detail
    if(request.method=="POST"):
        for key in master_container_detail:
            resp=master_delete_con(key)
            if(resp==200):
                return jsonify([key])
        return Response(status=400)
    else:
        return Response(status=405)

# This API is used to crash the "slave" container. This API makes a call to the function "slave_delete_con" along with the lowest slave's
# PID to achieve this.
@app.route("/api/v1/crash/slave", methods=["POST"])
def crash_slave():
    global slave_container_detail
    global crash_pid_flag
    # global master_container_detail
    if(request.method=="POST"):
        
        m=-1
        for pid in slave_container_detail:
            if(m<pid):
                m=pid
        if(m>-1):
            crash_pid_flag=1
            resp=slave_delete_con(m)
            if(resp==200):
                return jsonify([m])
            return Response(status=400)
        else:
            return Respsonse(status=400)
    else:
        return Response(status=405)

# This API is used to list all the Worker's PID. These are stored in master_container_detail and slave_container_detail dictionaries.
# Incase the first_request is a read request, it automatically starts the timer
# and also creates one slave and one master(converted after being spawned as a slave first). [Initial Slaves are created only after the first request is received]
@app.route("/api/v1/worker/list",methods=["GET"])
def list_container_pid():
    global slave_container_detail
    global master_container_detail
    global first_request
    if(request.method=="GET"):
        if(first_request):
            timer = threading.Timer(120.0, scaler)
            timer.start()
            first_request = False
            with open("request_count.json","w") as file:
                count={}
                count["total_requests"]=0
                json.dump(count,file)

        if(len(master_container_detail)==0 and len(slave_container_detail)==0):
            print("Orchestrator:(write_db()) First Request Received, Spawning a Slave!")
            y = requests.post("http://localhost:80/api/v1/create/slave")
            if(y):
                print()
                print("Orchestrator:(write_db()) Created Initial Slave")
        l=[]
        f = master_container_detail.keys()
        s = slave_container_detail.keys()
        l=sorted(list(f)+list(s))
        return jsonify(l)
    else:
        return Response(status=405)

# This API is used to clear the SQLite Database in every Worker Container.
# Incase the first_request is a read request, it automatically starts the timer
# and also creates one slave and one master(converted after being spawned as a slave first). [Initial Slaves are created only after the first request is received]
@app.route("/api/v1/db/clear", methods=["POST"])
def clear_db():
    global first_request
    global slave_container_detail
    global master_container_detail
    if(request.method=='POST'):
        
        if(first_request):
            timer = threading.Timer(120.0, scaler)
            timer.start()
            first_request = False
            with open("request_count.json","w") as file:
                count={}
                count["total_requests"]=0
                json.dump(count,file)

        if(len(master_container_detail)==0 and len(slave_container_detail)==0):
            print("Orchestrator:(write_db()) First Request Received, Spawning a Slave!")
            y = requests.post("http://localhost:80/api/v1/create/slave")
            if(y):
                print()
                print("Orchestrator:(write_db()) Created Initial Slave")
        data={'join':4}
        req=requests.post("http://localhost:80/api/v1/db/write",json=data)
        print("Req is:",req, " and req.text is:",req.text)
        return Response(status=int(req.text))
    else:
        return Response(status=405)



######################################### STARTING THE FLASK APPLICATION ############################################

if __name__=="__main__":
    
    app.run(host="0.0.0.0",port=80,debug=False)

#####################################################################################################################

