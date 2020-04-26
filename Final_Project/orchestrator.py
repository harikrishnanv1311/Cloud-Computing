#!/usr/bin/env python
from flask import Flask,render_template,jsonify,request,abort,Response
import threading
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

############################### FLASK, DB SETUP ##################################



# def custom_call():
#     #Your code
#     x=requests.get("http://localhost:80/api/v1/create/master")
#     y=requests.get("http://localhost:80/api/v1/create/slave")
#     if(x):
#         print("Created Initial Master")
#     if(y):
#         print("Created Initial Slave")

# class CustomServer(Server):
#     def __call__(self, app, *args, **kwargs):
#         custom_call()
#         return Server.__call__(self, app, *args, **kwargs)



app = Flask(__name__)


# manager = Manager(app)
# manager.add_command('runserver', CustomServer())

master_container_detail={}
slave_container_detail={}

first_request = True

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
    
    run=1
    while(run):
        print("Length of slave_container_detail:",len(slave_container_detail))
        if(len(slave_container_detail)==n):
            run=0

        if(len(slave_container_detail)<n):
            x=requests.post('http://localhost:80/api/v1/create/slave')
            if(x):
                print("Slave Added")
        if(len(slave_container_detail)>n):
            x=requests.post('http://localhost:80/api/v1/crash/slave')
            if(x):
                print("Slave Killed")
        

    timer = threading.Timer(120.0, scaler)
    timer.start()


                


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





def master_delete_con(ppid):
    v=master_container_detail[ppid]
    v.remove(force= True)
    print("Removed Master Container with "+str(ppid)+" Pid!")
    master_container_detail.pop(ppid)
    return 200

def slave_delete_con(ppid):
    v=slave_container_detail[ppid]
    v.remove(force= True)
    print("Removed Slave Container with "+str(ppid)+" Pid!")
    slave_container_detail.pop(ppid)
    return 200


@app.route("/api/v1/db/write",methods=["POST"])
def write_db():
    #access book name sent as JSON object
    #in POST request body
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

    if(len(master_container_detail)==0 and len(slave_container_detail)==0):
        x = requests.post("http://localhost:80/api/v1/create/master")
        y = requests.post("http://localhost:80/api/v1/create/slave")
        if(x):
            print("Created Initial Master")
        if(y):
            print("Created Initial Slave")

    message = request.get_json()
    writeRespObj = writeResponseObject()
    wresp = writeRespObj.call(message).decode()
    res = json.loads(wresp)

    del(writeRespObj)
    #rabbitMQwritecall(message)
    return Response(status=res["status"])



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
            count["total_requests"]=0
            json.dump(count,file)

        return Response(status=400)

    if(len(master_container_detail)==0 and len(slave_container_detail)==0):
        x = requests.post("http://localhost:80/api/v1/create/master")
        y = requests.post("http://localhost:80/api/v1/create/slave")
        if(x):
            print("Created Initial Master")
        if(y):
            print("Created Initial Slave")

    with open("request_count.json","r") as file:
        j = json.load(file)
        
    total_requests = j["total_requests"]
    j["total_requests"] = total_requests + 1
        
    with open("request_count.json","w") as file:
        json.dump(j,file)


    message = request.get_json()
    respObj = ResponseObject()

    response = respObj.call(message).decode()
    del(respObj)
    print(" [.] Got Response for READ:%r" % response)
	#rabbitMQreadcall(message)
    return response


@app.route("/api/v1/create/master",methods=["POST"])
def create_con_master():
    #global slave_container_detail
    global master_container_detail
    client = docker.from_env()
    client.images.build(path=".", tag="master")    
    client.containers.create("master",detach=True)
    v=client.containers.run("master",command="python worker.py 1",network='zookeeper_amqp_default',links={'rmq':'rmq'},detach=True)
    print ("New Master created!")
    #print("TOP ELEMENTS:",docker.top)
    #print("TOP ELEMENTS:",v.top())
    ppid = int(v.top()['Processes'][0][2])
    
    master_container_detail[ppid]=v
    return "Created"

	
@app.route("/api/v1/create/slave",methods=["POST"])
def create_con_slave():
    global slave_container_detail
    #global master_container_detail
    client = docker.from_env()
    client.images.build(path=".", tag="slave")    
    client.containers.create("slave",detach=True)
    v=client.containers.run("slave",command="python worker.py 0",network='zookeeper_amqp_default',links={'rmq':'rmq'},detach=True)
    print ("New Slave created!")
    #print("TOP ELEMENTS:",docker.top)
    #print("TOP ELEMENTS:",v.top())
    ppid = int(v.top()['Processes'][0][2])
    slave_container_detail[ppid]=v
    print("PPID is ",ppid)
    return "Created"



@app.route("/api/v1/crash/master",methods=["POST"])
def crash_master():
    # global slave_container_detail
    global master_container_detail
    if(request.method=="POST"):
        for key in master_container_detail:
            resp=master_delete_con(key)
            if(resp==200):
                return jsonify([key])
        return Response(status=400)
    else:
        return Response(status=405)


@app.route("/api/v1/crash/slave", methods=["POST"])
def crash_slave():
    global slave_container_detail
    # global master_container_detail
    if(request.method=="POST"):
        
        m=-1
        for pid in slave_container_detail:
            if(m<pid):
                m=pid
        if(m>-1):
            resp=slave_delete_con(m)
            if(resp==200):
                return jsonify([m])
            return Response(status=400)
        else:
            return Respsonse(status=400)
    else:
        return Response(status=405)


@app.route("/api/v1/list",methods=["GET"])
def list_container_pid():
    global slave_container_detail
    global master_container_detail
    if(request.method=="GET"):
        l=[]
        f = master_container_detail.keys()
        s = slave_container_detail.keys()
        l=sorted(f+s)
        return jsonify(l)
    else:
        return Response(status=405)



######################################### FLASK PART ############################################
if __name__=="__main__":
    

    #create_con_master()
    app.run(host="0.0.0.0",port=80,debug=False)

    

    
	



################################################################################################




'''
@app.route("/api/v1/db/read",methods=["POST"])
def read_db():
    #access book name sent as JSON object
    #in POST request body
    table=request.get_json()["table"]
    insert=request.get_json()["insert"]
    where_flag=request.get_json()["where_flag"]
    cols=""
    l=len(insert)
    for i in insert:
        l-=1
        cols+=i
        if(l!=0):
            cols+=","
    #print(cols)
    #print(type(cols))
    if(table=="users"):
        ##print(type(p))
        try:
            with sqlite3.connect(path) as con:
                #return "table is %s, username is %s, password is %s"%(table,u,p)
                cur = con.cursor()
                #print("hi")
                if(where_flag):
                    where=request.get_json()["where"]
                    query="SELECT "+cols+" from users WHERE "+where
                else:
                    query="SELECT "+cols+" from users"
                #print(query)
                cur.execute(query)
                #print("hello")
                con.commit()
                status=201
                s=""
                for i in cur:
                    s = s + str(i) + "\n"
                    # #print(type(i))
                return jsonify({"string":s})
        except:
       
               return Response(status=400)
    else:   #rides
        try:
            with sqlite3.connect(path) as con:
                #return "table is %s, username is %s, password is %s"%(table,u,p)
                cur = con.cursor()
                #print("BEFORE EXEC")
                if(where_flag):
                    where=request.get_json()["where"]
                    query="SELECT "+cols+" from rides WHERE "+where
                else:
                    query="SELECT "+cols+" from rides"
                #print(query)
                cur.execute(query)
                #print("AFTER EXEC")
                con.commit()
                status=201
                s=""
                for i in cur:
                    s = s + str(i) + "\n"
                return jsonify({"string":s})
        except:
                return Response(status=400)
    return s
'''



    # time.sleep(30)
    #print(v.labels)
    # v.remove(force=True)
   # print(client.info())

    #print("After trying to delete:",v.top())
    # v = client.containers.run("slave"+str(i),rm=True)
   
    #client.images.build(path=".", tag="slave12")    #print(client.containers.run("zookeeper_amqp_master_1"))
    # for container in client.containers.list():
    #     print(container.name)
    # client.containers.run("zookeeper_amqp_slave",["python3","worker.py","0"]
    #for i in range(2):
    #client.containers.create("slave12",detach=False)
    #client.containers.run("slave121",command="python worker.py 0",network='zookeeper_amqp_default',links={'rmq':'rmq'},detach=True)



# def CB(corr_id):
# def onresponse(ch, method, props, body):
#       # if(corr_id==props.corr_id):
#       print("Response is :" ,body)

# def rabbitMQreadcall(message):
#   connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
#   channel = connection.channel()
#   channel.queue_declare(queue='readQ', durable = True)
#   channel.queue_declare(queue='responseQ',durable = True)
    
#   # corr_id = str(uuid.uuid4())

#   channel.basic_publish(
#       exchange='',
#       routing_key='readQ',
#       properties=pika.BasicProperties(
#               reply_to = "responseQ"
#           ),
#       body=json.dumps(message))

#   print(" [x] Sent %r"% message)

#   # onresponse = CB(corr_id)
#   channel.basic_get(
#       queue='responseQ',
#       on_message_callback=onresponse
#       )
#   print(" After consuming!")

# def rabbitMQwritecall(message):
#   connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
#   channel = connection.channel()
#   channel.queue_declare(queue='writeQ', durable = True)

#   channel.basic_publish(
#       exchange='',
#       routing_key='writeQ',
#       body=json.dumps(message))

#   print(" [x] Sent %r" % message)