#!/usr/bin/env python
from flask import Flask,render_template,jsonify,request,abort,Response
import time
import sqlite3
import requests
import re
import csv
import datetime
import json 
import pika
import sys
import uuid

############################### FLASK, DB SETUP ##################################

app = Flask(__name__)


# def CB(corr_id):
# def onresponse(ch, method, props, body):
# 		# if(corr_id==props.corr_id):
# 		print("Response is :" ,body)

# def rabbitMQreadcall(message):
# 	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
# 	channel = connection.channel()
# 	channel.queue_declare(queue='readQ', durable = True)
# 	channel.queue_declare(queue='responseQ',durable = True)
	
# 	# corr_id = str(uuid.uuid4())

# 	channel.basic_publish(
# 		exchange='',
# 		routing_key='readQ',
# 		properties=pika.BasicProperties(
# 				reply_to = "responseQ"
# 			),
# 		body=json.dumps(message))

# 	print(" [x] Sent %r"% message)

# 	# onresponse = CB(corr_id)
# 	channel.basic_get(
# 		queue='responseQ',
# 		on_message_callback=onresponse
# 		)
# 	print(" After consuming!")




class ResponseObject(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='responseQ', durable = True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body)

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='readQ',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(n))
        while self.response is None:
            self.connection.process_data_events()
        print(self.response)
        return self.response









def rabbitMQwritecall(message):
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
	channel = connection.channel()
	channel.queue_declare(queue='writeQ', durable = True)

	channel.basic_publish(
		exchange='',
		routing_key='writeQ',
		body=json.dumps(message))

	print(" [x] Sent %r" % message)
	


	


@app.route("/api/v1/db/write",methods=["POST"])
def write_db():
    #access book name sent as JSON object
    #in POST request body
    message = request.get_json()

    rabbitMQwritecall(message)
    return Response(status=200) 

@app.route("/api/v1/db/read",methods=["POST"])
def read_db():
	message = request.get_json()
	respObj = ResponseObject()

	print(" [x] Requesting fib(30)")
	response = respObj.call(message)
	print(" [.] Got %r" % response)
	#rabbitMQreadcall(message)
	return response
	
	

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

######################################### FLASK PART ############################################
if __name__=="__main__":
	app.run(host="0.0.0.0",port=80,debug=True)





################################################################################################

