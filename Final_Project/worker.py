#!/usr/bin/env python

from kazoo.client import KazooClient
from kazoo.client import KazooState
import pika
import sqlite3
import re
import csv
import time
import docker
import json
import sys
import os
import logging
import multiprocessing


first_event_req=True


logging.basicConfig()

zk = KazooClient(hosts='zoo:2181')
zk.start()

zk.ensure_path("/orchestrator")


cont_id = os.popen("hostname").read().strip()
print()
print("Worker: Slave Container ID:",cont_id)

client = docker.from_env()
container_obj = client.containers.get(str(cont_id))
ppid = int(container_obj.top()['Processes'][0][2])
wok = "worker,"+str(ppid)
zk.create("/orchestrator/"+wok, b"slave," + str(ppid).encode("utf-8"), ephemeral=True)



connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))
channel = connection.channel()



updationQ=""



path = "app_"+str(cont_id)+".db"







#################################### FUNCTIONS NEEDED BY MAIN FUNCTIONS ##############################################

def creation_sync(query):
    with sqlite3.connect(path) as con:
        cur = con.cursor()
        q = query.decode()
        print()
        print("Worker:(creation_sync()) Sync Query is:",q," and it's type is:",type(q))
        cur.execute(q)
        print("Worker:(creation_sync()) Sync Query '"+q+"' Successfully executed")

def updationQueryExecute(ch, method, props, body):
    with sqlite3.connect(path) as con:
        cur = con.cursor()
        q = body.decode()
        print()
        print("Worker:(updationQueryExecute()) Updation Query is:",q," and it's type is:",type(q))
        cur.execute(q)
        print("Worker:(updationQueryExecute()) Updation Query '"+q+"' Successfully executed")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def callbackread(ch, method, props, body):
    print()
    print("Worker:(callbackread()) READ CALLBACK CALLED!") 
    request = json.loads(body)
    print("JSON BODY IS:",request)
    ch.queue_declare(queue='responseQ',durable = True)
    table=request["table"]
    insert=request["insert"]
    where_flag=request["where_flag"]
    cols=""
    l=len(insert)
    s=""
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

            '''    
            Use this as the json data to send from the user's container
            {
                "table":"users",
                "insert":["username"],
                "where_flag":0
            }
            '''
            
            with sqlite3.connect(path) as con:
                #return "table is %s, username is %s, password is %s"%(table,u,p)
                cur = con.cursor()
                #print("hi")
                query="SELECT username from users"
        
                #print(query)
                cur.execute(query)
                #print("hello")
                con.commit()
                status=201
                for i in cur:
                    s = s + str(i[0]) + ","
                    # #print(type(i))
                #return jsonify({"string":s})
                s=s[:-1]
                '''
                BROOOO SURYAAAAA, it returns it this way:

                'Hari','Surya','JT','Rahul'
                '''

                print("Users List:",s)
        except:
                print(e)
               #return Response(status=400)
    else:   #rides
        try:
            with sqlite3.connect(path) as con:
                #return "table is %s, username is %s, password is %s"%(table,u,p)
                cur = con.cursor()
                #print("BEFORE EXEC")
                if(where_flag):
                    where=request["where"]
                    query="SELECT "+cols+" from rides WHERE "+where
                else:
                    query="SELECT "+cols+" from rides"
                #print(query)
                cur.execute(query)
                #print("AFTER EXEC")
                con.commit()
                status=201
                
                for i in cur:
                    s = s + str(i) + "\n"
                #return jsonify({"string":s})
                print("Either list_source_to_destination or list_details of rides:")
                print(s)
        except:
                print(e)
                #return Response(status=400)

    print(props)
    ch.basic_publish(exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id =props.correlation_id),
            body=s)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    print("[*] Sent Message from Slave: ",s)
    

def callbackwrite(ch, method, properties, body):
    print()
    print("Worker:(callbackwrite()) WRITE CALLBACK CALLED!")
    request = json.loads(body)
    print("JSONBODY IS:",request)
    join = request["join"]
    print("join is",join)
    if(join==0):
        table=request["table"]
        if(table=="users"):
            ##print(type(p))
            try:
                print("enter1")
                with sqlite3.connect(path) as con:
                    print("enter2")
                    username=request["username"]
                    password=request["password"]
                        #return "table is %s, username is %s, password is %s"%(table,u,p)
                    cur = con.cursor()
                    print("enter 2.5")
                    q="INSERT into users values ('"+username+"','"+password+"')"
                    print(q)
                    cur.execute(q)
                    print("enter 3")
                    con.commit()

                    ch.basic_publish(exchange='fan', routing_key='', body=q)
                    ch.basic_publish(exchange='', routing_key='syncQ', body=q)

                    s = {"status":201}

                    ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))
                    #return Response(status=201)
            except Exception as e:
                s = {"status":400}
                print(e)
                ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))
                
                #return Response(status=400)


        if(table=="rides"):
            try:
                print("In")
                created_by=request["created_by"]
                timestamp=request["timestamp"]
                source=request["source"]
                destination=request["destination"]

                ride_users=""


                with sqlite3.connect(path) as con:

                    cur = con.cursor()
                    #cur.execute("DELETE FROM rides WHERE created_by=\"'hk'\"")
                    ##print("\nBefore Insertion\n")
                    #query="INSERT INTO rides (rideId,created_by, ride_users, timestamp, source, destination) values (None," + "'" + created_by + "'" + "," +  "'" + ride_users + "'" + "," + "'" + timestamp + "'" + "," + "'" + source + "'" + "," + "'" + destination + "'" + ")"
                    ##print(query)
                    
                    #print(created_by,timestamp,source,destination)
                    n=cur.execute("SELECT max(rideId) FROM rides").fetchone()[0]
                    if(n==None):
                        #print("Inside")
                        m=0
                    else:
                        m = n
                    #m=cur.fetchone()[0]

                    print(m)
                    cur.execute("INSERT into rides (rideId,created_by,ride_users,timestamp,source,destination) values (?,?,?,?,?,?)",(m+1,created_by,ride_users,timestamp,source,destination))
                    # print("Surya")
                    q="INSERT into rides (rideId,created_by,ride_users,timestamp,source,destination) values ("+str(m+1)+",'"+created_by+"','"+ride_users+"','"+timestamp+"','"+source+"','"+destination+"')"      
                    
                    ch.basic_publish(exchange='fan', routing_key='', body=q)
                    ch.basic_publish(exchange='', routing_key='syncQ', body=q)

                    s = {"status":201}

                    ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))

                    con.commit()
                    status=201
                    #return Response(status=201)
            except Exception as e:
                s = {"status":400}

                ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))
                print(e)
                #return Response(status=400)

    if(join==1):
        try:
            with sqlite3.connect(path) as con:
                rideId = request["rideId"]
                username = request["username"]

                print(username)
                u=""
                #check_rides_q = "SELECT COUNT(*) FROM rides WHERE rideId"+str(rideId)

                cur = con.cursor()

                '''
                query="UPDATE rides SET ride_users="+"'hari'"+" WHERE rideId=1"
                cur.execute(query)
                '''
                cur.execute("SELECT count(*) FROM rides WHERE rideId="+str(rideId))
                ride_flag=cur.fetchone()[0]
                #cur.execute("SELECT count(*) FROM users WHERE username="+"'"+str(username)+"'")
                user_flag=1
                con.commit()

                print(ride_flag,user_flag)
                if(ride_flag and user_flag):
                    cur.execute("SELECT ride_users FROM rides WHERE rideId="+str(rideId))
                    con.commit()
                    r_u=cur.fetchone()[0].split(",")
                    print(r_u,username)
                    if username not in r_u:
                        for i in r_u:
                            if(i!=""):
                                u = u + i + ","
                        u += username
                        print("total users", u)
                        query="UPDATE rides SET ride_users="+"'"+str(u)+"'"+" WHERE rideId="+str(rideId)
                        cur.execute(query)
                        con.commit()

                        ch.basic_publish(exchange='fan', routing_key='', body=query)
                        ch.basic_publish(exchange='', routing_key='syncQ', body=query)

                        s = {"status":200}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        #return Response(status=200)
                        print("Joined Ride!")

                    else:
                        s = {"status":400}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        #return Response(status=400)
                        print("Duplicate User!")
                else:
                    s = {"status":400}

                    ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))
                    print("Ride doesn't exist!")
                    #return Response(status=400)
        except Exception as e:
            s = {"status":405}

            ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                    body=json.dumps(s))
            print(e)
            #return Response(status=405)
        


    if(join==2):
        try:
                with sqlite3.connect(path) as con:
                    rideId = request["rideId"]
                    cur = con.cursor()
                    cur.execute("SELECT count(*) FROM rides WHERE rideId="+str(rideId))
                    ride_flag=cur.fetchone()[0]
                    con.commit()

                    if(ride_flag):
                        q = "DELETE FROM rides WHERE rideId="+str(rideId)
                        cur.execute(q)
                        con.commit()
                        ch.basic_publish(exchange='fan', routing_key='', body=q)
                        ch.basic_publish(exchange='', routing_key='syncQ', body=q)

                        s = {"status":200}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))

                        print("Ride Deleted Successfully")
                        #return Response(status=200)
                    else:
                        s = {"status":400}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        print("Ride doesn't exist!")
                        #return Response(status=400)
        except Exception as e:
            s = {"status":500}

            ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                    body=json.dumps(s))
            print(e)
            #return Response(status=405)


    if(join==3):
        try:
                with sqlite3.connect(path) as con:
                    #print("Connected")
                    username = request["username"]
                    cur = con.cursor()
                    cur.execute("SELECT count(*) FROM users where username="+"'"+str(username)+"'")
                    user_flag = cur.fetchone()[0]
                    con.commit()
                    if(user_flag):
                        q = "DELETE FROM users WHERE username="+"'"+str(username)+"'"
                        cur.execute(q)
                        #print("Executed")
                        con.commit()

                        ch.basic_publish(exchange='fan', routing_key='', body=q)
                        ch.basic_publish(exchange='', routing_key='syncQ', body=q)

                        s = {"status":200}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        print("User Deleted Successfully!")
                        #return Response(status=200)
                    else:
                        s = {"status":400}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        print("User doesn't exist!")
                        #return Response(status=400)
        except Exception as e:
            s = {"status":400}

            ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                    body=json.dumps(s))
            print(e)
            #eturn Response(status=400)
    ch.basic_ack(delivery_tag = method.delivery_tag)


######################################################################################################################




##################################################### MAIN FUNCTIONS #################################################

def run_as_slave():
    global updationQ
    global connection
    global channel
    
    con = sqlite3.connect(path)

    cur=con.cursor()
    con.execute("PRAGMA foreign_keys = ON")
    cur.execute("CREATE TABLE IF NOT EXISTS users(username TEXT primary key NOT NULL, password TEXT NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS rides(rideId INTEGER PRIMARY KEY, created_by TEXT,ride_users TEXT, timestamp TEXT, source TEXT, destination TEXT,FOREIGN KEY (created_by) REFERENCES users (username) ON DELETE CASCADE)")
    con.commit()

    print()
    print("Worker:(run_as_slave()) Running as Slave!")
    syncChannel = connection.channel()

    ret = syncChannel.queue_declare(queue='syncQ', durable=True)
    while(ret.method.message_count!=0):
        res = syncChannel.basic_get(queue='syncQ',auto_ack=False)
        creation_sync(res[2])
        ret = syncChannel.queue_declare(queue='syncQ',durable=True)

    print("Worker:(run_as_slave()) Sync Successful!")
    syncChannel.close()

    print("Worker:(run_as_slave) syncChannel Closed!")

    updationQ = "updationQ_"+str(cont_id)
    channel.queue_declare(queue='readQ', durable=True)
    channel.queue_declare(queue=updationQ, durable=True) #auto_delete=True)
    print('Worker:(run_as_slave()) UpdationQ created as:',updationQ)
    channel.exchange_declare(exchange='fan',exchange_type='fanout')
    channel.queue_bind(exchange='fan',queue=updationQ)
    print('Worker:(run_as_slave()) Fan Exchange Created!')
    try:
        channel.basic_consume(queue=updationQ, on_message_callback=updationQueryExecute)
    except:
        print("---------------------------------------------")
        print("run_as_slave [updationQ] EXCEPTION!")
        print("---------------------------------------------")
    # channel.queue_declare(queue='syncQ', durable=True)
    
    print('Worker:(run_as_slave()) Reading Messages Now!')
    
    try:
        channel.basic_consume(queue='readQ', on_message_callback=callbackread)
    except:
        print("---------------------------------------------")
        print("run_as_slave [readQ] EXCEPTION!")
        print("---------------------------------------------")


    try:
        channel.start_consuming()
    except:
        print("---------------------------------------------")
        print("run_as_slave [start_consuming] EXCEPTION!")
        print("---------------------------------------------")




#Delete all existing connections of slave, convert slave to a master.
def run_as_master():
    global updationQ
    global connection
    global channel
    
    try:
        print()
        print("Worker:(run_as_master()) Running as Master!")
        channel.queue_unbind(queue=updationQ,exchange='fan')
        channel.queue_delete(queue=updationQ,if_unused=False,if_empty=False)

        connection.close()

    except:
        print("---------------------------------------------")
        print("run_as_master EXCEPTION!")
        print("---------------------------------------------")

    master_connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rmq'))
    master_channel = master_connection.channel()



    master_channel.queue_declare(queue='writeQ', durable=True)
    master_channel.queue_declare(queue='writeResponseQ', durable=True)
    master_channel.exchange_declare(exchange='fan',exchange_type='fanout')
    master_channel.queue_declare(queue='syncQ',durable=True)
    # channel.queue_declare(queue='syncQ', durable=True)
    print(' Worker:(run_as_master()) Waiting for messages.')
    master_channel.basic_consume(queue='writeQ', on_message_callback=callbackwrite)
    master_channel.start_consuming()
    print("---------------------------------------------")
    print("Inside Master Function!")
    print("---------------------------------------------")













################################################# ZOOKEEPER EVENT ###################################################

@zk.DataWatch("/orchestrator/"+wok)
def data_change(data,stat,event):
    global first_event_req
    global process 
    global dont_run_slave

    print("Worker:(data_change()) Event Triggered!")
    if(first_event_req):
        first_event_req = False
    
    else:
        print()
        print("Worker:(data_change()) Switching "+wok+" to Master")
        print("Worker:(data_change()) The Data is:",data)
        print("Worker:(data_change()) The Stat is:",stat)
        print("Worker:(data_change()) The Event is:",event)
        # process.terminate()
        run_as_master()
        return False



######################################################################################################################

if __name__ == '__main__':

    # process = multiprocessing.Process(target=run_as_slave, args=())
    # process.start()
    print("MAIN IS USELESS ______")
    run_as_slave()
    
    