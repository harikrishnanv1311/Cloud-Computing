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

# Here we establish a connection with the Zookeeper Server.
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

# We create a znode which is a part of "/orchestrator/" with its name being "worker,<PID of the container>"
# We also set the znode-data as "slave" because it always runs as slave intially.
zk.create("/orchestrator/"+wok, b"slave," + str(ppid).encode("utf-8"), ephemeral=True)



connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))
channel = connection.channel()



updationQ=""


# Every Worker has a unique database to itself whose name is "app_<Container ID>.db"
path = "app_"+str(cont_id)+".db"



# This function gets every query executed by "master" and also executes it in slave BEFORE its
# creation to maintain consistency
def creation_sync(query):
    with sqlite3.connect(path) as con:
        cur = con.cursor()
        q = query.decode()
        print()
        print("Worker:(creation_sync()) Sync Query is:",q," and it's type is:",type(q))
        cur.execute(q)
        print("Worker:(creation_sync()) Sync Query '"+q+"' Successfully executed")

# This function gets every query executed by "master" and also executes it in slave AFTER its
# creation to maintain consistency
def updationQueryExecute(ch, method, props, body):
    with sqlite3.connect(path) as con:
        cur = con.cursor()
        q = body.decode()
        print()
        print("Worker:(updationQueryExecute()) Updation Query is:",q," and it's type is:",type(q))
        cur.execute(q)
        print("Worker:(updationQueryExecute()) Updation Query '"+q+"' Successfully executed")
        ch.basic_ack(delivery_tag=method.delivery_tag)




################################################### CALLBACK FUNCTIONS ###############################################

# This function is called when run_as_slave() receives a message from "readQ".
# This function executes the query and publishes the appropriate retrieved message to the "responseQ" 
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

    if(table=="users"):
        try:
            
            with sqlite3.connect(path) as con:
                cur = con.cursor()
                query="SELECT username from users"
        
                cur.execute(query)
                con.commit()
                status=201
                for i in cur:
                    s = s + str(i[0]) + ","
                s=s[:-1
                print("Users List:",s)

        except:
                print(e)
    else:
        try:
            with sqlite3.connect(path) as con:
                cur = con.cursor()
                if(where_flag):
                    where=request["where"]
                    query="SELECT "+cols+" from rides WHERE "+where
                else:
                    query="SELECT "+cols+" from rides"
                cur.execute(query)
                con.commit()
                status=201
                
                for i in cur:
                    s = s + str(i) + "\n"
                print("Either list_source_to_destination or list_details of rides:")
                print(s)
                
        except:
                print(e)

    print(props)

    # This is where we publish the response
    ch.basic_publish(exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id =props.correlation_id),
            body=s)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    print("[*] Sent Message from Slave: ",s)
    

# This function is called when run_as_master() receives a message from "writeQ"
# This function executes the query and publishes the appropriate status code to the "writeResponseQ".
# It also writes the write message to the "fan(exchange)" and also the "syncQ"
def callbackwrite(ch, method, properties, body):
    print()
    print("Worker:(callbackwrite()) WRITE CALLBACK CALLED!")
    request = json.loads(body)
    print("JSONBODY IS:",request)
    join = request["join"]
    print("join is",join)

    # join = 0 is for creation of rides or users.
    # join = 1 is for joining a particular ride.
    # join = 2 is for deleting a ride.
    # join = 3 is for deleting a user.
    # join = 4 is for deleting the database.
    if(join==0):
        table=request["table"]
        if(table=="users"):
            try:
                print("enter1")
                with sqlite3.connect(path) as con:
                    print("enter2")
                    username=request["username"]
                    password=request["password"]
                    cur = con.cursor()
                    print("enter 2.5")
                    q="INSERT into users values ('"+username+"','"+password+"')"
                    print(q)
                    cur.execute(q)
                    print("enter 3")
                    con.commit()

                    ch.basic_publish(exchange='fan', routing_key='', body=q)
                    ch.basic_publish(exchange='', routing_key='syncQ', body=q)

                    s = {"status":"201"}

                    ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))
            except Exception as e:
                s = {"status":"400"}
                print(e)
                ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))
                


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
                    n=cur.execute("SELECT max(rideId) FROM rides").fetchone()[0]
                    if(n==None):
                        m=0
                    else:
                        m = n

                    print(m)
                    cur.execute("INSERT into rides (rideId,created_by,ride_users,timestamp,source,destination) values (?,?,?,?,?,?)",(m+1,created_by,ride_users,timestamp,source,destination))
                    q="INSERT into rides (rideId,created_by,ride_users,timestamp,source,destination) values ("+str(m+1)+",'"+created_by+"','"+ride_users+"','"+timestamp+"','"+source+"','"+destination+"')"      
                    
                    ch.basic_publish(exchange='fan', routing_key='', body=q)
                    ch.basic_publish(exchange='', routing_key='syncQ', body=q)

                    s = {"status":"201"}

                    ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))

                    con.commit()
                    status=201
            except Exception as e:
                s = {"status":"400"}

                ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))
                print(e)

    if(join==1):
        try:
            with sqlite3.connect(path) as con:
                rideId = request["rideId"]
                username = request["username"]

                print(username)
                u=""

                cur = con.cursor()

                cur.execute("SELECT count(*) FROM rides WHERE rideId="+str(rideId))
                ride_flag=cur.fetchone()[0]
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

                        s = {"status":"200"}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        print("Joined Ride!")

                    else:
                        s = {"status":"400"}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        print("Duplicate User!")
                else:
                    s = {"status":"400"}

                    ch.basic_publish(exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                        body=json.dumps(s))
                    print("Ride doesn't exist!")
        except Exception as e:
            s = {"status":"405"}

            ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                    body=json.dumps(s))
            print(e)
        


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

                        s = {"status":"200"}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))

                        print("Ride Deleted Successfully")
                    else:
                        s = {"status":"400"}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        print("Ride doesn't exist!")
        except Exception as e:
            s = {"status":"500"}

            ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                    body=json.dumps(s))
            print(e)


    if(join==3):
        try:
                with sqlite3.connect(path) as con:
                    username = request["username"]
                    cur = con.cursor()
                    cur.execute("SELECT count(*) FROM users where username="+"'"+str(username)+"'")
                    user_flag = cur.fetchone()[0]
                    con.commit()
                    if(user_flag):
                        q = "DELETE FROM users WHERE username="+"'"+str(username)+"'"
                        cur.execute(q)
                        con.commit()

                        ch.basic_publish(exchange='fan', routing_key='', body=q)
                        ch.basic_publish(exchange='', routing_key='syncQ', body=q)

                        s = {"status":"200"}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        print("User Deleted Successfully!")
                    else:
                        s = {"status":"400"}

                        ch.basic_publish(exchange='',
                            routing_key=properties.reply_to,
                            properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                            body=json.dumps(s))
                        print("User doesn't exist!")
        except Exception as e:
            s = {"status":"400"}
            print(e)
            ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                    body=json.dumps(s))
            
    if(join==4):
        try:
            with sqlite3.connect(path) as con:
                cur=con.cursor()
                cur.execute('DELETE FROM users')
                cur.execute('DELETE FROM rides')
                con.commit()
                print("Database Deleted Successfully!")
                q1='DELETE FROM users'
                q2='DELETE FROM rides'
                ch.basic_publish(exchange='fan', routing_key='', body=q1)
                ch.basic_publish(exchange='fan', routing_key='', body=q2)
                ch.basic_publish(exchange='', routing_key='syncQ', body=q1)
                ch.basic_publish(exchange='', routing_key='syncQ', body=q2)
                s = {"status":"200"}
                ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                    body=json.dumps(s))


        except Exception as e:
            s={"status":"500"}
            print("Couldn't Delete DB!")
            print(e)
            ch.basic_publish(exchange='',
                    routing_key=properties.reply_to,
                    properties=pika.BasicProperties(correlation_id =properties.correlation_id),
                    body=json.dumps(s))

    ch.basic_ack(delivery_tag = method.delivery_tag)

######################################################################################################################


##################################################### MAIN FUNCTIONS #################################################


# This API is called intially called once the Worker container is made and makes it act like a slave. It establishes
# connections with the "syncQ", "updationQ","fan(exchange)","readQ","responseQ". It then starts consuming read requests
# from the "readQ" and publishes the appropriate response onto the "responseQ" 
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
    
    # Here we establish a connection to the syncQ which stores all the write requests made so far, and 
    # we call the creation_sync() function which synchronises the database.
    # Whenever a slave is created, it initially connects to the “syncQ” to execute all the requests that 
    # were received to the “writeQ” (master) without sending back an acknowledge which makes the state of 
    # every request in the “syncQ” as “Not Acknowledged”. We then close the connection between the “syncQ” 
    # and the new slave which then changes the state of all the requests back to “ready” state for consumption again. 
    try:
        syncChannel = connection.channel()
        ret = syncChannel.queue_declare(queue='syncQ', durable=True)
        while(ret.method.message_count!=0):
            res = syncChannel.basic_get(queue='syncQ',auto_ack=False)
            creation_sync(res[2])
            ret = syncChannel.queue_declare(queue='syncQ',durable=True)

        print("Worker:(run_as_slave()) Sync Successful!")
        syncChannel.close()

        print("Worker:(run_as_slave) syncChannel Closed!")
    except:
        print("---------------------------------------------")
        print("run_as_slave [syncing] EXCEPTION!")
        print("---------------------------------------------")

    # Here we establish a connection with the updationQ which is connected to a "fan" exchange.
    # This is used to immediately get the write message that was used by the master to maintain
    # consistency. Each Slave has its own updationQ.
    try:
        updationQ = "updationQ_"+str(cont_id)
        channel.queue_declare(queue='readQ', durable=True)
        channel.queue_declare(queue=updationQ, durable=True)
        print('Worker:(run_as_slave()) UpdationQ created as:',updationQ)
        channel.exchange_declare(exchange='fan',exchange_type='fanout')
        channel.queue_bind(exchange='fan',queue=updationQ)
        print('Worker:(run_as_slave()) Fan Exchange Created!')
    except:
        print("---------------------------------------------")
        print("run_as_slave [updationQ creation] EXCEPTION!")
        print("---------------------------------------------")
    
    # Start consuming from the updationQ
    try:
        channel.basic_consume(queue=updationQ, on_message_callback=updationQueryExecute)
    except:
        print("---------------------------------------------")
        print("run_as_slave [updationQ] EXCEPTION!")
        print("---------------------------------------------")
    
    print('Worker:(run_as_slave()) Reading Messages Now!')
    
    try:
        channel.basic_consume(queue='readQ', on_message_callback=callbackread)
    except:
        print("---------------------------------------------")
        print("run_as_slave [readQ] EXCEPTION!")
        print("---------------------------------------------")

    # Start consuming from the updationQ
    try:
        channel.start_consuming()
    except:
        print("---------------------------------------------")
        print("run_as_slave [start_consuming] EXCEPTION!")
        print("---------------------------------------------")




# When this is called, it means that a "slave" is to be converted into a "master". Hence it deletes all the 
# existing connections it established as a slave (like with the "readQ","responseQ","fan(exchange)","synQ","updationQ") 
# and converts the slave to a master. It then initialises or re-initialises its connection to a "writeQ",
# "writeResponseQ","fan(exchange)" and "syncQ". It now starts to work as a "master"
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
    print(' Worker:(run_as_master()) Waiting for messages.')
    
    # Start consuming from the writeQ
    master_channel.basic_consume(queue='writeQ', on_message_callback=callbackwrite)
    master_channel.start_consuming()
    print("---------------------------------------------")
    print("Inside Master Function!")
    print("---------------------------------------------")













################################################# ZOOKEEPER EVENT ###################################################

# This zookeeper event watches for a data change in "znode-data". Initially, every znode-data invoked inside a worker 
# is set to "slave", which might be modified in the "Orchestrator" in case of "master" election. When this happens
# this watch detects the change, calls the run_as_master() function which converts the "slave" to a "master". 
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
        run_as_master()
        return False



##################################################   MAIN   ##########################################################

if __name__ == '__main__':
    # Initially, every worker is made to run as a Slave, which is later converted to a master, if needed.
    run_as_slave()
    
######################################################################################################################    