from flask import Flask,render_template,jsonify,request,abort,Response
from flask_cors import CORS
import time
import sqlite3
import requests
import re
import csv
import datetime
import json

#IP addresses of instances
areapath = "AreaNameEnum.csv"
ipaddr = "http://35.169.72.238"
ipaddr_user = "http://34.236.8.161"
ipaddr_ride = "http://3.208.45.172"



app = Flask(__name__)
CORS(app) #to enable cross domain requests

#########################################################################################
#Certain flags are used throughout the code to identify the requests to a particular category
#join=0 ==> create a new user
#join=1 ==> a user wants to join a ride
#join=2 ==> delete a ride

#where=0 ==> The sql query to be executed on the database doesn't have a 'where' clause
#where=1 ==> The sql query to be executed on the database has a 'where' clause

#dual_request_flag is used to avoid counting internal requests sent to the database and only
#to count the requests sent by the user. This avoids considering internal requests as actual
#requests and avoid unnecessary scaling
#dual_request_flag=1 ==> read should be counted as a request sent by the client
#dual_request_flag=0 ==> read shouldnt be counted as a request sent by the client(internal request)
#########################################################################################


#Function checks if the data received as input is a time in the future
def check_date(date):
    rem_quotes=date.replace("'","")
    rep_date=rem_quotes.replace(":","-").split("-")
    rep_date1=[int(i) for i in rep_date]
    fin_date=datetime.datetime(rep_date1[2],rep_date1[1],rep_date1[0],rep_date1[5],rep_date1[4],rep_date1[3])
    cur_date=datetime.datetime.now()
    if(fin_date>cur_date):
        return 1
    else:
        return 0



# API to join an existing ride
@app.route("/api/v1/rides/<rideId>",methods=["POST"])
def join_ride(rideId):

    username=request.get_json()["username"]
    fetch_data = {"table":"users","insert":["username"],"where_flag":0,"dual_request_flag":0}
    #request sent to check if user exists
    x=requests.post(ipaddr+"/api/v1/db/read",json=fetch_data)
    if x.status_code==204:return Response(status=400)
    l1 = x.text.split(",")
    print(l1)
    if(username not in l1):
        return Response(status=400)
    data = {"table":"rides","rideId":rideId,"username":username,"join":1}
    #request sent to join the ride with a particular rideId
    req = requests.post(ipaddr+"/api/v1/db/write",json=data)
    print("user joined ")
    return Response(status=int(req.text))

#API to create a ride
@app.route("/api/v1/rides",methods=["POST"])
def create_ride():
    
    if(request.method=='POST'):
        dicts={}
        try:
            with open(areapath,'r') as csvfile:
                c = csv.reader(csvfile)
                for row in c:
                    #dictionary containing the mapping between area_name and area_code
                    dicts[row[0]]=row[1] 
            
            
            created_by=request.get_json()["created_by"]
            fetch_data = {"table":"users","insert":["username"],"where_flag":0,"dual_request_flag":0}

            x=requests.post(ipaddr+"/api/v1/db/read",json= fetch_data)
            if x.status_code==204:return Response(status=400)
            l1= x.text.split(",")
            print(l1)
            if(created_by not in l1):
                return Response(status=400)
            timestamp=request.get_json()["timestamp"]
            source=request.get_json()["source"]
            destination=request.get_json()["destination"]
            #checking the format of timestamp
            if(time.strptime(timestamp, "%d-%m-%Y:%S-%M-%H")):
                data = {"join":0,"table":"rides","created_by":created_by,"timestamp":timestamp,"source":dicts[source],"destination":dicts[destination]}
                #request sent to create a new ride
                req = requests.post(ipaddr+"/api/v1/db/write",json=data)
                return Response(status=int(req.text))

        except Exception as e:
                return Response(status=400)
    else:
        return Response(status=405)    #wrong method used


#API to list all the details of a particular ride
@app.route("/api/v1/rides/<rideId>",methods=["GET"])
def list_details(rideId):
    
    if(request.method=="GET"):
        #checking if ride with rideId exists
        check_ride={"table":"rides","insert":["rideId"],"where_flag":1,"where":"rideId="+rideId,"dual_request_flag":1} 
        req_ride=requests.post(ipaddr+"/api/v1/db/read",json=check_ride)
        response_from_api=req_ride.text
        resp1=response_from_api.split("\n")
        dicts={}
        print("Resp 1 is:",resp1)
        csvfile=open(areapath,'r')
        c = csv.reader(csvfile)
    

        for row in c:
            dicts[row[0]]=row[1]
        if(len(resp1)>1):
            #request sent to fetch ride details
            data={"table":"rides","insert":["rideId","created_by","ride_users","timestamp","source","destination"],"where":"rideId="+rideId,"where_flag":1,"dual_request_flag":0}
            req=requests.post(ipaddr+"/api/v1/db/read",json=data)
        
            response_from_api=req.text
            resp1=response_from_api.split("\n")
            for resp in resp1:
                resp2=resp[1:-1].split(",")
                users=",".join(resp2[2:len(resp2)-3])
                pt=len(resp2)-4
                #retreiving the area_code using area_name
                src=list(dicts.keys())[list(dicts.values()).index(resp2[pt+2][1:].replace("'",""))] 
                #retreiving the area_code using area_name
                dst=list(dicts.keys())[list(dicts.values()).index(resp2[pt+3][1:].replace("'",""))] 
            
                if(len(resp2)>1):
                    return jsonify(
                        rideId=int(resp2[0].replace("'","").replace(" ","")),
                        created_by=resp2[1].replace("'","").replace(" ",""),
                        ride_users=users.replace("'","").replace(" ","").split(","),
                        timestamp=resp2[pt+1].replace("'","").replace(" ",""),
                        source=int(src),
                        destination=int(dst)
                        ),200
                    
                else:
                    return Response(status=400)
        else:
            return Response(status=400)
    else:
        return Response(status=405) #wrong method used

#API to list the ride details of all the rides from a source to a destination
@app.route("/api/v1/rides",methods=["GET"])
def list_source_to_destination():

    if(request.method=='GET'):

        source=request.args.get('source',None)
        destination=request.args.get('destination',None)
        print(source)
        fields=[]
        rows=[]
        dicts={}

        try:
            with open(areapath,'r') as csvfile:
                c = csv.reader(csvfile)
                for row in c:
                    dicts[row[0]]=row[1]
                source_place=dicts[source]
                destination_place=dicts[destination]
        except KeyError:
            return Response(status=400)#if the area_code is not within the range
    
        data={"table":"rides","insert":["rideId","created_by","ride_users","timestamp","source","destination"],"where":"source='"+source_place+"' AND destination='"+destination_place+"'","where_flag":1,"dual_request_flag":1}
        print(data)
        #request sent to receive all the rides
        req = requests.post(ipaddr+"/api/v1/db/read",json=data)
        print("Request's reply :",req)
        response_from_api=req.text
        resp1=response_from_api.split("\n")
        #if no rides exist between source and destination
        if(len(resp1)==1):
            print("Resp1:",resp1)
            return jsonify(),204 
        
        
        print("Resp 1 is:",resp1)

        fin_list=[]
        for resp in resp1:
            resp2=resp[1:-1].split(",")
            
            #checking if rides from source and destination are upcoming or not
            if(len(resp2)>1 and check_date(resp2[len(resp2)-3])):
                users=",".join(resp2[2:len(resp2)-3])
                print(users)
                pt=len(resp2)-4 #extracting ride users
                src=list(dicts.keys())[list(dicts.values()).index(resp2[pt+2][1:].replace("'",""))]
                dst=list(dicts.keys())[list(dicts.values()).index(resp2[pt+3][1:].replace("'",""))]
                #if ride has multiple riders
                if len(users.replace("'","").replace(" ","").split(","))!=0:
                    fin_list.append({
                    "rideId":int(resp2[0].replace("'","").replace(" ","")),
                    "created_by":resp2[1].replace("'","").replace(" ",""),
                    "users":users.replace("'","").replace(" ","").split(","),
                    "timestamp":resp2[pt+1].replace("'","").replace(" ",""),
                    "source":str(src),
                    "destination":str(dst)
                    })
                #if ride has a single rider    
                else:
                    fin_list.append({
                "rideId":int(resp2[0].replace("'","").replace(" ","")),
                "created_by" :resp2[1].replace("'","").replace(" ",""),
                "users":[],
                "timestamp":resp2[pt+1].replace("'","").replace(" ",""),
                "source":str(src),
                "destination":str(dst)
            })

        if(len(fin_list)==0):
            print("fin_list:",fin_list)
            return jsonify(),204 #if rides dont exist from source to destination
        else:    
            return jsonify(fin_list),200   
    else:
        return Response(status=405) #wrong method used

#API to delete a ride with a particluar rideId
@app.route("/api/v1/rides/<rideId>",methods=["DELETE"])
def del_ride(rideId):
    
    if(request.method=='DELETE'):
        #join=2 flag indicates a delete operation to the database
        data = {"rideId":rideId,"join":2} 
        #request to delete the ride with rideId=rideId
        req = requests.post(ipaddr+"/api/v1/db/write",json=data) 
        return Response(status=int(req.text))
    else:
        return Response(status=405) #wrong method used




#######################################################################################

#API to count the number of rides
@app.route("/api/v1/rides/count",methods=["GET"])
def ride_count():
    
    if(request.method=="GET"):

        data = {"table":"rides", "insert":"count(*)", "where_flag" : 0, "dual_request_flag":1}
        #request to retreive the number of rides
        req = requests.post(ipaddr+"/api/v1/db/read",json=data) 
        r=req.text[:-1]
        return int(r)
    else:
        return Response(status=405) #wrong method used


#######################################################################################



if __name__=="__main__":
    app.run(host="0.0.0.0",port=80,debug=True)
