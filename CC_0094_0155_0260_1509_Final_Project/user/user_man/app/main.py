from flask import Flask,render_template,jsonify,request,abort,Response
from flask_cors import CORS
import time
import sqlite3
import requests
import re
import csv
import datetime
import json 

# CSV file provided to map the names with their respective numbers 
areapath = "AreaNameEnum.csv"

# the ip addresses of the instances containing the orchestrator , user and ride respectively 
ipaddr = "http://35.169.72.238"
ipaddr_user = "http://34.236.8.161"
ipaddr_ride = "http://3.208.45.172"


# Intialisation of Flask "app"
app = Flask(__name__)

# To support Cross Origin Resorce Sharing 
CORS(app)



# Function to check date

def check_date(date):
    rem_quotes=date.replace("'","")
    rep_date=rem_quotes.replace(":","-").split("-")
    ##print(rep_date)
    rep_date1=[int(i) for i in rep_date]
    fin_date=datetime.datetime(rep_date1[2],rep_date1[1],rep_date1[0],rep_date1[5],rep_date1[4],rep_date1[3])
    cur_date=datetime.datetime.now()
    if(fin_date>cur_date):
        return 1
    else:
        return 0

#######################################################################################
    
    
# API to create a new user

@app.route("/api/v1/users",methods=["PUT"])
def add_user():
    if(request.method=="PUT"):
        try:
            
            #Receiving the username and pasword 
            username=request.get_json(force=True)["username"]
            password=request.get_json()["password"]
            print(type(password))
            string=password.lower()
            print(string)
            x=re.findall("[g-z]",string)
            print(x)
            flag=len(x)
            print("HERE",flag,len(password),username)
            
            # To check whether the given details meet the criteria , i.e , password field being a SHA1 hash hex and username field is not blank
            if(flag==0 and len(password)==40 and username!=""):
                print("In")
                
                # The user name and password are stored as json in the variable 'data' and are sent to the orchestrator
                # the join:0 indicates our write api in worker.py that it is a write request made by user and thus user tables needs to be updated
                data={"table":"users","username":username,"password":password,"join":0}
                print("Json Data:",data)
                print("Request being sent is:",ipaddr+"/api/v1/db/write")
                
                #The 'req' stores the response sent by the orchestrator 
                req=requests.post(ipaddr+"/api/v1/db/write",json=data)
                print("In after request")
                return Response(status=int(req.text))
            else:
                print("THE ERROR IS(1):",e)
                return Response(status=400)

        except Exception as e:
            print("THE ERROR IS(2):",e)
            return Response(status=400)

    else:
        return Response(status=405)

#######################################################################################


# API to remove user 

@app.route("/api/v1/users/<username>",methods=["DELETE"])
def del_users(username):
    if(request.method=="DELETE"):
       # the join:3 indicates our write api in worker.py that it is a write request made by user to delete an existing user and thus user tables needs to be updated
        data={"username":username,"join":3}
        req = requests.post(ipaddr+"/api/v1/db/write",json=data)
        return Response(status=int(req.text))
    else:
        return Response(status=405)
    
#######################################################################################

# API to list all the users 

@app.route("/api/v1/users",methods=["GET"])
def list_users():

    if(request.method=="GET"):
        s=[]
        print("check")
        try:
            # the where_flag indicates whether the request made by the container contains a 'WHERE' clause ; where_flag :0 indicating 'WHERE' clause not used and where_flag:1 indicating 'WHERE' clause used
            # the dual_request_flag is to check whether the read request should be counted or not for scaling the slaves ; 1 indicating it should be counted for scaling and 0 indicating it should not be counted for scaling  
            data={"table":"users","insert":["username"],"where_flag":0,"dual_request_flag":1}
            req = requests.post(ipaddr+"/api/v1/db/read",json=data)
            return jsonify(req.text.split(","))
            
        except:
            return Response(status=400)
    else:
        return Response(status=405)


#######################################################################################


if __name__=="__main__":
    app.run(host="0.0.0.0",port=80,debug=True)


