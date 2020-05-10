from flask import Flask,render_template,jsonify,request,abort,Response
import time
import sqlite3
import requests
import re
import csv
import datetime
import json 

# path = "app.db"
areapath = "AreaNameEnum.csv"
#ipaddr = "http://35.169.72.238"
ipaddr="http://localhost:8000"
ipaddr_user = "http://34.236.8.161"
ipaddr_ride = "http://3.208.45.172"

# con = sqlite3.connect("app.db")

# cur=con.cursor()
# con.execute("PRAGMA foreign_keys = ON")
# cur.execute("CREATE TABLE IF NOT EXISTS users(username TEXT primary key NOT NULL, password TEXT NOT NULL)")
# cur.execute("CREATE TABLE IF NOT EXISTS rides(rideId INTEGER PRIMARY KEY, created_by TEXT,ride_users TEXT, timestamp TEXT, source TEXT, destination TEXT,FOREIGN KEY (created_by) REFERENCES users (username) ON DELETE CASCADE)")
# con.commit()

# con.close()

#from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///./'
# db = SQLAlchemy(app)

# class User(db.Model):
#     username = db.Column(db.String(80), primary_key = True, nullable = False)
#     password = db.Column(db.String(40))

# class Rides(db.Model):
#     rideId = db.Column(db.Integer, nullable = False, primary_key = True)
#     username = db.Column(db.String(80), nullable = False)
#     source = db.Column(db.String(80), nullable = False)
#     destination = db.Column(db.String(80), nullable = False)
#     timestamp = db.Column(db.String(20), nullable = False)


def check_date(date):
    #29-01-2020:30-20-18
    ##print(date)
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


@app.route("/api/v1/users",methods=["PUT"])
def add_user():
    if(request.method=="PUT"):
        try:
            print("LOL")
            username=request.get_json(force=True)["username"]
            print("yo")
            password=request.get_json()["password"]
            print(type(password))
            string=password.lower()
            print(string)
            x=re.findall("[g-z]",string)
            print(x)
            flag=len(x)
            print("this is the username ->",username)
            print("HERE",flag,len(password),username)
            # #print(flag)
            if(flag==0 and len(password)==40 and username!=""):
                print("In")
                data={"table":"users","username":username,"password":password,"join":0}
                req=requests.post(ipaddr+"/api/v1/db/write",json=data)
                print("inside after request")
                return Response(status=int(req.text))
            else:
                return Response(status=400)

        except Exception as e:
            return Response(status=400)

    else:
        return Response(status=405)




@app.route("/api/v1/users/<username>",methods=["DELETE"])
def del_users(username):
    if(request.method=="DELETE"):
        #print("entering here")
        data={"username":username,"join":3}
        req = requests.post(ipaddr+"/api/v1/db/write",json=data)
        return Response(status=int(req.text))
    else:
        return Response(status=405)
#######################################################################################
@app.route("/api/v1/users",methods=["GET"])
def list_users():

    if(request.method=="GET"):
        s=[]
        print("check")
        try:
            data={"table":"users","insert":["username"],"where_flag":0,"dual_request_flag":1}
            req = requests.post(ipaddr+"/api/v1/db/read",json=data)
            return jsonify(req.text.split(","))
            # with sqlite3.connect(path) as con:
            #     cur=con.cursor()
            #     cur.execute("SELECT username FROM users")
            #     # print(cur)
            #     con.commit()
            #     status=200
            #     for i in cur:
            #         print(i)
            #         # s = s + str(i[0])
            #         s.append(str(i[0]))
            #         print(type(i))
            #     return jsonify(s)
            #     # return Response(status=200)
        except:
            return Response(status=400)
    else:
        return Response(status=405)

#######################################################################################

# @app.route("/api/v1/_count", methods=["GET"])
# def requestCount():
#     if(request.method=="GET"):

#         with open("count.json","r") as jsonFile:
#             data=json.load(jsonFile)

#         temp=data["count"]
#         l=[]
#         l.append(temp)

#         return jsonify(l),200

#     else:
#         return Response(status=405)

        

# @app.route("/api/v1/_count",methods=["DELETE"])
# def del_requestCount():
#     if(request.method=="DELETE"):
        
#         with open("count.json","r") as jsonFile:
#             data=json.load(jsonFile)

#         temp=data["count"]
#         temp=0
#         data["count"]=temp

#         with open("count.json", "w") as jsonFile:
#             json.dump(data, jsonFile)

#         return jsonify(),200

#     else:
#         return Response(status=405)



############################################################################


if __name__=="__main__":
    app.run(host="0.0.0.0",port=6000,debug=True)


