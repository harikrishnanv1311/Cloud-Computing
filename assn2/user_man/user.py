from flask import Flask,render_template,jsonify,request,abort,Response
import time
import sqlite3
import requests
import re
import csv
import datetime

con = sqlite3.connect("app.db")


cur=con.cursor()
con.execute("PRAGMA foreign_keys = ON")
cur.execute("CREATE TABLE IF NOT EXISTS users(username TEXT primary key NOT NULL, password TEXT NOT NULL)")
cur.execute("CREATE TABLE IF NOT EXISTS rides(rideId INTEGER PRIMARY KEY, created_by TEXT,ride_users TEXT, time_stamp TEXT, source TEXT, destination TEXT,FOREIGN KEY (created_by) REFERENCES users (username) ON DELETE CASCADE)")
con.commit()


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




@app.route("/api/v1/db/write",methods=["POST"])
def write_db():
    print("entering write")
    #access book name sent as JSON object
    #in POST request body
    join = request.get_json()["join"]
    if(join==0):
        table=request.get_json()["table"]
        if(table=="users"):
        ##print(type(p))
            try:
                with sqlite3.connect("app.db") as con:
                    username=request.get_json()["username"]
                    password=request.get_json()["password"]
                    print( "table is %s, username is %s, password is %s"%(table,username,password))
                    cur = con.cursor()
                    cur.execute("INSERT into users values (?,?)",(username,password))
                    con.commit()
                    return Response(status=201)
            except Exception as e:
                    #print(e)
                    return Response(status=400)

                   
        if(table=="rides"):
            try:
                created_by=request.get_json()["created_by"]
                time_stamp=request.get_json()["time_stamp"]
                source=request.get_json()["source"]
                destination=request.get_json()["destination"]
                
                ride_users=""
           

                with sqlite3.connect("app.db") as con:

                    cur = con.cursor()
                    #cur.execute("DELETE FROM rides WHERE created_by=\"'hk'\"")
                    ##print("\nBefore Insertion\n")
                    #query="INSERT INTO rides (rideId,created_by, ride_users, time_stamp, source, destination) values (None," + "'" + created_by + "'" + "," +  "'" + ride_users + "'" + "," + "'" + time_stamp + "'" + "," + "'" + source + "'" + "," + "'" + destination + "'" + ")"
                    ##print(query)
                    
                    #print(created_by,time_stamp,source,destination)
                    n=cur.execute("SELECT max(rideId) FROM rides").fetchone()[0]
                    if(n==None):
                        #print("Inside")
                        m=0
                    else:
                        m = n
                    #m=cur.fetchone()[0]
                    #print(m)
                    cur.execute("INSERT into rides (rideId,created_by,ride_users,time_stamp,source,destination) values (?,?,?,?,?,?)",(m+1,created_by,ride_users,time_stamp,source,destination))
                    print("Surya")
                    con.commit()
                    status=201
                    return Response(status=201)
            except Exception as e:
                print(e)
                return Response(status=400)

    if(join==1):
        try:
            with sqlite3.connect("app.db") as con:
                rideId = request.get_json()["rideId"]
                username = request.get_json()["username"]
                
                ##print(username)
                u=""
                #check_rides_q = "SELECT COUNT(*) FROM rides WHERE rideId"+str(rideId)
               
                cur = con.cursor()
               
                '''
                query="UPDATE rides SET ride_users="+"'hari'"+" WHERE rideId=1"
                cur.execute(query)
                '''
                cur.execute("SELECT count(*) FROM rides WHERE rideId="+str(rideId))
                ride_flag=cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM users WHERE username="+"'"+str(username)+"'")
                user_flag=cur.fetchone()[0]
                con.commit()

            
                if(ride_flag and user_flag):
                    cur.execute("SELECT ride_users FROM rides WHERE rideId="+str(rideId))
                    con.commit()
                    r_u=cur.fetchone()[0].split(",")
                    if username not in r_u:
                        for i in r_u:
                            if(i!=""):
                                u = u + i + ","
                        u += username
                        query="UPDATE rides SET ride_users="+"'"+str(u)+"'"+" WHERE rideId="+str(rideId)
                        cur.execute(query)
                        con.commit()
                        return Response(status=200)
               
                    else:
                        return Response(status=400)
                else:
                    return Response(status=400)
        except Exception as e:
            return Response(status=405)
            
        

    if(join==2):
        try:
            with sqlite3.connect("app.db") as con:
                rideId = request.get_json()["rideId"]
                cur = con.cursor()
                cur.execute("SELECT count(*) FROM rides WHERE rideId="+str(rideId))
                ride_flag=cur.fetchone()[0]
                con.commit()

                if(ride_flag):
                    cur.execute("DELETE FROM rides WHERE rideId="+str(rideId))
                    con.commit()
                    return Response(status=200)
                else:
                    return Response(status=400)
        except Exception as e:
            return Response(status=405)
           

    if(join==3):
        try:
            with sqlite3.connect("app.db") as con:
                #print("Connected")
                username = request.get_json()["username"]
                cur = con.cursor()
                cur.execute("SELECT count(*) FROM users where username="+"'"+str(username)+"'")
                user_flag = cur.fetchone()[0]
                con.commit()
                if(user_flag):
                    cur.execute("DELETE FROM users WHERE username="+"'"+str(username)+"'")
                    #print("Executed")
                    con.commit()
                    return Response(status=200)
                else:
                    return Response(status=400)
        except Exception as e:
            return Response(status=400)

@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
    try:
        with sqlite3.connect("app.db") as con:
            cur = con.cursor()
            cur.execute("DELETE FROM users")
            #cur.execute("DELETE FROM rides")
            con.commit()
            return Response(status=200)

    except:
        return Response(status=405)
               
@app.route("/api/v1/users",methods=["GET"])
def list_users():
    s=""
    print("check")
    try:
        with sqlite3.connect("app.db") as con:
            cur=con.cursor()
            cur.execute("SELECT username FROM users")
            # print(cur)
            con.commit()
            status=200
            for i in cur:
                print(i)
                s = s + str(i) + "\n"
                print(type(i))
            return s
            # return Response(status=200)
    except:
        return Response(status=405)



@app.route("/api/v1/users",methods=["PUT"])
def add_user():
    if(request.method=="PUT"):
        try:
            ##print("LOL")
            username=request.get_json(force=True)["username"]
            ##print("yo")
            password=request.get_json()["password"]
            ##print(type(password))
            string=password.lower()
            # #print(string)
            x=re.findall("[g-z]",string)
            # #print(x)
            flag=len(x)
            print("HERE",flag,len(password))
            # print(flag)
            if(flag==0 and len(password)==40 and username!=""):
                print("In")
                data={"table":"users","username":username,"password":password,"join":0}
                req=requests.post("http://127.0.0.1:5000/api/v1/db/write",json=data)
                if(req.status_code==200):
                      return Response(status=201)
                else:
                      return Response(status=req.status_code)
            else:
                return Response(status=400)

        except Exception as e:
            return Response(status=400)

    else:
        return Response(status=405)


@app.route("/")
def greet():
    return "HERE IN THE USER PAGE!"


@app.route("/api/v1/users/<username>",methods=["DELETE"])
def del_users(username):
    #print("entering here")
    data={"username":username,"join":3}
    req = requests.post("http://54.209.210.47/api/v1/db/write",json=data)
    return Response(status=req.status_code)


if __name__=="__main__":
    app.run(host="0.0.0.0",debug=True)

