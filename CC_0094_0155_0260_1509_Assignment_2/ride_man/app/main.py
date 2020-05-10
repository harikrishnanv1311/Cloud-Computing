from flask import Flask,render_template,jsonify,request,abort,Response
import time
import sqlite3
import requests
import re
import csv
import datetime
import json

path = "app.db"
areapath = "AreaNameEnum.csv"
ipaddr = "http://18.233.235.204:8000"

con = sqlite3.connect("app.db")

cur=con.cursor()
con.execute("PRAGMA foreign_keys = ON")
cur.execute("CREATE TABLE IF NOT EXISTS users(username TEXT primary key NOT NULL, password TEXT NOT NULL)")
cur.execute("CREATE TABLE IF NOT EXISTS rides(rideId INTEGER PRIMARY KEY, created_by TEXT,ride_users TEXT, timestamp TEXT, source TEXT, destination TEXT,FOREIGN KEY (created_by) REFERENCES users (username) ON DELETE CASCADE)")
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

@app.route("/api/v1/db/write",methods=["POST"])
def write_db():
    #access book name sent as JSON object
    #in POST request body
    join = request.get_json()["join"]
    print("join is",join)
    if(join==0):
        table=request.get_json()["table"]
        if(table=="users"):
        ##print(type(p))
            try:
                print("enter1")
                with sqlite3.connect(path) as con:
                    print("enter2")
                    username=request.get_json()["username"]
                    password=request.get_json()["password"]
                    #return "table is %s, username is %s, password is %s"%(table,u,p)
                    cur = con.cursor()
                    print("enter 2.5")
                    q="INSERT into users values ('"+username+"','"+password+"')"
                    print(q)
                    cur.execute(q)
                    print("enter 3")
                    con.commit()
                    return Response(status=201)
            except Exception as e:
                    #print(e)
                    return Response(status=400)

                   
        if(table=="rides"):
            try:
                print("In")
                created_by=request.get_json()["created_by"]
                timestamp=request.get_json()["timestamp"]
                source=request.get_json()["source"]
                destination=request.get_json()["destination"]
                
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
                    con.commit()
                    status=201
                    return Response(status=201)
            except Exception as e:
                print(e)
                return Response(status=400)

    if(join==1):
        try:
            with sqlite3.connect(path) as con:
                rideId = request.get_json()["rideId"]
                username = request.get_json()["username"]
                
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
                        return Response(status=200)
               
                    else:
                        return Response(status=400)
                else:
                    return Response(status=400)
        except Exception as e:
            return Response(status=405)
            
        

    if(join==2):
        try:
            with sqlite3.connect(path) as con:
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
            with sqlite3.connect(path) as con:
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
        with sqlite3.connect(path) as con:
            cur = con.cursor()
            cur.execute("DELETE FROM users")
            cur.execute("DELETE FROM rides")
            con.commit()
            return Response(status=200)
    except:
        return Response(status=405)
               


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
            # #print(flag)
            if(flag==0 and len(password)==40 and username!=""):
                #print("In")
                data={"table":"users","username":username,"password":password,"join":0}
                req=requests.post(ipaddr+"/api/v1/db/write",json=data)
                return Response(status=201)
            else:
                return Response(status=400)

        except Exception as e:
            return Response(status=400)

    else:
        return Response(status=405)


@app.route("/api/v1/rides/<rideId>",methods=["POST"])
def join_ride(rideId):
    username=request.get_json()["username"]
    x=requests.get("http://18.233.235.204:8080/api/v1/users")
    if x.status_code==204:return Response(status=400)
    l1 = json.loads(x.text)
    print(l1)
    if(username not in l1):
        print("")
        return Response(status=400)
    data = {"rideId":rideId,"username":username,"join":1}
    req = requests.post(ipaddr+"/api/v1/db/write",json=data)
    print("user joined ")
    return Response(status=200)

@app.route("/")
def greet():
    return "HERE AT RIDE PAGE!"


@app.route("/api/v1/rides",methods=["POST"])
def create_ride():
    if(request.method=='POST'):
        dicts={}
        try:
            with open(areapath,'r') as csvfile:
                # print("Got Data")
                c = csv.reader(csvfile)
                for row in c:
                    dicts[row[0]]=row[1]
            
            #print(dicts)
            #print(type(request.get_json()))
            created_by=request.get_json()["created_by"]
            x=requests.get("http://18.233.235.204:8080/api/v1/users")
            print("yolo")
            if x.status_code==204:return Response(status=400)
            print("yolo2")
            l1= json.loads(x.text)
            # l1 = s.split("\n")
            # print("#########")
            print(l1)
            # print("#########")
            if(created_by not in l1):
                return Response(status=400)
            timestamp=request.get_json()["timestamp"]
            #print("2")
            source=request.get_json()["source"]
            #print("3")
            destination=request.get_json()["destination"]
            #print(timestamp)
            if(time.strptime(timestamp, "%d-%m-%Y:%S-%M-%H")):
                #print("correct format")
                data = {"join":0,"table":"rides","created_by":created_by,"timestamp":timestamp,"source":dicts[source],"destination":dicts[destination]}
                #print(data)
                req = requests.post(ipaddr+"/api/v1/db/write",json=data)
                return Response(status=201)

        except Exception as e:
                return Response(status=400)
    else:
        return Response(status=405)    

@app.route("/api/v1/rides/<rideId>",methods=["GET"])
def list_details(rideId):
    if(request.method=="GET"):
        check_ride={"table":"rides","insert":["rideId"],"where_flag":0}
        req_ride=requests.post(ipaddr+"/api/v1/db/read",json=check_ride)
        response_from_api=req_ride.json()["string"]
        resp1=response_from_api.split("\n")
        dicts={}

        csvfile=open(areapath,'r')
        c = csv.reader(csvfile)
    
        ##print(fields)
        for row in c:
            dicts[row[0]]=row[1]
        if(len(resp1)>1):

            data={"table":"rides","insert":["rideId","created_by","ride_users","timestamp","source","destination"],"where":"rideId="+rideId,"where_flag":1}
            req=requests.post(ipaddr+"/api/v1/db/read",json=data)
        
            response_from_api=req.json()["string"]
            resp1=response_from_api.split("\n")
            #print(resp1)
            for resp in resp1:
                resp2=resp[1:-1].split(",")
                users=",".join(resp2[2:len(resp2)-3])
                pt=len(resp2)-4
                src=list(dicts.keys())[list(dicts.values()).index(resp2[pt+2][1:].replace("'",""))]
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
                    return Response(status=204)
        else:
            return Response(status=204)
    else:
        return Response(status=405)

#200,204,400,405
@app.route("/api/v1/rides",methods=["GET"])
def list_source_to_destination():
    if(request.method=='GET'):

        source=request.args.get('source',None)
        destination=request.args.get('destination',None)
        print("FINALLY INSIDE!")
        print(source)
        fields=[]
        rows=[]
        dicts={}

        try:
            with open(areapath,'r') as csvfile:
                c = csv.reader(csvfile)
    
        ##print(fields)
                for row in c:
                    dicts[row[0]]=row[1]
                source_place=dicts[source]
                destination_place=dicts[destination]
        except KeyError:
            #print("source or destination doesnt exist")
            return Response(status=400)
    
        data={"table":"rides","insert":["rideId","created_by","ride_users","timestamp","source","destination"],"where":"source='"+source_place+"' AND destination='"+destination_place+"'","where_flag":1}
        print(data)
        req = requests.post(ipaddr+"/api/v1/db/read",json=data)
        print(req.json())
        response_from_api=req.json()["string"]
        resp1=response_from_api.split("\n")
        ##print(resp1)
        
        if(len(resp1)==1):
            return Response(status=204)
        #s="[\n"
        fin_list=[]
        for resp in resp1:
            resp2=resp[1:-1].split(",")
            
            
            #if(len(resp2)>1 and check_date(resp2[len(resp2)-3])):
            if(len(resp2)>1):
                ##print("content present")
                users=",".join(resp2[2:len(resp2)-3])
                print(users)
                pt=len(resp2)-4
                src=list(dicts.keys())[list(dicts.values()).index(resp2[pt+2][1:].replace("'",""))]
                dst=list(dicts.keys())[list(dicts.values()).index(resp2[pt+3][1:].replace("'",""))]
                #s=s+"{\n rideId : "+resp2[0]+"\n created_by : "+resp2[1]+"\n ride_users : "+users+"\n timestamp : "+resp2[pt+1]+"\n source : "+src+"\n destination : "+dst+"\n }"+",\n"
                #fin_list.append("{\n rideId : "+resp2[0]+"\n created_by : "+resp2[1]+"\n ride_users : "+users+"\n timestamp : "+resp2[pt+1]+"\n source : "+resp2[pt+2]+"\n destination : "+resp2[pt+3]+"\n }")
                if len(users.replace("'","").replace(" ","").split(","))!=0:
                    fin_list.append({
                    "rideId":int(resp2[0].replace("'","").replace(" ","")),
                    "created_by":resp2[1].replace("'","").replace(" ",""),
                    "users":users.replace("'","").replace(" ","").split(","),
                    "timestamp":resp2[pt+1].replace("'","").replace(" ",""),
                    "source":str(src),
                    "destination":str(dst)
                    })
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
            return Response(status=204)
        else:    
            #return s[:-2]+"\n]"
            #print(fin_list)
            return jsonify(fin_list),200   
    else:
        return Response(status=405) 

################################things to be added#####################################
@app.route("/api/v1/rides/<rideId>",methods=["DELETE"])
def del_ride(rideId):
    if(request.method=='DELETE'):
        data = {"rideId":rideId,"join":2}
        req = requests.post(ipaddr+"/api/v1/db/write",json=data)
        return Response(status=200)
    else:
        return Response(status=405)

@app.route("/api/v1/users/<username>",methods=["DELETE"])
def del_users(username):
    if(request.method=="DELETE"):
        #print("entering here")
        data={"username":username,"join":3}
        req = requests.post(ipaddr+"+"+"/api/v1/db/write",json=data)
        return Response(status=200)
    else:
        return Response(status=405)
#######################################################################################

if __name__=="__main__":
    app.run(host="127.0.0.1",port=8000,debug=True)
