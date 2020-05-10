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



app = Flask(__name__)

#########################################################################################

#dual_request_flag => 1 -> read should be counted , 0 -> read shouldnt be counted
#########################################################################################
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




@app.route("/api/v1/rides/<rideId>",methods=["POST"])
def join_ride(rideId):

    # with open("count.json", "r") as jsonFile:
    #     data = json.load(jsonFile)

    # temp = data["count"]
    # temp=temp+1
    # data["count"] = temp

    # with open("count.json", "w") as jsonFile:
    #     json.dump(data, jsonFile)
    

    username=request.get_json()["username"]
    fetch_data = {"table":"users","insert":["username"],"where_flag":0,"dual_request_flag":0}
    x=requests.post(ipaddr+"/api/v1/db/read",json=fetch_data)
    if x.status_code==204:return Response(status=400)
    l1 = x.text.split(",")
    print(l1)
    if(username not in l1):
        print("")
        return Response(status=400)
    data = {"table":"rides","rideId":rideId,"username":username,"join":1}
    req = requests.post(ipaddr+"/api/v1/db/write",json=data)
    print("user joined ")
    return Response(status=int(req.text))


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
            fetch_data = {"table":"users","insert":["username"],"where_flag":0,"dual_request_flag":0}

            x=requests.post(ipaddr+"/api/v1/db/read",json= fetch_data)
            print("yolo")
            if x.status_code==204:return Response(status=400)
            print("yolo2")
            l1= x.text.split(",")
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
                return Response(status=int(req.text))

        except Exception as e:
                return Response(status=400)
    else:
        return Response(status=405)    

@app.route("/api/v1/rides/<rideId>",methods=["GET"])
def list_details(rideId):
    
    if(request.method=="GET"):
        check_ride={"table":"rides","insert":["rideId"],"where_flag":0,"dual_request_flag":1} 
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

            data={"table":"rides","insert":["rideId","created_by","ride_users","timestamp","source","destination"],"where":"rideId="+rideId,"where_flag":1,"dual_request_flag":0}
            req=requests.post(ipaddr+"/api/v1/db/read",json=data)
        
            response_from_api=req.text
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
        response_from_api=req.text
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
        return Response(status=int(req.text))
    else:
        return Response(status=405)




#######################################################################################

#######################################################################################

@app.route("/api/v1/rides/count",methods=["GET"])
def ride_count():
    
    if(request.method=="GET"):
        data = {"table":"rides", "insert":"count(*)", "where_flag" : 0, "dual_request_flag":1}
        req = requests.post(ipaddr+"/api/v1/db/read",json=data)
        r=req.text[:-1]
        return int(r)
    else:
        return Response(status=405)


#######################################################################################



if __name__=="__main__":
    app.run(host="0.0.0.0",port=5000,debug=True)
