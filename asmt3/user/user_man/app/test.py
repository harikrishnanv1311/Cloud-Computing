with open("count.json", "r") as jsonFile:
    data = json.load(jsonFile)

tmp = data["count"]
print(temp)
temp=temp+1
print(temp)
data["count"] = temp

with open("count.json", "w") as jsonFile:
    json.dump(data, jsonFile)