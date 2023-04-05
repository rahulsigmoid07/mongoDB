
from pymongo import MongoClient
from pprint import pprint

try:
    conn = MongoClient()
    print("Connected successfully!!!")
except:  
    print("Could not connect to MongoDB")
  
# database name: mydatabase
db = conn.assignment

# Created or Switched to collection names: 
theatres=db.theatres


class theaters:
    def addMovies(self,dict):
        theatres.insert_one(dict)
        
    def top10CitiesMostTheaters(self):
        pipe=[
            {"$group":{"_id":"$location.address.city","cnt":{"$sum":1}}},
            {"$sort":{"cnt":-1}},
            {"$limit":10}
        ]
        pprint(list(theatres.aggregate(pipe)))
        
    def top10theatersNear(self,cod):
        pipe=[
            {"$project":{"_id":0,"theaterId":1,"cord":"$location.geo.coordinates"}},
        ]
        result=list(theatres.aggregate(pipe))
        # pprint(result)
        lst=[]
        for i in result:
            lst.append(( i['theaterId'], (cod[0]-i['cord'][0])*(cod[0]-i['cord'][0]) + (cod[1]-i['cord'][1])*(cod[1]-i['cord'][1]) ))
        lst.sort(key=lambda a: a[1])
        for i in range(0,10):
            print(f"TheaterId: {lst[i][0]}")
        
        
def main():
    obj=theaters()
    print("""
            Enter Choice:
            1. Top 10 cities with most theaters
            2. Top 10 theaters nearby given co-ordinates
            """)
    ch=int(input())
    if ch == 1:
        obj.top10CitiesMostTheaters()
    elif ch==2:
        l=[]
        l.append(float(input("Enter Latitute: ")))
        l.append(float(input("Enter Longitude: ")))
        obj.top10theatersNear(l)
    else:
        print("Wrong choice")
        
        

if __name__ == "__main__":
    main()