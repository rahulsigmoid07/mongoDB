
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
        
    def top10theatersNear(self,coords):
         theatres.create_index([("location.geo", "2dsphere")])
         pprint(list(theatres.find(
        {
        "location.geo": {
            "$near": {
            "$geometry": {
                "type": "Point" ,
                "coordinates": coords
            }}
        },
      
        
        },{"_id":0,"location":1,"theaterId":1}).limit(10)))
        
        
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