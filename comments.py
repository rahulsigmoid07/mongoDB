from datetime import datetime
from pymongo import MongoClient
from pprint import pprint


try:
    conn = MongoClient()
    print("Connected successfully!!!")
except:  
    print("Could not connect to MongoDB")
  
# database name: mydatabase
db = conn.assignment

# Created or Switched to comments names: 
comments = db.comments
movies=db.movies

class Comments:
    
    def insert_in_db(self,dict):
        comments.insert_one(dict)
    
    def top10UserWithMaxComment(self):
        pipeline = [
        {
            "$group": {
                "_id": "$name",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
        {
            "$limit": 10
        }
        ]
        pprint(list(comments.aggregate(pipeline)))
            
    def top10MovieswithMaxmComment(self):
        pipeline = [
        {
            "$group": {
                "_id": "$movie_id",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": -1
            }
        },
        {
            "$limit": 10
        }
        ]
        results = comments.aggregate(pipeline)
        for doc in results:
            movie = db.movies.find_one({"_id": doc['_id']})
            print("Title of movie: " + movie['title'] + " , count: " + str(doc['count']))

    def givenYearTotalCommentEachMonth(self,year):
        pipeline=[
            # {"year":{"$year":"$date"} , "month":{"$month":"$date"}
            { "$project": {"year":{"$year":"$date"} , "month":{"$month":"$date"} }},
            {"$match" : { "year":year }},
            {"$group" : {"_id" : "$month", "count" : {"$sum":1}}},
            {"$project": {"month":"$_id","count":1,"_id":0 }},
            {"$sort" : {"month":1 }}
        ]
        pprint(list(comments.aggregate(pipeline)))
        

def main():
    
    obj=Comments()
    print("""Enter the choice:
          1.Top 10 users with most comment
          2.top 10 movies with maximum comment.
          3.for a given year,find comment each month.
          4.for inserting in commentss.
          """)
    choice=int(input())
    if(choice==1):
        obj.top10UserWithMaxComment()
    elif(choice==2):
        obj.top10MovieswithMaxmComment()
    elif(choice==3):
        year=int(input("Enter the year: "))
        obj.givenYearTotalCommentEachMonth(year)
    elif(choice==4):
        obj.insert_in_db()
    else:
        print("wrong choice")
    
    
if __name__ == "__main__":
    main()
    
    