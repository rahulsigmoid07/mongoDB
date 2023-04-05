from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from pprint import pprint

try:
    conn = MongoClient()
    print("Connected successfully!!!")
except:  
    print("Could not connect to MongoDB")
  
# database name: mydatabase
db = conn.assignment

# Created or Switched to collection names: 
# collection = db.comments
movies=db.movies




class Movies:
    def addMovies(self,dict):
        movies.insert_one(dict)
    
    def topNMoviesWith(self,N,choice):
        if choice==1:
            pipe=[
                {"$match": {"imdb.rating":{"$ne":""}}},
                {"$sort":{ "imdb.rating":-1}},
                {"$limit":N},
                {"$project":{"_id":0,"title":1,"rating":"$imdb.rating"}}
            ]
        elif choice==2:
            year=int(input("Enter the year: "))
            pipe=[
                    {"$match": { "$and":[ {"imdb.rating":{"$ne":""}},{"year":year} ]}},
                    {"$sort":{ "imdb.rating":-1}},
                    {"$limit":N},
                    {"$project":{"_id":0,"title":1,"rating":"$imdb.rating","year":1}}
                ]
        elif choice==3:
            pipe=[
                    {"$match": { "$and":[ {"imdb.rating":{"$ne":""}},{"imdb.votes":{"$gte":1000}} ]}},
                    {"$sort":{ "imdb.rating":-1}},
                    {"$limit":N},
                    {"$project":{"_id":0,"title":1,"rating":"$imdb.rating","votes":"$imdb.votes"}}
                ]   
        elif choice==4:
            pat=input("Enter the pattern: ")
            pipe=[
                    {"$match": { "title": {"$regex":pat,"$options":"i"}}},
                    {"$sort":{ "tomatoes.viewer.rating":-1}},
                    {"$limit":N},
                    {"$project":{"_id":0,"title":1,"rating":"$tomatoes.viewer.rating"}}
                ]
        else:
            print("Wrong choice")
            return
        
        pprint(list(movies.aggregate(pipeline=pipe))) 
    
    def topNDirector(self,N,choice):
        if choice == 1:
            pipe=[
                    {"$unwind":"$directors"},
                    {"$group":{"_id":"$directors","noOfMovies":{"$sum":1}}},
                    {"$sort":{"noOfMovies":-1}},
                    {"$limit":N}
                ]
        elif choice ==2:
            year=int(input("Enter the year: "))
            pipe=[
                {"$match":{"year":year}},
                {"$unwind":"$directors"},
                {"$group":{"_id":"$directors","noOfMovies":{"$sum":1}}},
                {"$sort":{"noOfMovies":-1}},
                {"$limit":N}
            ]
        elif choice ==3:
            genre=input("Enter the genre: ")
            pipe=[
                {"$match":{"genres":genre}},
                {"$unwind":"$directors"},
                {"$group":{"_id":"$directors","noOfMovies":{"$sum":1}}},
                {"$sort":{"noOfMovies":-1}},
                {"$limit":N}
            ]
        else:
            print("Wrong choice")
            return 
        
        pprint(list(movies.aggregate(pipeline=pipe))) 

    def topNActors(self,N,choice):
        if choice == 1:
            pipe=[
                {"$unwind":"$cast"},
                {"$group":{"_id":"$cast","noOfMovies":{"$sum":1}}},
                {"$sort":{"noOfMovies":-1}},
                {"$limit":N}
            ]
        elif choice == 2:
            year=int(input("Enter the year: "))
            pipe=[
                {"$match":{"year":year}},
                {"$unwind":"$cast"},
                {"$group":{"_id":"$cast","noOfMovies":{"$sum":1}}},
                {"$sort":{"noOfMovies":-1}},
                {"$limit":N}
            ]
        elif choice == 3:
            genre=input("Enter the genre: ")
            pipe=[
                {"$match":{"genres":genre}},
                {"$unwind":"$cast"},
                {"$group":{"_id":"$cast","noOfMovies":{"$sum":1}}},
                {"$sort":{"noOfMovies":-1}},
                {"$limit":N}
            ]
        else:
            print("Wrong choice")
            return
        
        pprint(list(movies.aggregate(pipeline=pipe))) 
 
    def topNMoviesForAGenre(self,N):
        pipe=[
            {"$unwind":"$genres"},
            {"$group":{"_id":"$genres"}}
        ]
        for i in list(movies.aggregate(pipe)):
            genre=i['_id']
            print("Genre: "+genre)
            pipe=[
                {"$match":{"genres":genre}},
                {"$sort":{"imdb.rating":-1}},
                {"$match":{"imdb.rating":{"$ne":""}}},
                {"$project":{"_id":0,"title":1,"rating":"$imdb.rating"}},
                {"$limit":N}
            ] 
            pprint(list(movies.aggregate(pipe))) 
                         

def main():
    obj=Movies()
    print("""
        1. Top 'N' movies with
        2. Top 'N' director with
        3. Top 'N' actors with
        4. Top 'N' movies for every genre 
        """)
    ch=int(input("Enter Choice:"))
    N=int(input("Enter N:"))
    if ch == 1:
        print("""
            Enter Choice:
            1. Highest IMDB rating
            2. Highest IMDB rating in a given year
            3. Highest IMDB rating with number of votes > 1000
            4. Title matching with a pattern and sorted according to tomatoes rating
            """)
        ch2=int(input("Enter Choice:"))
        obj.topNMoviesWith(N,ch2)
    elif ch==2:
        print("""
            Enter Choice:
            1. Most number of movies
            2. Most number of movies in a given year
            3. Most number of movies in a particular genre
            """)
        ch2=int(input("Enter Choice:"))
        obj.topNDirector(N,ch2)
        
    elif ch==3:
        print("""
            Enter Choice:
            1. Most number of movies
            2. Most number of movies in a given year
            3. Most number of movies in a particular genre
            """)
        ch2=int(input("Enter Choice:"))
        obj.topNActors(N,ch2)
    elif ch==4:
        obj.topNMoviesForAGenre(N)
    else:
        print("Wrong choice")
        
    
    
if __name__ == "__main__":
    main()