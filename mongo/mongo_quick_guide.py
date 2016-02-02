'''
PyMongo quick start demo
'''
import datetime
from pymongo import MongoClient
'import PyMongo'

'A document'
post = {
    "author": {"first_name": "Nick", "last_name": "Tang"},
    "text": "My first blog post!",
    "tags": ["mongodb", "python", "pymongo"],
    "date": datetime.datetime.utcnow()
}

client = MongoClient('localhost', 27017)
'Setup connection. you can also use MongoDB uri "mongodb://localhost:27017/"'

db = client.test
collection = db.test_collection
'get a database and collection, also you can use db["test"]'

post_id = collection.insert_one(post).inserted_id
'insert a document, insert_one() returns an instance of InsertOneResult. '

doc = collection.find_one({"author": {"first_name": "Nick", "last_name": "Tang"}})
print(doc)
'query a document'

collection.find_one({"_id": post_id})
'query by _id'



