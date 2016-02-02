#-*- coding: UTF-8 -*-

'''
Study aggregate in mongodb 3.2
'''

from pymongo import MongoClient
import datetime
from pprint import pprint

#Preparation
post = {
    "author": {"first_name": "Nick", "last_name": "Tang"},
    "text": "My first blog post!",
    "tags": ["mongodb", "python", "pymongo"],
    "date": datetime.datetime.utcnow(),
    "level": 1,
    "cc": {
        "level":1,
        "dd": {
            "level": 1
        }
    }
}

db = MongoClient().test
db.aggregate_demo.drop()
db.aggregate_demo.insert_one(post)

##$Project
'''
support embedded doc field
添加不在源文档的内容，需要使用$literal表示
'''
cursor = db.aggregate_demo.aggregate(
    [
        {
            "$project": {
                "author.first_name": 1,
                "tags": 1,
                "_id": 0,
                "full_name": {"$concat": ["$author.first_name", "$author.last_name"]},
                "new": {"$literal": "value"}
            }
        }
    ]
)


pprint(list(cursor))

'''
[{u'author': {u'first_name': u'Nick'},
  u'full_name': u'NickTang',
  u'new': u'value',
  u'tags': [u'mongodb', u'python', u'pymongo']}]
'''



##$match
cursor = db.aggregate_demo.aggregate(
    [
        {
            "$match": {"author.first_name": "Nick"}
        }
    ]
)

pprint(list(cursor))

#can not found, if reshape by project
cursor = db.aggregate_demo.aggregate(
    [
        {
            "$project": {
                "tags": 1
            }
        },
        {
            "$match": {"author.first_name": "Nick"}
        }
    ]
)

pprint(list(cursor))

'''
[{u'_id': ObjectId('56a1e6bcc202ed51073bc58e'),
  u'author': {u'first_name': u'Nick', u'last_name': u'Tang'},
  u'date': datetime.datetime(2016, 1, 22, 8, 22, 20, 886000),
  u'tags': [u'mongodb', u'python', u'pymongo'],
  u'text': u'My first blog post!'}]

[]
'''


#$redact
cursor = db.aggregate_demo.aggregate(
    [
        {
            "$redact": "$$DESCEND"
        }
    ]
)

pprint(list(cursor))


#$limit

#$skip

#$unwind
##output 3 docs
cursor = db.aggregate_demo.aggregate(
    [
        {
            "$unwind": "$tags"
        }
    ]
)

pprint(list(cursor))


#$group
sum_test = [
    { "_id" : 1, "item" : "abc", "price" : 10, "quantity" : 2, "date" : datetime.datetime(2014, 1, 1) },
    { "_id" : 2, "item" : "jkl", "price" : 20, "quantity" : 1, "date" : datetime.datetime(2014, 2, 3) },
    { "_id" : 3, "item" : "xyz", "price" : 5, "quantity" : 5, "date" : datetime.datetime(2014, 2, 3) },
    { "_id" : 4, "item" : "abc", "price" : 10, "quantity" : 10, "date" : datetime.datetime(2014, 2, 15)  },
    { "_id" : 5, "item" : "xyz", "price" : 5, "quantity" : 10, "date" : datetime.datetime(2014, 2, 15) }
]
db.aggregate_demo.insert_many(sum_test)

cursor = db.aggregate_demo.aggregate(
    [
        {
            "$group":
            {
                "_id": { "day": { "$dayOfYear": "$date"}, "year": { "$year": "$date" } },
                "totalAmount": { "$sum": { "$multiply": [ "$price", "$quantity" ] } },
                "count": { "$sum": 1 }
            }
        }
    ]
)
print "*"*20
pprint(list(cursor))


cursor = db.aggregate_demo.aggregate(
    [
        {
            "$group":
            {
                "_id": { "day": { "$dayOfYear": "$date"}, "year": { "$year": "$date" } },
                "totalAmount": { "$sum": { "$add": [ "$price", "$quantity" ] } },
                "count": { "$sum": 1 }
            }
        }
    ]
)
print "*"*20
pprint(list(cursor))


cursor = db.aggregate_demo.aggregate(
    [
        {
            "$group":
            {
                "_id": None,
                "totalAmount": { "$sum": { "$add": [ "$price", "$quantity" ] } },
                "count": { "$sum": 1 }
            }
        }
    ]
)
print "*"*20
pprint(list(cursor))


cursor = db.aggregate_demo.aggregate(
    [
        {
            "$project": {
                "totalAmount": {"$add": ["$price", "$quantity"]}
            }
        },
        {
            "$group":
            {
                "_id": None,
                "totalAmount": { "$sum": "$totalAmount"},
                "count": { "$sum": 1 }
            }
        }
    ]
)
print "*"*20
pprint(list(cursor))

