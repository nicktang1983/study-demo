'''
pymongo -- mapreduce sample
'''

from pymongo import MongoClient
from bson.code import Code

db = MongoClient().mydb
db.things.drop()
db.things.insert_many([
    {"x": 1, "tags": ["dog", "cat"]},
    {"x": 2, "tags": ["cat"]},
    {"x": 2, "tags": ["mouse", "cat", "dog"]},
    {"x": 3, "tags": []}
    ])

'''
count the number of occurrences for each tag
'''

mapper = Code('''
    function () {
        this.tags.forEach(function(z){
                emit(z, 1);
            });
    }
    ''')


reducer = Code('''
    function (key, values) {
        var total = 0;
        for (var i = 0; i < values.length; i++) {
            total += values[i];
        }
        return total;
    }

    ''')

result = db.things.map_reduce(mapper, reducer, "myresult")

for doc in result.find():
    print(doc)