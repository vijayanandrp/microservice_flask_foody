# PyMongo Tutorial : Insert, Read, Update, Delete in MongoDB

# !/usr/bin/env python3.5
# -*- coding: UTF-8 -*-

import pprint
import dateutil.parser
import datetime

try:
    from pymongo import MongoClient
except ImportError:
    raise ImportError('PyMongo is not installed in your machine.')

# The below code will connect on the default host and port.
client = MongoClient()

# The below code will connect on the specified  host and port.
client = MongoClient(host='127.0.0.1', port=27017, maxPoolSize=100)

# selecting database (use db)
database = client['test']

# selecting collection - kind of selecting table in NoSQL
collection = database['restaurants']

# get/query/fetch only one value from collection  without any conditions
result = collection.find_one()

# get/query/fetch all documents from collection without any conditions.
# Below will return all values from collection
result = collection.find()

#  inserting a sample document
sample_document_post = {
    "address":       {
        "street":   "2 Avenue",
        "zipcode":  "10075",
        "building": "1480",
        "coord":    [-73.9557413, 40.7720266]
    },
    "borough":       "Manhattan",
    "cuisine":       "Italian",
    "grades":        [
        {
            "date":  dateutil.parser.parse("2014-10-01T00:00:00Z"),
            "grade": "A",
            "score": 11
        },
        {
            "date":  dateutil.parser.parse("2014-01-16T00:00:00Z"),
            "grade": "B",
            "score": 17
        }
    ],
    "name":          "Vella",
    "restaurant_id": "41704620"
}

# post_id = collection.insert(sample_document_post)
# pprint.pprint(post_id)

#  Query by a Top Level Field
''' I am trying to get the above restaurant ID  from collection  '''
result = collection.find({"restaurant_id": "41704620"})

# Query by a Top Level Field
result = collection.find_one({"restaurant_id": "41704620"})

#   Query by a Top Level Field
''' The following operation finds documents whose borough field equals "Manhattan". '''
result = collection.find({"borough": "Manhattan"})

# Query by a Field in an Embedded Document
'''
To specify a condition on a field within an embedded document, use the dot notation.
Dot notation requires quotes around the whole dotted field name.
The following operation specifies an equality condition on the zipcode field in the address embedded document.
Example -
{"address": {
        "street": "2 Avenue",
        "zipcode": "10075",
        "building": "1480",
        "coord": [-73.9557413, 40.7720266 ]
    }}
'''
result = collection.find({"address.zipcode": "10075"})

#  Query by a Field in an Array
'''
The grades array contains embedded documents as its elements.
To specify a condition on a field in these documents, use the dot notation.
Dot notation requires quotes around the whole dotted field name.
The following queries for documents whose grades array contains an embedded document with a field grade equal to "B".
Example -
{"grades": [
        {
            "date": dateutil.parser.parse("2014-10-01T00:00:00Z"),
            "grade": "A",
            "score": 11
        },
        {
            "date": dateutil.parser.parse("2014-01-16T00:00:00Z"),
            "grade": "B",
            "score": 17
        }]}
'''
result = collection.find({"grades.grade": "B"})

#  Greater Than Operator ($gt)
''' Query for documents whose grades array contains an embedded document with a field score greater than 30. '''
result = collection.find({"grades.score": {'$gt': 30}})

#  Lesser Than Operator ($lt)
''' Query for documents whose grades array contains an embedded document with a field score lesser than 10. '''
result = collection.find({"grades.score": {'$lt': 10}})

#  Lesser Than Equals To Operator ($lte) / Greater Than Equals To Operator ($gte)
''' I would like to get all documents in the year 2015 '''
start_date = datetime.datetime(year=2015, month=1, day=1)
end_date = datetime.datetime(year=2015, month=12, day=31)
result = collection.find({'grades.date': {'$gte': start_date, '$lte': end_date}}).count()

# Combine Conditions - Logical AND
'''
You can specify a logical conjunction (AND) for a list of query conditions by
separating the conditions with a comma in the conditions document
'''
result = collection.find({"cuisine": "Italian", "address.zipcode": "10075"})

# Combine Conditions - Logical OR
'''
You can specify a logical disjunction (OR) for a list of query conditions by using the $or query operator.
'''
result = collection.find({'$or': [{"cuisine": "Italian", "address.zipcode": "10075"}]})

# Sort Query Results
'''
To specify an order for the result set, append the sort() method to the query.

For example, the following operation returns all documents in the restaurants collection,
sorted first by the borough field in ascending order, and then, within each borough,
by the "address.zipcode" field in ascending order:
'''
result = collection.find().sort("borough").sort("address.zipcode")

# Update Top-Level Fields
'''
The following operation updates the first document with name equal to "Juni",
using the $set operator to update the cuisine field and the $currentDate operator to
update the lastModified field with the current date.

upsert (boolean): True - if no matching documents found, then create a new one.
multi (boolean): True - if update all the matching records.
'''
result = collection.update({
    "name": "Juni"}, {
    '$set':         {"cuisine": "American (New) Vijay Anand"},
    '$currentDate': {"lastModified": True}
}, upsert=False, multi=False)


# Remove/Delete All Documents That Match a Condition
result = collection.remove({"cuisine": "American (New) Vijay Anand"})

# Remove/Delete one Document - Use the justOne Option
result = collection.remove({"borough": "Queens"}, {'$justOne': True})

# Total count of all value from collection
count = collection.find().count()
pprint.pprint('Total documents  - {}'.format(count))

# Total count value from collection
count = collection.find({"restaurant_id": "41704620"}).count()
pprint.pprint('Total documents found with {} - {}'.format({"restaurant_id": "41704620"}, count))

# Group Documents by a Field and Calculate Count
'''
Use the $group stage to group by a specified key. In the $group stage,
specify the group by key in the _id field. $group accesses fields by the
field path, which is the field name prefixed by a dollar sign $.
The $group stage can use accumulators to perform calculations for each group.
The following example groups the documents in the restaurants collection by the
borough field and uses the $sum accumulator to count the documents for each group.
'''
result = collection.aggregate([{'$group': {"_id": "$borough", "count": {'$sum': 1}}}])


# Filter and Group Documents
''' The _id field contains the distinct zipcode value, i.e., the group by key value. '''
result = collection.aggregate([
    {'$match': {"borough": "Brooklyn"}},
    {'$group': {"_id": "$address.zipcode", "count": {'$sum': 1}}}])


# $in operator for getting matching documents
borough = ['Missing', 'Manhattan']
result = collection.find({"borough": {'$in': borough}})


# get overall database, collection information
details = dict((db, [collection for collection in client[db].collection_names()])
               for db in client.database_names())
pprint.pprint(details)


import pandas as pd


def posts_2_df(iterator, chunk_size=1000):
    """
        Turn an iterator into multiple small pandas.DataFrame
        This is a balance between memory and efficiency
    """
    records = []
    frames = []
    for index, record in enumerate(iterator):
        records.append(record)
    if index % chunk_size == chunk_size - 1:
        frames.append(pd.DataFrame(records))
        records = []
    if records:
        frames.append(pd.DataFrame(records))
    return pd.concat(frames)

result = collection.find({'grades.date': {'$gte': start_date, '$lte': end_date}})
data_frame = posts_2_df(iterator=result, chunk_size=10000)
print(data_frame.head())
