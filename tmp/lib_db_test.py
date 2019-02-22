from lib.db import MongoDB


app_name = "foody"
collection_name = "restaurant"

mongo = MongoDB(database_name=app_name, collection_name=collection_name)

name = 'amer'
output = []
print(name)
for s in mongo.find({"name": {"$regex": "{}".format(name), "$options": "$i"}}):
    output.append({'name': s['name'], 'cuisine': s['cuisine']})

print(output)
import random

mongo_user = MongoDB(database_name=app_name, collection_name='user')
# fist name, last name, email id, password_hash
# import hashlib
# password = 'Ilovemyjob'
# password_hash = hashlib.sha3_384(password.encode('utf-8')).hexdigest()
# print(password_hash)
# update_query = {'First Name': 'Vijay', 'Last Name': 'Anand',
#                 'Email': 'vijayanandrp@gmail.com', 'password_hash':password_hash}
# mongo_user._collection.insert_one(update_query)

# for s in mongo.find():
#     restaurant_id = s['restaurant_id']
#     dish_type = random.choice(['veg', 'non-veg'])
#     mongo._collection.update_one({"restaurant_id": restaurant_id}, {"$set": {"dish_type": dish_type}})
#
#


# restaurant_id = "40356018"
# from datetime import datetime
# date_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
# mongo._collection.update_one({"restaurant_id": restaurant_id},
#                              {"$push": {"grades": {"date": date_stamp,
#                                                    "grade": "AZ",
#                                                    "score": 200}}})
