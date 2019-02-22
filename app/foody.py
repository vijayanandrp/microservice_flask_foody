# mongo.py

from flask import Flask
from flask import jsonify
from flask import request
from lib.db import MongoDB
from flask import Response
from datetime import datetime
import http.client, urllib.parse
import json


app = Flask(__name__)

app_name = "foody"
collection_name = "restaurant"
app.config['MONGO_DBNAME'] = app_name
app.config['MONGO_URI'] = 'mongodb://localhost:27017/{}'.format(app_name)

mongo = MongoDB(database_name=app_name, collection_name=collection_name)
mongo_user = MongoDB(database_name=app_name, collection_name='user')


# Fetch all restaurant
@app.route('/all', methods=['GET'])
def get_all_restaurant():
    conn = http.client.HTTPConnection("127.0.0.1", 5002)
    conn.request("GET", "/all")
    r1 = conn.getresponse()
    print(r1.status, r1.reason)
    data1 = r1.read()
    return Response(data1, status=201, mimetype='application/json')


# Search Restaurant
@app.route('/all', methods=['POST'])
def find_restaurant():
    conn = http.client.HTTPConnection("127.0.0.1", 5002)
    params = urllib.parse.urlencode(request.json)
    print(params, request.json)
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn.request("POST", "/all", body=request.json, headers=headers)
    response = conn.getresponse()
    print(params, response.status, response.reason)
    data = response.read()
    return Response(data, status=201, mimetype='application/json')


#
#
# # dish_type group by veg / non-veg
# @app.route('/dish', methods=['POST'])
# def group_dish():
#     dish_type = request.json['dish_type']
#     if dish_type not in ['veg', 'non-veg']:
#         return Response("[]", status=201, mimetype='application/json')
#     output = []
#     for s in mongo.find({"dish_type": dish_type}):
#         output.append({'name': s['name'], 'cuisine': s['cuisine']})
#     return jsonify({'result': output})
#
#
# # Ratings (Anonymous)
# @app.route('/rating', methods=['POST'])
# def post_rating():
#     output = []
#     restaurant_id = request.json['restaurant_id']
#     grade = request.json['grade']
#     score = request.json['score']
#     date_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
#     mongo._collection.update_one({"restaurant_id": restaurant_id},
#                                  {"$push": {"grades": {"date": date_stamp,
#                                                        "grade": grade, "score": score}}})
#     return jsonify({'result': output})
#
#
# # create user
# @app.route('/user', methods=['POST'])
# def user_creation():
#     output = []
#     first_name = request.json['first_name']
#     last_name = request.json['last_name']
#     email = request.json['email']
#     password = request.json['password']
#     password_hash = hashlib.sha3_384(password.encode('utf-8')).hexdigest()
#     print(password_hash)
#     update_query = {'First Name': first_name, 'Last Name': last_name,
#                     'Email': email, 'password_hash': password_hash}
#     mongo_user._collection.insert_one(update_query)
#     return jsonify({'result': output})
#

if __name__ == '__main__':
    app.run(debug=True)
