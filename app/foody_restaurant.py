# mongo.py

from flask import Flask
from flask import jsonify
from flask import request
from lib.db import MongoDB
from flask import Response
from datetime import datetime
import hashlib

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
    output = []
    for s in mongo.find():
        output.append({'name': s['name'], 'cuisine': s['cuisine']})
    return jsonify({'result': output})


# Search Restaurant
@app.route('/all', methods=['POST'])
def find_restaurant():
    name = request.json['name']
    print(name)
    if not name:
        return Response("[]", status=201, mimetype='application/json')
    output = []
    for s in mongo.find({"name": {"$regex": "{}".format(name), "$options": "$i"}}):
        address = ' '.join([s['address']['building'], s['address']['street'], s['borough'], s['address']['zipcode']])
        output.append({'name': s['name'], 'cuisine': s['cuisine'], 'address': address})
    return jsonify({'result': output})


# dish_type group by veg / non-veg
@app.route('/dish', methods=['POST'])
def group_dish():
    dish_type = request.json['dish_type']
    if dish_type not in ['veg', 'non-veg']:
        return Response("[]", status=201, mimetype='application/json')
    output = []
    for s in mongo.find({"dish_type": dish_type}):
        output.append({'name': s['name'], 'cuisine': s['cuisine']})
    return jsonify({'result': output})


# Ratings (Anonymous)
@app.route('/rating', methods=['POST'])
def post_rating():
    output = []
    restaurant_id = request.json['restaurant_id']
    grade = request.json['grade']
    score = request.json['score']
    date_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    mongo._collection.update_one({"restaurant_id": restaurant_id},
                                 {"$push": {"grades": {"date": date_stamp,
                                                       "grade": grade, "score": score}}})
    return jsonify({'result': output})



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)

