# mongo.py

from flask import Flask
from flask import jsonify
from flask import request
from lib.db import MongoDB
from flask import Response
from datetime import datetime
import hashlib

app = Flask(__name__)

app_name = "foody_user"
collection_name = "user"
app.config['MONGO_DBNAME'] = app_name
app.config['MONGO_URI'] = 'mongodb://localhost:27017/{}'.format(app_name)

mongo_user = MongoDB(database_name=app_name, collection_name=collection_name)


# Fetch all restaurant
# create user
@app.route('/user', methods=['POST'])
def user_creation():
    output = []
    first_name = request.json['first_name']
    last_name = request.json['last_name']
    email = request.json['email']
    password = request.json['password']
    password_hash = hashlib.sha3_384(password.encode('utf-8')).hexdigest()
    print(password_hash)
    update_query = {'First Name': first_name, 'Last Name': last_name,
                    'Email': email, 'password_hash': password_hash}
    mongo_user._collection.insert_one(update_query)
    return jsonify({'result': output})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
