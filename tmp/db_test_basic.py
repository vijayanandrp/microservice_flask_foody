import pprint
import random
import string
import datetime
from lib.db import MongoDB


def random_data(max_len=3):
	return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(max_len))


def get_dummy_data():
	post = {"author": random_data(8),
			"text": ' '.join(random_data(10) for _ in range(10)),
			"tags": ["mongodb", "python", "pymongo"],
			"date": datetime.datetime.utcnow()}
	return post


def status(obj, msg=''):
	print()
	print('-'*60, msg.upper(), '-'*60)
	if obj:
		pprint.pprint(obj)
	
	
# req - create connection only
m0 = MongoDB()
status(m0.get_current_status(), 'only connection')
# req - create database with validation
m0.create_db('try000')
status(m0.get_current_status(), 'only database')
# 2
m = MongoDB(database_name='try001')
status(m.get_current_status(), 'only database - part 2')

# req - drop db by name
mm = MongoDB(database_name='DELETE_ME')
mm.create_collection(collection_name='Temporary')
mm.insert(get_dummy_data())
status(mm.get_database_names(), msg='create and delete db')
print('collection created - ', mm.get_collection_names())
mm.drop_db(database_name='DELETE_ME')
print('DB names after dropping - ', mm.get_database_names())
# req - trying to recreate DB with old details RAISE EXCEPTION
# mm.insert(get_dummy_data())
# print('DB names after dropping - ', mm.get_database_names())

# req - get db names
y = MongoDB()
status(y.get_database_names(), 'database names')

# req - get collection names
y = MongoDB(database_name='try001')
y.create_collection(collection_name='collection001')
y.insert(get_dummy_data())
y.insert(get_dummy_data())
y.insert(get_dummy_data())
y.create_collection(collection_name='collection002')
y.insert(get_dummy_data())
y.insert(get_dummy_data())
y.insert(get_dummy_data())
y.create_collection(collection_name='collection003')
y.insert(get_dummy_data())
y.insert(get_dummy_data())
y.insert(get_dummy_data())
y.insert(get_dummy_data())
y.insert(get_dummy_data())
y.insert(get_dummy_data())
status(y.get_collection_names(), 'get collection names')

# req - get overall details
y = MongoDB()
status(y.get_overall_details(), msg='get overall details')


# req - create collection with validation
m0.create_collection(collection_name='collection000')
status(m0.get_current_status(), 'creating collection')
# 2
m = MongoDB(database_name='try002', collection_name='collection002')
status(m.get_current_status(), 'creating collection - part 2')
# req - insert a value
m.insert(get_dummy_data())
status(m.insert(get_dummy_data()), msg=' Insert one')
# req - insert many
status(m.insert_many([get_dummy_data() for _ in range(10)]), msg=' Insert Many ')

# req - find/query
status(m.find_one({'author': 'sR4aSZ5q'}), msg=' find one')
status(m.find(), msg=' find many')
# req - count
status(m.find(count=True), msg='count')
print('Count again - ', m.count())
# req - drop collection by name
status(m.drop_collection(), msg='drop collection')
# m.find()      # Raise Exception Error

# req - update

# req - insert/bulk write

# req - create index

# req - create indexs
