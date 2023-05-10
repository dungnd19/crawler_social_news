from pymongo import MongoClient
from pymongo.collection import Collection

from src import settings

mongo_client = MongoClient(settings.MONGODB_URL)
db = mongo_client[settings.MONGODB_DBNAME]

db_article: Collection = db['article_v2']
db_source: Collection = db['source_v2']
db_user: Collection = db['user']
