from src.common.mdb import db_article, db_source

if __name__ == '__main__':
    existed_source_ids = set(db_article.distinct('source_id'))
    source_ids = set(db_source.find({'sourceType': 1}).distinct('_id'))

    sources = db_source.find({'_id': {'$in': list(source_ids - existed_source_ids)}})
    for source in sources:
        print(source)
