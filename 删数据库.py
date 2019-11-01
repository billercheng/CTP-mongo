import pymongo

if __name__ == '__main__':
    # 数据库列
    listDatabase = ['cta12_trade', 'cta13_trade', 'cta14_trade', 'cta15_trade', 'cta16_trade', 'cta17_trade', 'cta18_trade', 'cta19_trade']
    # mongodb 软件的连接
    mon = pymongo.MongoClient("mongodb://localhost:27017/")  # mongodb 数据库
    # 循环数据库
    for database in listDatabase:
        dbCon = mon[database]
        docs = dbCon.list_collection_names()
        for doc in docs:
            temp = dbCon[doc]
            temp.drop()