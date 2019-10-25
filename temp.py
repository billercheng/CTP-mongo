import pymongo
import pandas as pd

def readMongo(name):
    cursor = db[name].find()
    df = pd.DataFrame(list(cursor))
    df.drop(['_id'], axis=1, inplace = True)
    return df



mon = pymongo.MongoClient("mongodb://localhost:27017/")
db = mon["runoobdb"]
mycol = db["sites"]

mydict = {"name": "RUNOOB", "alexa": "10000", "url": "https://www.runoob.com"}

x = mycol.insert_one(mydict)
df = readMongo('sites')














