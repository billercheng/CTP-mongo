import pandas as pd
from datetime import *
from sqlalchemy import *
import pymongo
import numpy as np

if __name__ == '__main__':
    # 选择的频段
    listFreq = [24]
    # 公共参数
    dfGoodsTab = pd.read_excel("RD files\\公共参数.xlsx", sheet_name="品种信息")
    listGoodsCode = dfGoodsTab['品种代码'].tolist()  # 品种代码数列
    listGoodsName = dfGoodsTab['品种名称'].tolist()  # 品种名称数列
    # 选择开始时间，删除的数据为 ： >= startTime
    startTime = datetime(2019, 10, 23, 8)
    
    dictFreqDatabase = {}
    mon = pymongo.MongoClient("mongodb://localhost:27017/")  # mongodb 数据库
    dictMon = {}
    
    for freq in listFreq:
        # 品种频段对应本地数据库
        dictMon[freq] = mon['cta{}_trade'.format(freq)]
        # 频段名称
        if freq == 1:
            con = dictMon[1]
            for goodsName in listGoodsName:
                listTables = ['{}_调整表'.format(goodsName)]
                for table in listTables:
                    # 删除数据操作
                    table = con[table]
                    table.delete_many({'trade_time': {'$gte': startTime}})
        else:
            con = dictMon[freq]
            for goodsName in listGoodsName:
                listTables = [goodsName + '_调整表',
                              goodsName + '_均值表',
                              goodsName + '_重叠度表']
                for table in listTables:
                    table = con[table]
                    table.delete_many({'trade_time': {'$gte': startTime}})