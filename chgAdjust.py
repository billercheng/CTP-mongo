from parameter import *
import os
from WindPy import w

def checkChg(ee):
    s = lambda x: x in '0123456789'
    freq = 1
    con = dictCon[freq]
    for goodsCode in listGoods:
        eachFile = '{} position_max.csv'.format(goodsCode.upper())
        goodsName = dictGoodsName[goodsCode]
        #region 更新position_max表格
        dfPosition = pd.read_csv('position_max' + '\\' + eachFile, encoding='gbk',
                                  parse_dates=['trade_time']).set_index('trade_time')
        nextDate = tradeDay[tradeDay.index(dfPosition.index[-1]) + 1]
        nowDate = datetime.now()
        while nextDate.date() < nowDate.date() or (
                nextDate.date() == nowDate.date() and nowDate.time() > time(15, 30)):
            heyueData = w.wset('futurecc', "startdate={};enddate={};wind_code={}"
                               .format(nowDate.strftime('%Y-%m-%d'), nowDate.strftime('%Y-%m-%d'), goodsCode))
            listHeyue = heyueData.Data[2]
            dfMax = pd.DataFrame(columns=['maxHeyue', 'maxLogcation']).set_index('maxHeyue')
            maxHeYue = dfPosition['stock'].iat[-1].upper()
            boolTemp = False
            for tempHeyue in listHeyue:
                if tempHeyue == maxHeYue:
                    boolTemp = True
                # 只取这一天作出比较：
                if boolTemp:
                    getPosition = w.wsd(tempHeyue, 'oi', nextDate, nextDate)
                    if getPosition.ErrorCode == 0 and getPosition.Data[0][0] != None:
                        dfMax.loc[tempHeyue] = {'maxLogcation':getPosition.Data[0][0]}
            # 还需要判断是否存在相同持仓量的情况
            if dfMax[dfMax['maxLogcation'] == dfMax['maxLogcation'].max()].shape[0] > 1:
                index = dfMax[dfMax['maxLogcation'] == dfMax['maxLogcation'].max()].index[-1]
            else:
                index = dfMax['maxLogcation'].tolist().index(dfMax['maxLogcation'].max())
            dfPosition.loc[nextDate] = {'stock': dfMax.index[index],
                                         'position': dfMax['maxLogcation'].max()}
            nextDate = tradeDay[tradeDay.index(nextDate) + 1]
        dfPosition.to_csv('position_max' + '\\' + eachFile, encoding = "gbk")
        #endregion
        #region 更新chg_data表格
        dfChgData = pd.read_csv(r'.\chg_data' + '\\' + goodsCode.upper() + ' chg_data.csv', encoding='gbk',
                                  parse_dates=['adjdate'])
        dfChgData = dfChgData.drop(['id'], axis=1)
        chgExcData = dfChgData['adjdate'].iat[-2]
        dfPosition = dfPosition[dfPosition.index >= chgExcData]
        mainHeyue = dfPosition.stock[0]
        dfPosition = dfPosition.drop(['position'], axis=1)
        listDay = [None]
        listStock = [dfChgData['stock'].iat[-2]]
        for i in range(1, dfPosition.shape[0]):
            # if mainHeyue != dfPosition.stock[i] and int(''.join(list(filter(s, dfPosition.stock[i])))) > \
            #         int(''.join(list(filter(s, mainHeyue)))):
            if mainHeyue != dfPosition.stock[i]:
                listDay.append(dfPosition.index[i])
                listStock.append(dfPosition['stock'][i])
                mainHeyue = dfPosition['stock'][i]
        adjinterval = []
        for the_num in range(1, len(listDay)):
            real_index = listDay[the_num]
            # 使用日数据的结束时间：那肯定是没有错的话：
            firstExc = w.wsd(listStock[the_num], 'close', real_index, real_index)
            firstClose = pd.Series(firstExc.Data[0]).tolist()[0]
            secondExc = w.wsd(listStock[the_num - 1], 'close', real_index, real_index)
            secondClose = pd.Series(secondExc.Data[0]).tolist()[0]
            adjinterval.append(firstClose - secondClose)
        if len(adjinterval) > 0:
            theChgData = pd.DataFrame(
                {'goods_name': goodsName, 'goods_code': goodsCode, 'adjinterval': adjinterval, 'stock': listStock[1:],
                 'adjdate': listDay[1:]})
            theChgData = theChgData[['goods_code', 'goods_name', 'adjdate', 'adjinterval', 'stock']]
            dfChgData = pd.concat([dfChgData[:-1], theChgData])
            dfChgData.index = range(1, dfChgData.shape[0] + 1)
            dfChgData.index.name = 'id'
            dfChgData.loc[dfChgData.shape[0] + 1] = {'goods_code': goodsCode.upper(), 'goods_name': goodsName,
                                                         'adjdate': dfPosition.index[-1], 'adjinterval': 0,
                                                         'stock': listStock[-1]}
            dfChgData = dfChgData[['goods_code', 'goods_name', 'adjdate', 'adjinterval', 'stock']]
            dfChgData.to_csv(r'.\chg_data' + '\\' + goodsCode.upper() + ' chg_data.csv', encoding = "gbk")
        else:
            dfChgData['adjdate'].iat[-1] = dfPosition.index[-1]
            dfChgData.index = range(1, dfChgData.shape[0] + 1)
            dfChgData.index.name = 'id'
            dfChgData = dfChgData[['goods_code', 'goods_name', 'adjdate', 'adjinterval', 'stock']]
            dfChgData.to_csv(r'.\chg_data' + '\\' + goodsCode.upper() + ' chg_data.csv', encoding = "gbk")
        #endregion
        dfChgData = dfChgData.reset_index(drop=True)
        if goodsCode.split('.')[1] in ['CZC', 'CFE']:
            dfChgData['goods_code'] = dfChgData['stock']
        else:
            dfChgData['goods_code'] = dfChgData['stock'].apply(
                lambda x: x.split('.')[0].lower() + '.' + x.split('.')[1])
        dfChgData = dfChgData[['goods_code', 'goods_name', 'adjdate', 'adjinterval']]
        dfChgData = dfChgData[1:-1]
        dictGoodsAdj[goodsCode] = dfChgData.set_index('goods_code')  # 记录 dictGoodsAdj
        # dictGoodsAdj 这个时间是需要加上16小时，方便进行比较的
        dictGoodsAdj[goodsCode]['adjdate'] = pd.to_datetime(dictGoodsAdj[goodsCode]['adjdate']) + timedelta(
            hours=17)
        dictGoodsInstrument[goodsCode] = dictGoodsAdj[goodsCode].index[-1]
        dictGoodsChg[goodsCode.split('.')[0]] = goodsCode.split('.')[1]
        # 将dfChgData 插入到数据库上：
        dfAdj = pd.read_sql('select adjdate from {}_调整时刻表'.format(dictGoodsName[
                                                                      goodsCode]) + ' order by adjdate desc limit 1',con)
        if dfAdj.shape[0] == 0:
            dfChgData = dfChgData.copy()
        else:
            dfChgData = dfChgData[dfChgData['adjdate'] > dfAdj['adjdate'][0]]
        if dfChgData.shape[0] > 0:
            for eachIndex in dfChgData.index:
                putLogEvent(ee, '品种 {} 的最新合约号为：{}'.format(dfChgData['goods_name'][eachIndex], dfChgData['goods_code'][eachIndex]))
            dfChgData.to_sql(dictGoodsName[goodsCode] + '_调整时刻表', con, if_exists='append', index=False,
                      schema='cta{}_trade'.format(freq))