from PyQt5.QtWidgets import QApplication
from qtpy.QtCore import QTimer
import sys
from parameter import *
from mdApi import *
import threading
from WindPy import w

class RdMdUi():

    # 程序初始化操作
    def __init__(self):
        self.getEngine()  # 建立事件注册引擎
        self.getInit()  # 更新切换合约表和数据更新

    # region 主引擎事件
    def getEngine(self):
        ee = EventEngine()
        downLogProgram("执行程序")
        ee.register(EVENT_MARKETDATA_CONTRACT, self.dealData)  # tick数据处理方式
        ee.register(EVENT_SUBSCRIBE, self.subscribeData)  # 订阅主力合约
        ee.start(timer=False)
    # endregion

    # region 自动更新 chg_data 与 position_data
    def getInit(self):
        threading.Thread(target=self.getWind, daemon=True).start()

    def getWind(self):
        self.q = Quote()
        w.start()
        downLogProgram("正在检查chg_data与position_max是否为最新的数据")
        checkChg(self.ee)
        for eachFreq in listFreqPlus:
            dictData[eachFreq] = {}
            con = dictCon[eachFreq]
            putLogEvent(self.ee, "将 CTA{} 数据库数据写入内存上".format(eachFreq))
            for eachGoodsName in dictGoodsName.values():
                if eachFreq == 1:
                    dictData[eachFreq][eachGoodsName + '_调整表'] = pd.read_sql(
                        "select * from {}_调整表 order by trade_time desc limit {}".format(eachGoodsName, lastDataNum * 15),
                        con).set_index(
                        'trade_time').sort_index()
                    dictData[eachFreq][eachGoodsName + '_调整表'] = dictData[eachFreq][eachGoodsName + '_调整表'].drop(['id'],
                                                                                                                 axis=1)
                else:
                    dictData[eachFreq][eachGoodsName + '_调整表'] = pd.read_sql(
                        "select * from {}_调整表 order by trade_time desc limit {}".format(eachGoodsName, lastDataNum * 2), con).set_index(
                        'trade_time').sort_index()
                    dictData[eachFreq][eachGoodsName + '_调整表'] = dictData[eachFreq][eachGoodsName + '_调整表'].drop(['id'],
                                                                                                                 axis=1)
                    dictData[eachFreq][eachGoodsName + '_均值表'] = pd.read_sql(
                        "select * from {}_均值表 order by trade_time desc limit {}".format(eachGoodsName, lastDataNum), con).set_index(
                        'trade_time').sort_index()
                    dictData[eachFreq][eachGoodsName + '_均值表'] = dictData[eachFreq][eachGoodsName + '_均值表'].drop(['id'],
                                                                                                                 axis=1)
                    dictData[eachFreq][eachGoodsName + '_重叠度表'] = pd.read_sql(
                        "select * from {}_重叠度表 order by trade_time desc limit {}".format(eachGoodsName, lastDataNum), con).set_index(
                        'trade_time').sort_index()
                    listDrop = ['id']
                    for eachMvl in mvlenvector:
                        listDrop.extend(
                            ['StdMux高均值_{}'.format(eachMvl), 'StdMux低均值_{}'.format(eachMvl), 'StdMux收均值_{}'.format(eachMvl),
                             '重叠度高收益_{}'.format(eachMvl), '重叠度低收益_{}'.format(eachMvl), '重叠度收收益_{}'.format(eachMvl)])
                    dictData[eachFreq][eachGoodsName + '_重叠度表'] = dictData[eachFreq][eachGoodsName + '_重叠度表'].drop(listDrop, axis=1)
        putLogEvent(self.ee, "将数据库数据写入内存上操作完成")
        openBtn(self.ee)
    # endregion

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ui = RdMdUi()
    ui.show()
    sys.exit(app.exec_())