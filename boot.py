from PyQt5.QtWidgets import QApplication
from qtpy.QtCore import QTimer
import sys
from parameter import *
from mdApi import *
import threading

class RdMdUi():

    # 程序初始化操作
    def __init__(self):
        self.q = Quote()
        self.getInit()  # 更新切换合约表和数据更新
        self.getEngine()  # 建立事件注册引擎

    # region 主引擎事件
    def getEngine(self):
        ee = EventEngine()
        downLogProgram("执行程序")
        # ee.register(EVENT_MARKETDATA_CONTRACT, self.dealData)  # tick数据处理方式
        # ee.register(EVENT_SUBSCRIBE, self.subscribeData)  # 订阅主力合约
        ee.start(timer=False)
    # endregion

    # region 自动更新 chg_data 与 position_data
    def getInit(self):
        threading.Thread(target=self.getData, daemon=True).start()

    def getData(self):
        downLogProgram("正在从 mongodb 上读取 CTA 数据到内存")
        for freq in listFreqPlus:
            dictData[freq] = {}
            con = dictFreqCon[freq]
            downLogProgram("将 CTA{} 数据写入内存上".format(freq))
            for goodsName in dictGoodsName.values():
                if freq == 1:
                    dictData[freq][goodsName + '_调整表'] = readMongoNum(con, '{}_调整表'.format(goodsName), readNum).set_index('trade_time').sort_index()
                    dictData[freq][goodsName + '_调整表'] = dictData[freq][goodsName + '_调整表'][listMin]
                    print(dictData[freq][goodsName + '_调整表'])
                else:
                    dictData[freq][goodsName + '_调整表'] = readMongoNum(con, '{}_调整表'.format(goodsName), readNum).set_index('trade_time').sort_index()
                    dictData[freq][goodsName + '_均值表'] = readMongoNum(con, '{}_均值表'.format(goodsName), readNum).set_index('trade_time').sort_index()
                    dictData[freq][goodsName + '_重叠度表'] = readMongoNum(con, '{}_重叠度表'.format(goodsName), readNum).set_index('trade_time').sort_index()
                    dictData[freq][goodsName + '_调整表'] = dictData[freq][goodsName + '_调整表'][listMin]
                    dictData[freq][goodsName + '_均值表'] = dictData[freq][goodsName + '_均值表'][listMa]
                    dictData[freq][goodsName + '_重叠度表'] = dictData[freq][goodsName + '_重叠度表'][listOverLap]
                    print(dictData[freq][goodsName + '_调整表'])
                    print(dictData[freq][goodsName + '_均值表'])
                    print(dictData[freq][goodsName + '_重叠度表'])
        downLogProgram('将数据库数据写入内存上操作完成')
    # endregion

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ui = RdMdUi()
    sys.exit(app.exec_())