from PyQt5.QtWidgets import QApplication
from py_ctp.ctp_struct import *
from py_ctp.ctp_quote import *
from py_ctp.eventEngine import *
from py_ctp.eventType import *

class MdApi:
    def __init__(self, ee, q):
        self.ee = ee
        self.list_account = ['申万实盘', '中证实盘', '国泰君安实盘', '广发实盘', '中国国际实盘', "广金实盘", '']
        self.list_server_brokerid = ["88888", "66666", "7090", "9000", "8090", ""]
        self.list_server_investorid = ["8701000683", "830300035", "28900528", "886810370", "33305188", ""]
        self.list_server_password = ["600467", "600467", "600467", "600467", "600467", ""]
        self.list_server_address = ["tcp://180.168.212.51:41213",
                                    "tcp://ctp1-md7.citicsf.com:41213",
                                    "tcp://180.169.75.21:41213",
                                    "tcp://116.228.246.81:41213",
                                    "tcp://180.168.102.193:41213",
                                    "tcp://114.80.54.236:41213"]
        self.choice = 5
        self.userid = '096114'
        self.password = 'cheng1234567'
        self.brokerid = '9999'
        self.address = 'tcp://180.168.146.187:10110'
        # 创建Quote对象
        self.q = q
        api = self.q.CreateApi()
        spi = self.q.CreateSpi()
        self.q.RegisterSpi(spi)
        self.q.OnFrontConnected = self.onFrontConnected  # 交易服务器登陆相应
        self.q.OnFrontDisconnected = self.onFrontDisconnected
        self.q.OnRspUserLogin = self.onRspUserLogin  # 用户登陆
        self.q.OnRspUserLogout = self.onRspUserLogout  # 用户登出
        self.q.OnRspError = self.onRspError
        self.q.OnRspSubMarketData = self.onRspSubMarketData
        self.q.OnRtnDepthMarketData = self.onRtnDepthMarketData
        self.q.RegCB()
        self.q.RegisterFront(self.address)
        self.q.Init()
        self.islogin = False  # 判断是否登陆成功

    def onFrontConnected(self):
        """服务器连接"""
        putLogEvent(self.ee, '行情服务器连接成功')
        print('行情服务器连接成功')
        self.q.ReqUserLogin(BrokerID=self.brokerid, UserID=self.userid, Password=self.password)

    def onFrontDisconnected(self, n):
        """服务器断开"""
        putLogEvent(self.ee, '行情服务器连接断开')

    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        if error.getErrorID() == 0:
            log = '行情服务器登陆成功'
            self.islogin = True
            print('行情服务器登陆成功')
            event = Event(type_=EVENT_SUBSCRIBE)
            self.ee.put(event)
        else:
            log = '行情服务器登陆回报，错误代码：' + str(error.getErrorID()) + \
                  ',   错误信息：' + str(error.getErrorMsg())
        putLogEvent(self.ee, log)

    def onRspUserLogout(self, data, error, n, last):
        if error.getErrorID() == 0:
            log = '行情服务器登出成功'
            self.islogin = False
        else:
            log = '行情服务器登出回报，错误代码：' + str(error.getErrorID()) + \
                  ',   错误信息：' + str(error.getErrorMsg())
        putLogEvent(self.ee, log)

    def onRspError(self, error, n, last):
        """错误回报"""
        log = '行情错误回报，错误代码：' + str(error.getErrorID()) \
              + '错误信息：' + + str(error.getErrorMsg())
        putLogEvent(self.ee, log)

    def onRspSubMarketData(self, data, info, n, last):
        pass

    def onRtnDepthMarketData(self, data):
        """行情推送"""
        event = Event(type_=EVENT_MARKETDATA_CONTRACT)
        event.dict_['InstrumentID'] = data.getInstrumentID()
        event.dict_['TradingDay'] = data.getTradingDay()
        event.dict_['UpdateTime'] = data.getUpdateTime()
        event.dict_['UpdateMillisec'] = data.getUpdateMillisec()
        event.dict_['LastPrice'] = data.getLastPrice()
        event.dict_['Volume'] = data.getVolume()
        event.dict_['Turnover'] = data.getTurnover()
        event.dict_['OpenInterest'] = data.getOpenInterest()
        self.ee.put(event)

if __name__ == '__main__':
    q = Quote()
    ee = EventEngine()
    md = MdApi(ee, q)
    app = QApplication(sys.argv)
    sys.exit(app.exec_())