from py_ctp.eventEngine import *
from py_ctp.eventType import *
from py_ctp.ctp_quote import Quote
import pandas as pd
from datetime import *
import logging
from PyQt5.QtCore import QCoreApplication
import sys
from parameter import *

class MdApi:
    def __init__(self):
        # 登陆的账户与密码
        self.userid = ''
        self.password = ''
        self.brokerid = '3003'
        self.address = 'tcp://114.80.54.237:41213'
        # 创建Quote对象
        self.q = Quote()
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

    def onFrontConnected(self):
        """服务器连接"""
        self.q.ReqUserLogin(BrokerID=self.brokerid, UserID=self.userid, Password=self.password)

    def onFrontDisconnected(self, n):
        """服务器断开"""
        putLogEvent('行情服务器连接断开')

    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        if error.getErrorID() == 0:
            putLogEvent('行情服务器登陆成功，订阅主力合约')
            for instrument in listInstrument:
                self.q.SubscribeMarketData(instrument)
        else:
            log = '行情服务器登陆回报，错误代码：' + str(error.getErrorID()) + \
                  ',   错误信息：' + str(error.getErrorMsg())
            putLogEvent(log)

    def onRspUserLogout(self, data, error, n, last):
        if error.getErrorID() == 0:
            log = '行情服务器登出成功'
        else:
            log = '行情服务器登出回报，错误代码：' + str(error.getErrorID()) + \
                  ',   错误信息：' + str(error.getErrorMsg())
        putLogEvent(log)

    def onRspError(self, error, n, last):
        """错误回报"""
        log = '行情错误回报，错误代码：' + str(error.getErrorID()) \
              + '错误信息：' + + str(error.getErrorMsg())
        putLogEvent(log)

    def onRspSubMarketData(self, data, info, n, last):
        pass

    def onRtnDepthMarketData(self, data):
        """行情推送"""
        event = Event(type_=EVENT_MARKETDATA_CONTRACT)
        event.dict_['InstrumentID'] = data.getInstrumentID()
        event.dict_['LastPrice'] = data.getLastPrice()
        event.dict_['TradingDay'] = data.getTradingDay()
        event.dict_['UpdateTime'] = data.getUpdateTime()
        event.dict_['UpdateMillisec'] = data.getUpdateMillisec()
        ee.put(event)
