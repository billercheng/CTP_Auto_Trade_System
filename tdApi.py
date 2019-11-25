from py_ctp.ctp_trade import Trade
from parameter import *
from PyQt5.QtWidgets import QApplication
import sys

class TdApi:
    def __init__(self, userid, password, brokerid, RegisterFront, product_info, app_id, auth_code):
        # 初始化账号
        self.t = Trade()
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.product_info = product_info
        self.app_id = app_id
        self.auth_code = auth_code
        api = self.t.CreateApi()
        spi = self.t.CreateSpi()
        self.t.RegisterSpi(spi)
        self.t.OnFrontConnected = self.onFrontConnected  # 交易服务器登陆相应
        self.t.OnFrontDisconnected = self.onFrontDisconnected
        self.t.OnRspAuthenticate = self.onRspAuthenticate  # 申请码检验
        self.t.OnRspUserLogin = self.onRspUserLogin  # 用户登陆
        self.t.OnRspUserLogout = self.onRspUserLogout  # 用户登出
        self.t.OnRtnInstrumentStatus = self.onRtnInstrumentStatus
        self.t.OnRspQryInstrument = self.onRspQryInstrument  # 查询全部交易合约
        self.t.OnRspSettlementInfoConfirm = self.onRspSettlementInfoConfirm  # 结算单确认，显示登陆日期
        self.t.OnRspQryTradingAccount = self.onRspQryTradingAccount  # 查询账户
        self.t.OnRtnOrder = self.onRtnOrder  # 报单
        self.t.OnRtnTrade = self.onRtnTrade  # 成交
        # self.t.OnRspParkedOrderInsert = self.onRspParkedOrderInsert
        self.t.OnErrRtnOrderInsert = self.onErrRtnOrderInsert
        self.t.OnRspQryDepthMarketData = self.onRspQryDepthMarketData  # 查询涨跌停
        self.t.RegCB()
        self.t.RegisterFront(RegisterFront)
        self.t.Init()
        self.islogin = False

    def onFrontConnected(self):
        """服务器连接"""
        downLogProgram('交易服务器连接成功')
        self.t.ReqAuthenticate(self.brokerid, self.userid, self.product_info, self.auth_code, self.app_id)

    def onFrontDisconnected(self, n):
        downLogProgram('交易服务器连接断开')

    def onRspAuthenticate(self, pRspAuthenticateField: CThostFtdcRspAuthenticateField, pRspInfo: CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        # downLogProgram('auth：{0}:{1}'.format(pRspInfo.getErrorID(), pRspInfo.getErrorMsg()))
        self.t.ReqUserLogin(BrokerID=self.brokerid, UserID=self.userid, Password=self.password, UserProductInfo=self.product_info)

    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        if error.getErrorID() == 0:
            self.Investor = data.getUserID()
            self.BrokerID = data.getBrokerID()
            log = self.Investor + '交易服务器登陆成功'
            self.islogin = True
            self.t.ReqSettlementInfoConfirm(self.BrokerID, self.Investor)  # 对账单确认
            self.t.ReqQryDepthMarketData()
        else:
            log = '交易服务器登陆回报，错误代码：' + str(error.getErrorID()) + \
                  ',   错误信息：' + str(error.getErrorMsg())
        downLogProgram(log)

    def onRspUserLogout(self, data, error, n, last):
        if error.getErrorID() == 0:
            log = '交易服务器登出成功'
            self.islogin = False
        else:
            log = '交易服务器登出回报，错误代码：' + str(error.getErrorID()) + \
                  ',   错误信息：' + str(error.getErrorMsg())
        downLogProgram(log)

    def onRtnInstrumentStatus(self, data):
        pass

    def onRspQryInstrument(self, data, error, n, last):
        pass

    def onRspSettlementInfoConfirm(self, data, error, n, last):
        """确认结算信息回报"""
        downLogProgram('账号：{}, 日期：{}, 时间：{}'.format(data.getInvestorID(), data.getConfirmDate(), data.getConfirmTime()))

    def getPosition(self):
        self.checkPosition = False
        downLogProgram("读取账号持仓情况")
        self.t.ReqQryInvestorPosition(self.brokerid, self.userid)

    def onRspQryTradingAccount(self, data, error, n, last):
        """资金账户查询回报"""
        if error.getErrorID() == 0:
            event = Event(type_=EVENT_ACCOUNT)
            event.dict_['BrokerID'] = data.getBrokerID()
            event.dict_['AccountID'] = data.getAccountID()
            event.dict_['PreDeposit'] = data.getPreDeposit()
            event.dict_['PreBalance'] = data.getPreBalance()
            event.dict_['PreMargin'] = data.getPreMargin()
            event.dict_['CurrMargin'] = data.getCurrMargin()
            event.dict_['Available'] = data.getAvailable()
            event.dict_['WithdrawQuota'] = data.getWithdrawQuota()
            ee.put(event)
        else:
            log = ('账户查询回报，错误代码：' + str(error.getErrorID()) + ',   错误信息：' + str(error.getErrorMsg()))
            downLogProgram(log)

    def getAccount(self):
        self.t.ReqQryTradingAccount(self.brokerid, self.userid)

    def onRtnOrder(self, data):
        # 常规报单事件
        if time(7) <= datetime.now().time() <= time(8, 15):  # # 判断 连接 断开 与 连接 重连 的问题
            return
        event = Event(type_=EVENT_ORDER)
        event.dict_ = data.__dict__.copy()
        ee.put(event)

    def onRtnTrade(self, data):
        """成交回报"""
        if time(7) <= datetime.now().time() <= time(8, 15):
            return
        event = Event(type_=EVENT_TRADE)
        event.dict_ = data.__dict__.copy()
        ee.put(event)

    # 预下单，没有用了
    def onRspParkedOrderInsert(self, data=CThostFtdcParkedOrderField, pRspInfo=CThostFtdcRspInfoField,
                               nRequestID=int, bIsLast=bool):
        event = Event(type_=EVENT_ORDERPARK)
        event.dict_['data'] = data._fields_
        ee.put(event)

    def onErrRtnOrderInsert(self, data, error):
        """发单错误回报（交易所）"""
        if time(7) <= datetime.now().time() <= time(8, 15):
            return
        event = Event(type_=EVENT_ORDER_ERROR)
        event.dict_['OrderRef'] = data.getOrderRef()
        event.dict_['InstrumentID'] = data.getInstrumentID()
        event.dict_['Direction'] = data.getDirection()
        event.dict_['CombOffsetFlag'] = data.getCombOffsetFlag()
        event.dict_['VolumeTotalOriginal'] = data.getVolumeTotalOriginal()
        event.dict_['LimitPrice'] = data.getLimitPrice()
        event.dict_['ErrorMsg'] = error.getErrorMsg()
        ee.put(event)

    # region 下单操作
    def sendorder(self, instrumentid, orderref, price, vol, direction, offset,
                  OrderPriceType=TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice):
        goodsCode = getGoodsCode(instrumentid)
        if goodsCode.split('.')[1] == 'SHF':
            exChangeID = 'SHFE'
        elif goodsCode.split('.')[1] == 'DCE':
            exChangeID = 'DCE'
        elif goodsCode.split('.')[1] == 'CZC':
            exChangeID = 'CZCE'
        elif goodsCode.split('.')[1] == 'CFE':
            exChangeID = 'CFFEX'
        elif goodsCode.split('.')[1] == 'INE':
            exChangeID = 'INE'
        self.t.ReqOrderInsert(BrokerID=self.brokerid,
                              InvestorID=self.userid,
                              InstrumentID=instrumentid,
                              OrderRef=orderref,
                              UserID=self.userid,
                              OrderPriceType=OrderPriceType,
                              Direction=direction,
                              CombOffsetFlag=offset,
                              CombHedgeFlag=chr(TThostFtdcHedgeFlagType.THOST_FTDC_HF_Speculation.value),
                              LimitPrice=price,
                              VolumeTotalOriginal=vol,
                              TimeCondition=TThostFtdcTimeConditionType.THOST_FTDC_TC_GFD,
                              VolumeCondition=TThostFtdcVolumeConditionType.THOST_FTDC_VC_AV,
                              MinVolume=1,
                              ForceCloseReason=TThostFtdcForceCloseReasonType.THOST_FTDC_FCC_NotForceClose,
                              ContingentCondition=TThostFtdcContingentConditionType.THOST_FTDC_CC_Immediately,
                              ExchangeID=exChangeID
                              )
        return orderref

    def buy(self, symbol, orderref, price, vol):  # 买开
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value)
        self.sendorder(symbol, orderref, price, vol, direction, offset)

    def sell(self, symbol, orderref, price, vol):  # 买平
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        self.sendorder(symbol, orderref, price, vol, direction, offset)

    def sellMarket(self, symbol, orderref, vol):  # 买平市
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        self.sendorder(symbol, orderref, 0, vol, direction, offset, TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice)

    def sellToday(self, symbol, orderref, price, vol):  # 买平今
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_CloseToday.value)
        self.sendorder(symbol, orderref, price, vol, direction, offset)

    def sellMarketToday(self, symbol, orderref, vol):  # 买平市
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_CloseToday.value)
        self.sendorder(symbol, orderref, 0, vol, direction, offset, TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice)

    def short(self, symbol, orderref, price, vol):  # 卖开
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value)
        self.sendorder(symbol, orderref, price, vol, direction, offset)

    def cover(self, symbol, orderref, price, vol):  # 卖平
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        self.sendorder(symbol, orderref, price, vol, direction, offset)

    def coverMarket(self, symbol, orderref, vol):  # 卖平市
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        self.sendorder(symbol, orderref, 0, vol, direction, offset, TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice)

    def coverToday(self, symbol, orderref, price, vol):  # 卖平今
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_CloseToday.value)
        self.sendorder(symbol, orderref, price, vol, direction, offset)

    def coverMarketToday(self, symbol, orderref, vol):  # 卖平市
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_CloseToday.value)
        self.sendorder(symbol, orderref, 0, vol, direction, offset, TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice)

    def cancelOrder(self, order):
        """撤单"""
        self.t.ReqOrderAction(BrokerID=self.brokerid,
                              InvestorID=self.userid,
                              OrderRef=order['OrderRef'],
                              FrontID=int(order['FrontID']),
                              SessionID=int(order['SessionID']),
                              OrderSysID=order['OrderSysID'],
                              ActionFlag=TThostFtdcActionFlagType.THOST_FTDC_AF_Delete,
                              ExchangeID=order["ExchangeID"],
                              InstrumentID=order['InstrumentID'])

    # endregion

    # region 预埋单
    def sendorderPark(self, instrumentid, orderref, price, vol, direction, offset,
                      OrderPriceType=TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice):
        goodsCode = getGoodsCode(instrumentid)
        if goodsCode.split('.')[1] == 'SHF':
            exChangeID = 'SHFE'
        elif goodsCode.split('.')[1] == 'DCE':
            exChangeID = 'DCE'
        elif goodsCode.split('.')[1] == 'CZC':
            exChangeID = 'CZCE'
        elif goodsCode.split('.')[1] == 'CFE':
            exChangeID = 'CFFEX'
        elif goodsCode.split('.')[1] == 'INE':
            exChangeID = 'INE'
        self.t.ReqParkedOrderInsert(BrokerID=self.brokerid,
                                    InvestorID=self.userid,
                                    InstrumentID=instrumentid,
                                    OrderRef=orderref,
                                    UserID=self.userid,
                                    OrderPriceType=OrderPriceType,
                                    Direction=direction,
                                    CombOffsetFlag=offset,
                                    CombHedgeFlag=chr(TThostFtdcHedgeFlagType.THOST_FTDC_HF_Speculation.value),
                                    LimitPrice=price,
                                    VolumeTotalOriginal=vol,
                                    TimeCondition=TThostFtdcTimeConditionType.THOST_FTDC_TC_GFD,
                                    VolumeCondition=TThostFtdcVolumeConditionType.THOST_FTDC_VC_AV,
                                    MinVolume=1,
                                    ForceCloseReason=TThostFtdcForceCloseReasonType.THOST_FTDC_FCC_NotForceClose,
                                    ContingentCondition=TThostFtdcContingentConditionType.THOST_FTDC_CC_Immediately,
                                    ExchangeID=exChangeID)
        return orderref

    def buyPark(self, symbol, orderref, price, vol):  # 多开
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value)
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def sellPark(self, symbol, orderref, price, vol):  # 多平
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def sellMarketPark(self, symbol, orderref, vol):  # 多平
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        self.sendorderPark(symbol, orderref, 0, vol, direction, offset, TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice)

    def selltodayPark(self, symbol, orderref, price, vol):  # 平今多
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_CloseToday.value)
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def shortPark(self, symbol, orderref, price, vol):  # 卖开空开
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value)
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def coverPark(self, symbol, orderref, price, vol):  # 空平
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def coverMarketPark(self, symbol, orderref, vol):  # 空平
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        self.sendorderPark(symbol, orderref, 0, vol, direction, offset, TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice)

    def covertodayPark(self, symbol, orderref, price, vol):  # 平今空
        direction = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        offset = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_CloseToday.value)
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def cancelOrderPark(self, order):  # 预撤单
        self.t.ReqParkedOrderAction(BrokerID=self.brokerid,
                                    InvestorID=self.userid,
                                    OrderRef=order['OrderRef'],
                                    FrontID=int(order['FrontID']),
                                    SessionID=int(order['SessionID']),
                                    OrderSysID=order['OrderSysID'],
                                    ActionFlag=TThostFtdcActionFlagType.THOST_FTDC_AF_Delete,
                                    ExchangeID=order["ExchangeID"],
                                    InstrumentID=order['InstrumentID'])

    def onRspQryDepthMarketData(self, data, error, n, last): #获取tick数据操作
        icon = filter(lambda x:x.isalpha(), data.getInstrumentID())
        icon = ''.join(list(icon))
        event = Event(type_=EVENT_INSTRUMENT)
        event.dict_['InstrumentID'] = data.getInstrumentID()
        event.dict_['ProductID'] = icon
        event.dict_['OpenInterest'] = data.getOpenInterest()
        event.dict_['last'] = last
        # 主力合约的涨停版
        if event.dict_['InstrumentID'] in listInstrument:
            # 可以避免多线程下数据的混乱
            if dictInstrumentUpDownPrice.get(event.dict_['InstrumentID'], [0, 0]) == [0, 0]:
                dictInstrumentUpDownPrice[event.dict_['InstrumentID']] = [data.getUpperLimitPrice(), data.getLowerLimitPrice()]
        ee.put(event)
    # endregion
