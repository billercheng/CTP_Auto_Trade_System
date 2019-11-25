import socket
from mdApi import MdApi
from tdApi import TdApi
from onBar import *
import queue
from PyQt5.QtWidgets import QApplication
from qtpy.QtCore import QTimer
import sys

class RdMd():

    def __init__(self):
        # 检测是否登陆
        self.loginTime = None
        # 关于建立 socket 通信时使用的变量
        self.queueRecv = queue.PriorityQueue()
        self.strRecv = ""
        # 事件处理方法
        self.registerEngine()
        # 读取数据到内存
        threading.Thread(target=self.getData, daemon=True).start()
        # qtimer 检测自动下止盈单
        self.timer0 = QTimer()
        self.timer0.timeout.connect(self.flushPosition)  # 确保所有持仓都挂止盈单
        self.timer0.start(7000)
        # qtimer 检测持仓的情况
        self.timer1 = QTimer()
        self.timer1.timeout.connect(self.showPosition)  # 确保显示所有的持仓信息
        self.timer1.start(30000)
    
    def getData(self):
        downLogProgram("读取 mongodb 数据库数据，并写入内存上")
        for freq in listFreqPlus:
            dictData[freq] = {}
            downLogProgram("将频段 {} 数据库数据写入内存".format(freq))
            db = con['cta{}_trade'.format(freq)]
            for goodsCode in dictGoodsName.keys():
                if goodsCode in dictFreqUnGoodsCode[freq]:  # 如果没有交易的话，不需要读取
                    continue
                goodsName = dictGoodsName[goodsCode]
                num = len(dictFreqGoodsClose[freq][goodsCode]) * 5 + mvlenvector[-1] + 10
                dictData[freq][goodsName + '_调整表'] = readMongoNum(db, '{}_调整表'.format(goodsName), num).set_index('trade_time').sort_index()
                dictData[freq][goodsName + '_调整表'] = dictData[freq][goodsName + '_调整表'][listMin]
                if freq != 1:
                    dictData[freq][goodsName + '_均值表'] = readMongoNum(db, '{}_均值表'.format(goodsName), num).set_index('trade_time').sort_index()
                    dictData[freq][goodsName + '_均值表'] = dictData[freq][goodsName + '_均值表'][listMa]
                    dictData[freq][goodsName + '_重叠度表'] = readMongoNum(db, '{}_重叠度表'.format(goodsName), num).set_index('trade_time').sort_index()
                    dictData[freq][goodsName + '_重叠度表'] = dictData[freq][goodsName + '_重叠度表'][listOverLap]
                    # 这个需要从本地的数据库上读取
                    dictData[freq][goodsName + '_周交易明细表'] = readMongo(goodsName + '_周交易明细表', dictFreqDb[freq])
                    if dictData[freq][goodsName + '_周交易明细表'].shape[0] == 0:
                        dictData[freq][goodsName + '_周交易明细表'] = pd.DataFrame(columns=listWeekTradeTab).set_index('交易时间', drop=False)
                    else:
                        dictData[freq][goodsName + '_周交易明细表'] = dictData[freq][goodsName + '_周交易明细表'][listWeekTradeTab].set_index('交易时间', drop=False)
                    getWeekTradeTab(goodsCode, freq)
        self.getZhuli()  # 获取品种的调整时刻表
        # 刷新周交易明细表
        # 如果执行的程序不在交易时间内，不执行周交易明细表
        now = datetime.now()
        if (time(2, 30) < now.time() < time(8, 59)) or (time(11, 30) < now.time() < time(13, 30)) or (time(10, 15) < now.time() < time(10, 30)) or (time(15, 15) < now.time() < time(20, 59)) or now.date() not in tradeDate.tolist():
            self.execWeekTradeTab()
        event = Event(EVENT_LOGIN)  # 登陆事件
        ee.put(event)
    
    def getZhuli(self):
        downLogProgram("从 CTA{} 上读取主力合约".format(1))
        for goodsCode in dictGoodsName.keys():
            goodsName = dictGoodsName[goodsCode]
            dictGoodsAdj[goodsCode] = readMongo(goodsName + '_调整时刻表', con['cta1_trade']).set_index('goods_code')
            dictGoodsAdj[goodsCode]['adjdate'] = pd.to_datetime(dictGoodsAdj[goodsCode]['adjdate']) + timedelta(hours=16)
            dictGoodsInstrument[goodsCode] = dictGoodsAdj[goodsCode].index[-1].split('.')[0]
            instrument = dictGoodsAdj[goodsCode].index[-1].split('.')[0]
            listInstrument.append(instrument)
            dictInstrumentPrice[instrument] = 0
            dictInstrumentUpDownPrice[instrument] = [0, 0]
        # 如果指令单上的合约不在主力合约的话，那么自动切换为主力合约吧
        for i in range(dfCommand.shape[0]):
            instrument = dfCommand['合约号'].iat[i]
            if instrument not in listInstrument:
                dfCommand['合约号'].iat[i] = dictGoodsInstrument[getGoodsCode(instrument)]
            else:
                # 如果新的周数不需要这个频段品种的话，直接加上 '.'
                goodsCode = getGoodsCode(instrument)
                freq = int(dfCommand['本地下单码'][i].split('.')[1])
                if goodsCode in dictFreqUnGoodsCode[freq]:
                    dfCommand['合约号'].iat[i] += '.'
        dfCommand.to_pickle('pickle\\dfCommand.pkl')

    def execWeekTradeTab(self):
        downLogProgram("正在刷新周交易明细表")
        for freq in listFreq:
            for goodsCode in dictGoodsName.keys():
                if goodsCode in dictFreqUnGoodsCode[freq]:
                    continue
                goodsName = dictGoodsName[goodsCode]
                dictData[freq][goodsName + '_周交易明细表'] = dictData[freq][goodsName + '_周交易明细表'][0:0].copy()
                table = dictFreqDb[freq]['{}_周交易明细表'.format(goodsName)]  # 删除周交易明细表
                table.drop()
                getWeekTradeTab(goodsCode, freq)
        self.execCommand()
        downLogProgram("刷新周交易明细表完成")
        
    def execCommand(self):
        for index in dfCommand.index:  # 将所有的下单指令，全部改为失效
            event = Event(type_=EVENT_SHOWCOMMAND)
            d = {}
            d['isChg'] = True
            d['index'] = index
            d['goods_code'] = dfCommand['合约号'][index] + '.'
            event.dict_ = d
            ee.put(event)
        # 刷新指令
        for freq in listFreq:  # 刷新指令
            for goodsCode in dictGoodsName.keys():
                if goodsCode not in dictFreqUnGoodsCode[freq]:
                    goodsName = dictGoodsName[goodsCode]
                    tradeTime = dictData[freq][goodsName + '_调整表'].index[-1]
                    indexGoods = listGoods.index(goodsCode)
                    indexBar = dictFreqGoodsClose[freq][goodsCode].index(tradeTime.time())
                    orderRef = theTradeDay.strftime('%Y%m%d') + '.' + str(freq) + '.' + str(indexGoods) + '.' + str(indexBar) + '.'
                    preOrderRef = ""
                    if ((datetime.now() - tradeTime) < timedelta(days=1, hours=1)) or (dictFreqGoodsCloseNight[freq][goodsCode][-1] == dictFreqGoodsCloseNight[1][goodsCode][-1]):
                        getOrder(freq, goodsCode, orderRef, preOrderRef, False)

    # region 事件对应处理方法
    def registerEngine(self):
        ee.register(EVENT_ORDER, self.order)  # 委托单
        ee.register(EVENT_TRADE, self.trade)  # 成交单
        ee.register(EVENT_ORDER_ERROR, self.error)  # 错误单
        ee.register(EVENT_SHOWPOSITION, self.showPositionEvent)  # 重新显示持仓信息
        ee.register(EVENT_ORDERCOMMAND, self.orderCommand)  # 下单
        ee.register(EVENT_ORDERCANCEL, self.orderCancel)  # 撤单
        ee.register(EVENT_ORDERCANCELPARK, self.orderCancelPark)  # 预撤单
        ee.register(EVENT_MARKETDATA_CONTRACT, self.dealTickData)  # 处理tick数据
        ee.register(EVENT_SHOWCOMMAND, self.showCommand)  # 指令单的更改操作
        ee.register(EVENT_LOGIN, self.login)  # 登陆
        self.checkInstrumentChg = True  # 切换主力合约的判断
        self.listInstrumentInformation = []  # 保存合约资料
        ee.register(EVENT_INSTRUMENT, self.insertMarketData)  # 检测是否切换合约
        ee.register(EVENT_MARKETORDER, self.marketOrder)  # 进行某频段某品种的市价平仓操作
        ee.start()

    # 查询委托单状态
    def order(self, event):
        var = event.dict_
        tmp = {}
        if var["OrderRef"] not in dictOrderRef.keys():
            return
        else:
            tmp["本地下单码"] = dictOrderRef[var["OrderRef"]]
        tmp["时间"] = pd.to_datetime(theTradeDay.strftime('%Y%m%d') + ' ' + var["InsertTime"])
        tmp["代码"] = var['InstrumentID']
        varGoodsCode = getGoodsCode(var['InstrumentID'])
        freq = int(tmp["本地下单码"].split('.')[1])
        # 不在频段，return
        if freq not in listFreq:
            return
        # 同一个orderRef 上，还需要相同的goodsCode才进行操作
        codeGoodsCode = listGoods[int(tmp["本地下单码"].split('.')[2])]
        if codeGoodsCode != varGoodsCode:
            return
        if var["Direction"] == TThostFtdcDirectionType.THOST_FTDC_D_Buy:
            if var["CombOffsetFlag"] == '0':
                tmp["方向"] = "买/开"
                tmp["价格"] = var["LimitPrice"]
                tmp["数量"] = var["VolumeTotalOriginal"]
            else:
                tmp["方向"] = "买/平"
                tmp["价格"] = var["LimitPrice"]
                tmp["数量"] = var["VolumeTotalOriginal"]
        else:
            if var["CombOffsetFlag"] == '0':
                tmp["方向"] = "卖/开"
                tmp["价格"] = var["LimitPrice"]
                tmp["数量"] = var["VolumeTotalOriginal"] * (-1)
            else:
                tmp["方向"] = "卖/平"
                tmp["价格"] = var["LimitPrice"]
                tmp["数量"] = var["VolumeTotalOriginal"] * (-1)
        tmp["状态"] = var["StatusMsg"]
        tmp["已成交"] = var["VolumeTraded"]
        tmp["成交均价"] = var["LimitPrice"]
        tmp["拒绝原因"] = var["VolumeTraded"]
        # 记录：
        downLogTradeRecord('order: ' + str(tmp))
        if var["OrderRef"] not in dfOrderSource['OrderRef'].tolist():
            with lockDfOrder:
                tmp['id'] = dfOrder.shape[0]
                dfOrder.loc[dfOrder.shape[0]] = [tmp[x] for x in listFreqOrder]
                dfOrderSource.loc[dfOrderSource.shape[0]] = [var[x] for x in listOrderSourceColumns]
        else:
            with lockDfOrder:
                index = dfOrderSource['OrderRef'].tolist().index(var["OrderRef"])
                tmp['id'] = index
                dfOrder.loc[index] = [tmp[x] for x in listFreqOrder]
                dfOrderSource.loc[index] = [var[x] for x in listOrderSourceColumns]
        # 记录委托单数据：
        dfFreqOrder = dictFreqOrder[freq]
        dfFreqOrderSource = dictFreqOrderSource[freq]
        table = dictFreqDb[freq]['委托记录']
        if var["OrderRef"] not in dfFreqOrderSource['OrderRef'].tolist():
            with lockDictFreqOrder:
                tmp['id'] = dfFreqOrder.shape[0]
                dfFreqOrder.loc[dfFreqOrder.shape[0]] = [tmp[x] for x in listFreqOrder]
                dfFreqOrderSource.loc[dfFreqOrderSource.shape[0]] = [var[x] for x in listOrderSourceColumns]
            # 记录到 mongodb 上
            newDict = dict([[x, tmp[x]] for x in listFreqOrder])
            newDict = insertDbChg(newDict)
            table.insert_one(newDict)
        else:
            with lockDictFreqOrder:
                index = dfFreqOrderSource['OrderRef'].tolist().index(var["OrderRef"])
                tmp['id'] = index
                dfFreqOrder.loc[index] = [tmp[x] for x in listFreqOrder]
                dfFreqOrderSource.loc[index] = [var[x] for x in listOrderSourceColumns]
            # 在 mongodb 上修改， 通过 index 位置
            mongoIndex = {'id': int(index)}
            newDict = dict([[x, tmp[x]] for x in listFreqOrder])
            newDict = insertDbChg(newDict)
            table.update_one(mongoIndex, {"$set": newDict})
        if tmp["状态"] == '已撤单' \
                and tmp["本地下单码"] in dictPreOrderRefOrder.keys():
            # 如果是最后一个的话，那么进行下单操作
            if var["OrderRef"] == dictRefOrder[tmp["本地下单码"]][-1]:
                event = dictPreOrderRefOrder.pop(tmp["本地下单码"])
                event.dict_['preOrderRef'] = ''
                self.orderCommand(event)

    def trade(self, event):
        var = event.dict_
        tmp = {}
        if var["OrderRef"] not in dictOrderRef.keys():
            return
        else:
            var["OrderRef"] = dictOrderRef[var["OrderRef"]]
            tmp["本地下单码"] = var["OrderRef"]
        tmp["时间"] = pd.to_datetime(theTradeDay.strftime('%Y%m%d') + ' ' + var["TradeTime"])
        tmp["代码"] = var['InstrumentID']
        if tmp["代码"][-4:].isdigit():
            if tmp["代码"][:-4] not in dictGoodsChg.keys():
                return
        else:
            if tmp["代码"][:-3] not in dictGoodsChg.keys():
                return
        tradeTime = pd.to_datetime(tmp["时间"])
        goodsCode = getGoodsCode(var['InstrumentID'])
        tmp["名称"] = dictGoodsName[goodsCode]
        if var["Direction"] == TThostFtdcDirectionType.THOST_FTDC_D_Buy:
            if var["OffsetFlag"] == TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open:
                tmp["方向"] = "买/开"
                tmp["数量"] = var["Volume"]
            else:
                tmp["方向"] = "买/平"
                tmp["数量"] = var["Volume"]
        else:
            if var["OffsetFlag"] == TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open:
                tmp["方向"] = "卖/开"
                tmp["数量"] = var["Volume"] * (-1)
            else:
                tmp["方向"] = "卖/平"
                tmp["数量"] = var["Volume"] * (-1)
        tmp["价格"] = var["Price"]
        tmp['持仓盈亏'] = 0
        downLogTradeRecord('trade: ' + str(tmp))
        # 本地下单码 日期 5 位  频段 2 位  品种 2 位  第几个 bar 数据 2 位  开仓 还是 平仓 1 位
        freq = int(tmp["本地下单码"].split('.')[1])
        tmp['freq'] = freq
        if freq not in listFreq:
            return False
        codeGoodsCode = listGoods[int(tmp["本地下单码"].split('.')[2])]
        if codeGoodsCode != goodsCode:
            return
        # 关于持仓的增加问题
        dfFreqTrade = dictFreqTrade[freq]
        table = dictFreqDb[freq]['频段成交']
        if tmp["本地下单码"] not in dfFreqTrade['本地下单码'].tolist():
            tmp['id'] = dfFreqTrade.shape[0]
            dfFreqTrade.loc[dfFreqTrade.shape[0]] = [tmp[x] for x in listFreqTrade]
            # 记录到 mongodb 上
            newDict = dict([[x, tmp[x]] for x in listFreqTrade])
            newDict = insertDbChg(newDict)
            table.insert_one(newDict)
        else:
            index = dfFreqTrade[dfFreqTrade['本地下单码'] == tmp["本地下单码"]].index[0]
            dfFreqTrade.at[index, '数量'] += tmp["数量"]
            dfFreqTrade.at[index, '时间'] = tradeTime
            # 在 mongodb 上修改， 通过 index 位置
            mongoIndex = {'id': int(index)}
            newDict = {'数量':dfFreqTrade.at[index, '数量'], '时间':tradeTime}
            newDict = insertDbChg(newDict)
            table.update_one(mongoIndex, {"$set": newDict})
        # 查看该成交记录是否已经处理
        tradeID = tradeTime.strftime('%Y%m%d%H%M%S') + var['TradeID']
        if tradeID not in listTradeID:
            listTradeID.append(tradeID)
            pd.to_pickle(listTradeID, 'pickle\\listTradeID.pkl')
        else:
            return
        # 对频段持仓进行处理
        dfCommandTemp = dfCommand.copy()
        if var["OffsetFlag"] == TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open:
            if tmp["代码"] not in dictFreqPosition[freq]['代码'].values:
                event = Event(type_=EVENT_SHOWPOSITION)
                event.dict_ = tmp.copy()
                event.dict_['append'] = True
                self.showPositionEvent(event)
                # 进行下止盈单操作
                orderRef = tmp["本地下单码"][:-1] + '2'
                orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                orderEvent.dict_['InstrumentID'] = var['InstrumentID']
                if var["Direction"] == TThostFtdcDirectionType.THOST_FTDC_D_Buy:
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                    if not dfCommandTemp.loc[dfCommandTemp['本地下单码'] == tmp["本地下单码"]].empty:
                        index = dfCommandTemp.index[dfCommandTemp['本地下单码'] == tmp["本地下单码"]][-1]
                        s = dfCommandTemp.loc[index]
                        profitLine = s['多止盈线']
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = profitLine
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(var["Volume"])
                        self.orderCommand(orderEvent)
                        # region 将止损指令写入到 指令单上
                        if tmp["本地下单码"][:-1] + '0' not in dfCommand['本地下单码'].tolist():
                            # 新写入止损单
                            s["本地下单码"] = tmp["本地下单码"][:-1] + '0'
                            s['应开多手数'] = 0
                            s['应开空手数'] = 0
                            s['合约号'] = s['合约号'].split('.')[0]
                            s['持有多手数'] = int(var["Volume"])
                            s['发单时间'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            event = Event(type_=EVENT_SHOWCOMMAND)
                            event.dict_ = s
                            self.showCommand(event)
                        else:
                            # 能够直接执行是因为 dfCommand 是单线程操作，不会有两条线路操作
                            index = dfCommand['本地下单码'].tolist().index(tmp["本地下单码"][:-1] + '0')
                            with lockDfCommand:
                                dfCommand.at[index, '持有多手数'] += int(var["Volume"])
                                dfCommand.to_pickle('pickle\\dfCommand.pkl')
                        # endregion
                else:
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                    if not dfCommandTemp.loc[dfCommandTemp['本地下单码'] == tmp["本地下单码"]].empty:
                        index = dfCommandTemp.index[dfCommandTemp['本地下单码'] == tmp["本地下单码"]][-1]
                        s = dfCommandTemp.loc[index]
                        profitLine = s['空止盈线']
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = profitLine
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(var["Volume"])
                        self.orderCommand(orderEvent)
                        # region 写入 dfCommand 上
                        if tmp["本地下单码"][:-1] + '0' not in dfCommand['本地下单码'].tolist():
                            # 新写入止损单
                            s["本地下单码"] = tmp["本地下单码"][:-1] + '0'
                            s['应开多手数'] = 0
                            s['应开空手数'] = 0
                            s['合约号'] = s['合约号'].split('.')[0]
                            s['持有空手数'] = int(var["Volume"])
                            s['发单时间'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            event = Event(type_=EVENT_SHOWCOMMAND)
                            event.dict_ = s
                            self.showCommand(event)
                        else:  # 将止损指令写入到指令表格上
                            index = dfCommand['本地下单码'].tolist().index(tmp["本地下单码"][:-1] + '0')
                            with lockDfCommand:
                                dfCommand.at[index, '持有空手数'] += int(var["Volume"])
                                dfCommand.to_pickle('pickle\\dfCommand.pkl')
                        # endregion
            else:
                index = dictFreqPosition[freq]['代码'].tolist().index(tmp["代码"])
                event = Event(type_=EVENT_SHOWPOSITION)
                event.dict_ = tmp.copy()
                event.dict_['数量'] += dictFreqPosition[freq]['数量'][index]
                event.dict_['append'] = True
                self.showPositionEvent(event)
                # 进行下止盈单操作
                orderRef = tmp["本地下单码"][:-1] + '2'
                orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                orderEvent.dict_['InstrumentID'] = var['InstrumentID']
                if var["Direction"] == TThostFtdcDirectionType.THOST_FTDC_D_Buy:
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                    if not dfCommandTemp.loc[dfCommandTemp['本地下单码'] == tmp["本地下单码"]].empty:
                        index = dfCommandTemp.index[dfCommandTemp['本地下单码'] == tmp["本地下单码"]][-1]
                        s = dfCommandTemp.loc[index]
                        profitLine = s['多止盈线']
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = profitLine
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(var["Volume"])
                        self.orderCommand(orderEvent)
                        # region 写入 dfCommand 上
                        if tmp["本地下单码"][:-1] + '0' not in dfCommand['本地下单码'].tolist():
                            # 新写入止损单
                            s["本地下单码"] = tmp["本地下单码"][:-1] + '0'
                            s['应开多手数'] = 0
                            s['应开空手数'] = 0
                            s['合约号'] = s['合约号'].split('.')[0]
                            s['持有多手数'] = int(var["Volume"])
                            s['发单时间'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            event = Event(type_=EVENT_SHOWCOMMAND)
                            event.dict_ = s
                            self.showCommand(event)
                        else:
                            # 能够直接执行是因为 dfCommand 是单线程操作，不会有两条线路操作
                            index = dfCommand['本地下单码'].tolist().index(tmp["本地下单码"][:-1] + '0')
                            with lockDfCommand:
                                dfCommand.at[index, '持有多手数'] += int(var["Volume"])
                                dfCommand.to_pickle('pickle\\dfCommand.pkl')
                        # endregion
                else:
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                    if not dfCommandTemp.loc[dfCommandTemp['本地下单码'] == tmp["本地下单码"]].empty:
                        index = dfCommandTemp.index[dfCommandTemp['本地下单码'] == tmp["本地下单码"]][-1]
                        s = dfCommandTemp.loc[index]
                        profitLine = s['空止盈线']
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = profitLine
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(var["Volume"])
                        self.orderCommand(orderEvent)
                        # region 写入 dfCommand上
                        if tmp["本地下单码"][:-1] + '0' not in dfCommand['本地下单码'].tolist():
                            # 新写入止损单
                            s["本地下单码"] = tmp["本地下单码"][:-1] + '0'
                            s['应开多手数'] = 0
                            s['应开空手数'] = 0
                            s['合约号'] = s['合约号'].split('.')[0]
                            s['持有空手数'] = int(var["Volume"])
                            s['发单时间'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            event = Event(type_=EVENT_SHOWCOMMAND)
                            event.dict_ = s
                            self.showCommand(event)
                        else:
                            index = dfCommand['本地下单码'].tolist().index(tmp["本地下单码"][:-1] + '0')
                            with lockDfCommand:
                                dfCommand.at[index, '持有空手数'] += int(var["Volume"])
                                dfCommand.to_pickle('pickle\\dfCommand.pkl')
                        # endregion
        else:  # 如果 成交单 为平仓，而且 代码 在 dfFreqPosition[freq] 上的话，进行删除持仓操作
            if tmp["代码"] in dictFreqPosition[freq]['代码'].tolist():
                index = dictFreqPosition[freq]['代码'].tolist().index(tmp["代码"])
                if abs(tmp["数量"]) < abs(dictFreqPosition[freq]['数量'][index]):
                    event = Event(type_=EVENT_SHOWPOSITION)
                    event.dict_ = tmp.copy()
                    event.dict_['时间'] = dictFreqPosition[freq]['时间'][index]
                    event.dict_['数量'] += dictFreqPosition[freq]['数量'][index]
                    event.dict_['append'] = True
                    self.showPositionEvent(event)
                else:
                    event = Event(type_=EVENT_SHOWPOSITION)
                    event.dict_ = tmp.copy()
                    self.showPositionEvent(event)

    def error(self, event):
        var = event.dict_
        dictTemp = {}
        if var['OrderRef'] not in dictOrderRef.keys():
            return
        else:
            dictTemp['本地下单码'] = dictOrderRef[var['OrderRef']]
        dictTemp['代码'] = var['InstrumentID']
        if dictTemp["代码"][-4:].isdigit():
            if dictTemp["代码"][:-4] not in dictGoodsChg.keys():
                return
        else:
            if dictTemp["代码"][:-3] not in dictGoodsChg.keys():
                return
        freq = int(dictTemp["本地下单码"].split('.')[1])
        dictTemp['freq'] = freq
        goodsCode = getGoodsCode(var['InstrumentID'])
        dictTemp['名称'] = dictGoodsName[goodsCode]
        if var["Direction"] == TThostFtdcDirectionType.THOST_FTDC_D_Buy:
            if var["CombOffsetFlag"] == '0':
                theDirection = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                dictTemp["方向"] = "买/开"
                dictTemp["价格"] = var["LimitPrice"]
                dictTemp["数量"] = var["VolumeTotalOriginal"]
            else:
                theDirection = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                dictTemp["方向"] = "买/平"
                dictTemp["价格"] = var["LimitPrice"]
                dictTemp["数量"] = var["VolumeTotalOriginal"]
        else:
            if var["CombOffsetFlag"] == '0':
                theDirection = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                dictTemp["方向"] = "卖/开"
                dictTemp["价格"] = var["LimitPrice"]
                dictTemp["数量"] = var["VolumeTotalOriginal"] * (-1)
            else:
                theDirection = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                dictTemp["方向"] = "卖/平"
                dictTemp["价格"] = var["LimitPrice"]
                dictTemp["数量"] = var["VolumeTotalOriginal"] * (-1)
        dictTemp['错误原因'] = var.get('ErrorMsg', "未知原因")
        dictTemp['时间'] = datetime.now()
        downLogTradeRecord('error: ' + str(var))
        table = dictFreqDb[freq]['错误委托单']
        newDict = dict([[x, dictTemp[x]] for x in listFreqError])
        newDict = insertDbChg(newDict)
        table.insert_one(newDict)
        # 如果为 ‘CTP 报单错误， 不允许重复报单’， 刚进行重新下单操作
        if dictTemp['错误原因'] == 'CTP:报单错误：不允许重复报单':
            # 删除 dictOrderRef
            dictOrderRef.pop(var['OrderRef'])
            dictRefOrder[dictTemp['本地下单码']].remove(var['OrderRef'])
            pd.to_pickle(dictOrderRef, 'pickle\\dictOrderRef.pkl')
            pd.to_pickle(dictRefOrder, 'pickle\\dictRefOrder.pkl')
            # 进行下单操作
            orderEvent = Event(type_=EVENT_ORDERCOMMAND)
            orderEvent.dict_['InstrumentID'] = var['InstrumentID']
            orderEvent.dict_['Direction'] = theDirection
            orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
            orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
            orderEvent.dict_['LimitPrice'] = var["LimitPrice"]
            orderEvent.dict_['orderref'] = dictTemp['本地下单码']
            orderEvent.dict_['VolumeTotalOriginal'] = var["VolumeTotalOriginal"]
            self.orderCommand(orderEvent)
        elif dictTemp['错误原因'] == 'CTP:资金不足':
            downLogProgram('资金不足')
        else:
            downLogProgram('出现非预期错误，请查明原因。')
            assert False  # 如果错误不在预期之类，直接中断程序吧

    def showPositionEvent(self, event):
        with lockDictFreqPosition:
            if event.dict_.get('append', False):
                freq = event.dict_['freq']
                dictTemp = event.dict_.copy()
                if dictTemp['代码'] in dictFreqPosition[freq]['代码'].tolist():  # 如果在 持仓信息上， 则进行成交量的相加
                    index = dictFreqPosition[freq]['代码'].tolist().index(dictTemp['代码'])
                    dictFreqPosition[freq].loc[index] = [dictTemp[x] for x in listFreqPosition]
                    table = dictFreqDb[freq]['频段持仓']
                    mongoIndex = {'代码': dictTemp['代码']}
                    newDict = dict([[x, dictTemp[x]] for x in listFreqPosition])
                    newDict = insertDbChg(newDict)
                    table.update_one(mongoIndex, {"$set": newDict})
                else:
                    dictFreqPosition[freq].loc[dictFreqPosition[freq].shape[0]] = [dictTemp[x] for x in listFreqPosition]
                    table = dictFreqDb[freq]['频段持仓']
                    newDict = dict([[x, dictTemp[x]] for x in listFreqPosition])
                    newDict = insertDbChg(newDict)
                    table.insert_one(newDict)
            else:
                freq = event.dict_['freq']
                instrument = event.dict_['代码']
                if instrument in dictFreqPosition[freq]['代码'].tolist():
                    index = dictFreqPosition[freq]['代码'].tolist().index(instrument)
                    dictFreqPosition[freq] = dictFreqPosition[freq].drop([index]).reset_index(drop = True)
                    table = dictFreqDb[freq]['频段持仓']
                    mongoIndex = {'代码': instrument}
                    table.delete_one(mongoIndex)
        pd.to_pickle(dictFreqPosition, 'pickle\\dictFreqPosition.pkl')

    def orderCommand(self, event):
        instrument = event.dict_['InstrumentID']
        orderPrice = event.dict_['LimitPrice']
        upPrice = dictInstrumentUpDownPrice[instrument][0]
        lowPrice = dictInstrumentUpDownPrice[instrument][1]
        if upPrice != 0 and lowPrice != 0:
            if orderPrice != 0:
                if orderPrice > upPrice:
                    listStopProfit.append(event.dict_['orderref'])  # 因为价格超过止盈单，所以不需要挂单
                    return
                elif orderPrice < lowPrice:
                    listStopProfit.append(event.dict_['orderref'])  # 因为价格低于止损单，所以不需要挂单
                    return
        # 开仓操作
        if event.dict_['CombOffsetFlag'] == chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value):
            now = datetime.now() + timedelta(hours=6)
            mapOrderref = now.strftime('%H%M%S%f')[:9] + (uniformCode + event.dict_['orderref'].split('.')[1]).zfill(3)[-3:]  #
            dictOrderRef[mapOrderref] = event.dict_['orderref']
            if event.dict_['orderref'] in dictRefOrder.keys():
                dictRefOrder[event.dict_['orderref']].append(mapOrderref)
            else:
                dictRefOrder[event.dict_['orderref']] = [mapOrderref]
            if event.dict_['Direction'] == TThostFtdcDirectionType.THOST_FTDC_D_Buy:  # 做多
                if event.dict_['OrderPriceType'] == TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice:
                    self.td.buy(instrument, mapOrderref, orderPrice,
                                event.dict_['VolumeTotalOriginal'])
                else:
                    self.td.buy(instrument, mapOrderref, upPrice,
                                event.dict_['VolumeTotalOriginal'])
            elif event.dict_['Direction'] == TThostFtdcDirectionType.THOST_FTDC_D_Sell: # 做空
                if event.dict_['OrderPriceType'] == TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice:
                    self.td.short(instrument, mapOrderref, orderPrice,
                                  event.dict_['VolumeTotalOriginal'])
                else:
                    self.td.short(instrument, mapOrderref, lowPrice,
                                  event.dict_['VolumeTotalOriginal'])
        elif event.dict_['CombOffsetFlag'] == chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value):  # 平仓操作
            # 判断是否有preOrderRef
            if event.dict_.get('preOrderRef', '') != '':
                preOrderRef = event.dict_['preOrderRef']
                if preOrderRef in dfOrder['本地下单码'].tolist():
                    index = dfOrder['本地下单码'][dfOrder['本地下单码'] == preOrderRef].index[-1]
                    if dfOrder['状态'][index][:3] != "已撤单":
                        # 如果不等于已撤单的话，我需要等到撤单为止
                        dictPreOrderRefOrder[preOrderRef] = event
                        return
            freq = int(event.dict_['orderref'].split('.')[1])
            if (freq in listFreq) and (instrument in dictFreqPosition[freq]['代码'].tolist()):
                index = dictFreqPosition[freq]['代码'].tolist().index(instrument)
                timeTemp = dictFreqPosition[freq]['时间'].iat[index]
                timeTemp = datetime(timeTemp.year, timeTemp.month, timeTemp.day)
                if timeTemp == theTradeDay:
                    isToday = True
                else:
                    isToday = False
            else:
                return
            now = datetime.now() + timedelta(hours=6)
            mapOrderref = now.strftime('%H%M%S%f')[:9] + (uniformCode + event.dict_['orderref'].split('.')[1]).zfill(3)[-3:]
            dictOrderRef[mapOrderref] = event.dict_['orderref']
            if event.dict_['orderref'] in dictRefOrder.keys():
                dictRefOrder[event.dict_['orderref']].append(mapOrderref)
            else:
                dictRefOrder[event.dict_['orderref']] = [mapOrderref]
            if event.dict_['Direction'] == TThostFtdcDirectionType.THOST_FTDC_D_Buy:  # 买
                if event.dict_['OrderPriceType'] == TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice:
                    if isToday:
                        self.td.coverToday(instrument, mapOrderref,
                                           orderPrice,
                                           event.dict_['VolumeTotalOriginal'])
                    else:
                        self.td.cover(instrument, mapOrderref,
                                      orderPrice,
                                      event.dict_['VolumeTotalOriginal'])
                elif event.dict_['OrderPriceType'] == TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice:
                    if isToday:
                        self.td.coverToday(instrument, mapOrderref,
                                           upPrice,
                                           event.dict_['VolumeTotalOriginal'])
                    else:
                        self.td.cover(instrument, mapOrderref,
                                      upPrice,
                                      event.dict_['VolumeTotalOriginal'])
            elif event.dict_['Direction'] == TThostFtdcDirectionType.THOST_FTDC_D_Sell:
                if event.dict_['OrderPriceType'] == TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice:
                    if isToday:
                        self.td.sellToday(instrument, mapOrderref,
                                          orderPrice,
                                          event.dict_['VolumeTotalOriginal'])
                    else:
                        self.td.sell(instrument, mapOrderref,
                                     orderPrice,
                                     event.dict_['VolumeTotalOriginal'])
                elif event.dict_['OrderPriceType'] == TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice:
                    if isToday:
                        self.td.sellToday(instrument, mapOrderref,
                                          lowPrice,
                                          event.dict_['VolumeTotalOriginal'])
                    else:
                        self.td.sell(instrument, mapOrderref,
                                     lowPrice,
                                     event.dict_['VolumeTotalOriginal'])
        pd.to_pickle(dictOrderRef, 'pickle\\dictOrderRef.pkl')
        pd.to_pickle(dictRefOrder, 'pickle\\dictRefOrder.pkl')

    def orderCancel(self, event):
        orderref = event.dict_['orderref']
        for eachRefOrder in dictRefOrder.get(orderref, [orderref]):
            if eachRefOrder in dfOrderSource['OrderRef'].tolist():
                index = dfOrderSource['OrderRef'].tolist().index(eachRefOrder)
                if dfOrderSource['StatusMsg'][index] not in ["全部成交", "全部成交报单已提交"] and dfOrderSource['StatusMsg'][index][:3] != "已撤单":
                    dict = dfOrderSource.loc[index].to_dict()
                    self.td.cancelOrder(dict)

    def orderCancelPark(self, event):
        orderref = event.dict_['orderref']
        for eachRefOrder in dictRefOrder.get(orderref, [orderref]):
            if eachRefOrder in dfOrderSource['OrderRef'].tolist():
                index = dfOrderSource['OrderRef'].tolist().index(eachRefOrder)
                if dfOrderSource['StatusMsg'][index] not in ["全部成交", "全部成交报单已提交"] and dfOrderSource['StatusMsg'][index][:3] != "已撤单":
                    dict = dfOrderSource.loc[index].to_dict()
                    self.td.cancelOrderPark(dict)

    def showCommand(self, event):
        data = event.dict_
        with lockDfCommand:
            if data.get('isChg', False):  # 更改本地下单码的命令，转为无效
                dfCommand.at[data['index'], '合约号'] = data['goods_code']
            else:
                # 将之前相同本地下单码的指令单转成无效
                dfCommandTemp = dfCommand[dfCommand['本地下单码'] == data['本地下单码']].copy()
                for i in dfCommandTemp.index:
                    dfCommand.at[i, '合约号'] += '.'
                dfCommand.loc[dfCommand.shape[0]] = [data[x] for x in listCommand]
        dfCommand.to_pickle('pickle\\dfCommand.pkl')
        
    def dealTickData(self, event):
        instrument = event.dict_["InstrumentID"]
        goodsCode = getGoodsCode(instrument)
        goodsUnit = dictGoodsUnit[goodsCode]
        goodsInstrument = instrument + '.' + goodsCode.split('.')[1]
        close = event.dict_["LastPrice"]
        askPrice = event.dict_["AskPrice1"]
        bidPrice = event.dict_["BidPrice1"]
        now = datetime.now()
        theTradeTime = pd.to_datetime(event.dict_["TradingDay"] + ' '
                                    + event.dict_["UpdateTime"] + '.'
                                    + str(event.dict_["UpdateMillisec"]))
        if theTradeTime > now:  # 防止夜盘时，tick数据会出错
            theTradeTime = datetime(now.year, now.month, now.day,
                                    theTradeTime.hour, theTradeTime.minute, theTradeTime.second, theTradeTime.microsecond)
        now += timedelta(minutes=1)
        nowTime = time(now.hour, now.minute)
        if nowTime not in dictFreqGoodsClose[1][goodsCode]:  # 如果不在交易时间内，tick无效
            return
        if dictInstrumentPrice[instrument] == 0:
            dictInstrumentPrice[instrument] = round(close, 4)
        # region 这里可以执行预下单操作
        listDrop = []
        for index in range(dfInstrumentNextOrder.shape[0]):
            if dfInstrumentNextOrder['合约号'][index] == instrument:  # 合约号 等于 instrument
                if dfInstrumentNextOrder['开始时间'][index] <= theTradeTime < dfInstrumentNextOrder['结束时间'][index]:
                    self.orderCommand(dfInstrumentNextOrder['事件'][index])
                    listDrop.append(index)
                elif theTradeTime >= dfInstrumentNextOrder['结束时间'][index]:
                    listDrop.append(index)
        if len(listDrop) > 0:
            dfInstrumentNextOrder.drop(listDrop, inplace=True)  # 删除 listDrop 的行
            dfInstrumentNextOrder.reset_index(drop=True, inplace=True)  # 重写索引
        # endregion
        with lockDfCommand:
            dfCommandTemp = dfCommand.copy()
        dfCommandTemp = dfCommandTemp[dfCommandTemp['合约号'] == instrument].copy()  # 这个tick数据处理，是完全的错误的吧：是的：
        for i in range(dfCommandTemp.shape[0]):
            s = dfCommandTemp.iloc[i]
            orderRef = s['本地下单码']
            freq = int(orderRef.split('.')[1])
            index = dfCommandTemp.index[i]
            if not judgeCodeValue(orderRef, theTradeTime):
                downLogTradeRecord('tick数据时间{}已经超出代码{}的有效时间'.format(theTradeTime, orderRef))
                event = Event(type_=EVENT_SHOWCOMMAND)
                s['isChg'] = True
                s['index'] = index
                s['goods_code'] = instrument + '.'
                event.dict_ = s
                self.showCommand(event)
                continue
            if orderRef[-1] == '1':
                if close <= s['多开仓线'] <= dictInstrumentPrice[instrument] or close >= s['多开仓线'] >= dictInstrumentPrice[instrument]:
                    # 如果 dictFreqPosition[freq] 已经存在持仓的话，不进行开仓
                    if instrument in dictFreqPosition[freq]['代码'].tolist():  # 如果已经持仓的，那不需要再开了
                        downLogTradeRecord("满足品种 {} 策略 {} 的多开仓线，进行下多仓单操作，但是该频段却持有仓位，则不进行开仓操作".format(instrument, orderRef))
                        continue
                    downLogTradeRecord("满足品种 {} 策略 {} 的多开仓线，进行下多仓单操作".format(instrument, orderRef))
                    # 修改 dfCommand 操作
                    event = Event(type_=EVENT_SHOWCOMMAND)
                    s['isChg'] = True
                    s['index'] = index
                    s['goods_code'] = instrument + '.'
                    event.dict_ = s
                    self.showCommand(event)
                    # 进行下单操作
                    orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                    orderEvent.dict_['InstrumentID'] = instrument
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                    orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value)
                    orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                    orderEvent.dict_['LimitPrice'] = s['多开仓线'] + 2 * goodsUnit
                    orderEvent.dict_['orderref'] = orderRef
                    orderEvent.dict_['VolumeTotalOriginal'] = int(s['应开多手数'])
                    self.orderCommand(orderEvent)
                elif close <= s['空开仓线'] <= dictInstrumentPrice[instrument] or close >= s['空开仓线'] >= dictInstrumentPrice[instrument]:
                    if instrument in dictFreqPosition[freq]['代码'].tolist():
                        downLogTradeRecord("满足品种 {} 策略 {} 的多开仓线，进行下多仓单操作，但是该频段却持有仓位，则不进行开仓操作".format(instrument, orderRef))
                        continue
                    downLogTradeRecord("满足品种 {} 策略 {} 的空开仓线，进行下多仓单操作".format(instrument, orderRef))
                    # 修改 dfCommand 操作
                    event = Event(type_=EVENT_SHOWCOMMAND)
                    s['isChg'] = True
                    s['index'] = index
                    s['goods_code'] = instrument + '.'
                    event.dict_ = s
                    self.showCommand(event)
                    # 进行下单操作
                    orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                    orderEvent.dict_['InstrumentID'] = instrument
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                    orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value)
                    orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                    orderEvent.dict_['LimitPrice'] = s['空开仓线'] - 2 * goodsUnit
                    orderEvent.dict_['orderref'] = orderRef
                    orderEvent.dict_['VolumeTotalOriginal'] = int(s['应开空手数'])
                    self.orderCommand(orderEvent)
            elif orderRef[-1] == '0':
                if s['持有多手数'] > 0:
                    if close <= s['多止损线']:
                        # 别这么急着下止损单，先看看止盈单下了没有
                        with lockDictFreqOrder:
                            dfFreqOrderTemp = dictFreqOrder[freq].copy()
                        # 没有挂止盈单，且止盈单又不是涨停的话，暂时不需要挂止损单
                        if (orderRef[:-1] + "2" not in dfFreqOrderTemp['本地下单码'].tolist()) and (orderRef[:-1] + "2" not in listStopProfit):
                            continue
                        downLogTradeRecord("满足品种 {} 策略 {} 的多止损线，进行多止损操作".format(instrument, orderRef))
                        cancelOrderRef = orderRef[:-1] + '2'
                        downLogTradeRecord("品种：{} 之前的编号 {} 进行撤单操作".format(instrument, cancelOrderRef))
                        cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                        cancelEvent.dict_['orderref'] = cancelOrderRef
                        self.orderCancel(cancelEvent)
                        # 修改 dfCommand 操作
                        event = Event(type_=EVENT_SHOWCOMMAND)
                        s['isChg'] = True
                        s['index'] = index
                        s['goods_code'] = instrument + '.'
                        event.dict_ = s
                        self.showCommand(event)
                        # 进行下单操作
                        orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                        orderEvent.dict_['InstrumentID'] = instrument
                        orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                        orderEvent.dict_['LimitPrice'] = 0
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(s['持有多手数'])
                        orderEvent.dict_['preOrderRef'] = cancelOrderRef
                        self.orderCommand(orderEvent)
                elif s['持有空手数'] > 0:
                    if close >= s['空止损线']:
                        # 别这么急着下止损单，先看看止盈单下了没有
                        with lockDictFreqOrder:
                            dfFreqOrderTemp = dictFreqOrder[freq].copy()
                        # 没有挂止盈单，且止盈单又不是涨停的话，暂时不需要挂止损单
                        if (orderRef[:-1] + "2" not in dfFreqOrderTemp['本地下单码'].tolist()) and (orderRef[:-1] + "2" not in listStopProfit):
                            continue
                        downLogTradeRecord("满足品种 {} 策略 {} 的空止损线，进行空止损操作".format(instrument, orderRef))
                        cancelOrderRef = orderRef[:-1] + '2'
                        downLogTradeRecord("品种：{} 之前的编号 {} 进行撤单操作".format(instrument, cancelOrderRef))
                        cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                        cancelEvent.dict_['orderref'] = cancelOrderRef
                        self.orderCancel(cancelEvent)
                        # 修改 dfCommand 操作
                        event = Event(type_=EVENT_SHOWCOMMAND)
                        s['isChg'] = True
                        s['index'] = index
                        s['goods_code'] = instrument + '.'
                        event.dict_ = s
                        self.showCommand(event)
                        # 进行下单操作
                        orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                        orderEvent.dict_['InstrumentID'] = instrument
                        orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                        orderEvent.dict_['LimitPrice'] = 0
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(s['持有空手数'])
                        orderEvent.dict_['preOrderRef'] = cancelOrderRef
                        self.orderCommand(orderEvent)
        dictInstrumentPrice[instrument] = round(close, 4)

    def login(self, event):
        userid = dictLoginInformation['userid']
        password = dictLoginInformation['password']
        brokerid = dictLoginInformation['broker']
        RegisterFront = dictLoginInformation['front_addr']
        product_info = dictLoginInformation['product_info']
        app_id = dictLoginInformation['app_id']
        auth_code = dictLoginInformation['auth_code']
        self.td = TdApi(userid, password, brokerid, RegisterFront.split(',')[0], product_info, app_id, auth_code)
        t = 0
        while not self.td.islogin and t < 1000:
            t += 1
            ttt.sleep(0.01)
        if t >= 1000:
            self.td.t.ReqUserLogout(brokerid, userid)
            downLogProgram('账号登陆失败，请重新登陆')
        else:
            self.md = MdApi(userid, password, brokerid, RegisterFront.split(',')[1])
            # 这两个线程都用于接收 1 分钟数据，并处理操作
            thd = threading.Thread(target = self.getAPI, daemon=True)
            thd.start()
            thdExec = threading.Thread(target = self.execOnBar, daemon=True)
            thdExec.start()
            self.loginTime = datetime.now()
            
    def insertMarketData(self, event):
        var = event.dict_
        self.listInstrumentInformation.append(var)
        if var['last']:
            downLogProgram('查询主力合约是否变化完成')
            ret = pd.DataFrame(self.listInstrumentInformation)
            ret = ret.set_index('InstrumentID')
            for goodsIcon in dictGoodsChg.keys():
                dfTemp = ret.loc[ret['ProductID'] == goodsIcon]
                dfTemp = dfTemp.sort_values(by='OpenInterest',ascending= False)
                if dfTemp.shape[0] == 0:
                    continue
                if dfTemp.index[0] not in listInstrument:
                    downLogProgram('切换主力合约为 {} '.format(dfTemp.index[0]))
                    nowTime = datetime.now().time()
                    instrument = dictGoodsInstrument[goodsIcon + '.' + dictGoodsChg[goodsIcon]]
                    if nowTime > time(14, 59) and nowTime < time(15):  # 只需要日盘临近收盘时切换即可。
                        downLogProgram("因为切换了主力合约，进行平仓操作")
                        # 查看是否有该品种的持仓
                        with lockDictFreqPosition:
                            dictFreqPositionTemp = dictFreqPosition.copy()
                        for freq in listFreq:
                            dfFreqPosition = dictFreqPositionTemp[freq].copy()
                            if instrument in dfFreqPosition['代码'].tolist():
                                orderEvent = Event(type_=EVENT_MARKETORDER)
                                orderEvent.dict_['instrument'] = instrument
                                orderEvent.dict_['freq'] = freq
                                orderEvent.dict_['num'] = dfFreqPosition['数量'][dfFreqPosition['代码'] == instrument].iat[0]
                                self.marketOrder(orderEvent)
            self.listInstrumentInformation = []
            
    def marketOrder(self, event):  # 调用这个方法能够进行市价平仓操作，通过 频段 与 合约名 进行筛选判断。
        freq = event.dict_['freq']
        instrument = event.dict_['instrument']
        goodsCode = getGoodsCode(instrument)
        indexGoods = listGoods.index(goodsCode)
        num = event.dict_['num']
        now = datetime.now()
        nowTime = now.time()
        # 获取 indexBar 即为 Bar 的索引
        for i in range(len(dictFreqGoodsClose[freq][goodsCode])):
            if i == len(dictFreqGoodsClose[freq][goodsCode]) - 1:
                iN = 0
            else:
                iN = i + 1
            if dictFreqGoodsClose[freq][goodsCode][i] < dictFreqGoodsClose[freq][goodsCode][iN]:
                if dictFreqGoodsClose[freq][goodsCode][i] <= nowTime < dictFreqGoodsClose[freq][goodsCode][iN]:
                    indexBar = i
                    break
            else:
                if dictFreqGoodsClose[freq][goodsCode][i] <= nowTime or nowTime < dictFreqGoodsClose[freq][goodsCode][iN]:
                    indexBar = i
                    break
        else:
            return
        # 计算下单编码
        orderRef = theTradeDay.strftime('%Y%m%d') + '.' + str(freq) + '.' + str(indexGoods) + '.' + str(indexBar) + '.' + '9'
        # 撤单操作
        preOrderRef = ''
        with lockDictFreqOrder:
            dfFreqOrderTemp = dictFreqOrder[freq].copy()
        dfFreqOrderTemp = dfFreqOrderTemp[~dfFreqOrderTemp['本地下单码'].duplicated(keep='last')].reset_index(drop=True)
        with lockDfCommand:
            dfCommandTemp = dfCommand.copy()
            dfCommandTemp = dfCommandTemp[dfCommandTemp['合约号'] == instrument]
        if dfFreqOrderTemp.shape[0] > 0:
            for index in dfFreqOrderTemp['本地下单码'][
                        pd.DataFrame(dfFreqOrderTemp['本地下单码'].str.split('.').tolist())[2] == str(indexGoods)].index:
                if dfFreqOrderTemp['状态'][index] not in ["全部成交", "全部成交报单已提交"] and dfFreqOrderTemp['状态'][index][
                                                                                 :3] != "已撤单":  # 撤单。
                    cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                    cancelEvent.dict_['orderref'] = dfFreqOrderTemp['本地下单码'][index]
                    preOrderRef = cancelEvent.dict_['orderref']
                    self.orderCancel(cancelEvent)
                    if preOrderRef[:-1] + '0' in dfCommandTemp['本地下单码'].tolist():  # 清除指令操作
                        index = dfCommandTemp[dfCommandTemp['本地下单码'] == (preOrderRef[:-1] + '0')].index[0]
                        orderDBEvent = Event(type_=EVENT_SHOWCOMMAND)
                        orderDBEvent.dict_['isChg'] = True
                        orderDBEvent.dict_['index'] = index
                        orderDBEvent.dict_['goods_code'] = instrument + '.'
                        downLogProgram('清除指令 {} '.format(preOrderRef[:-1] + '0'))
                        self.showCommand(orderDBEvent)
        orderEvent = Event(type_=EVENT_ORDERCOMMAND)
        orderEvent.dict_['InstrumentID'] = instrument
        if num < 0:
            orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
        else:
            orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
        orderEvent.dict_['orderref'] = orderRef
        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
        orderEvent.dict_['LimitPrice'] = 0
        orderEvent.dict_['VolumeTotalOriginal'] = abs(num)
        orderEvent.dict_['preOrderRef'] = preOrderRef
        self.orderCommand(orderEvent)
    # endregion

    # region 建立 socket 属性，并 执行第二引擎
    def getAPI(self):
        try:
            obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            obj.connect((host, port))
            while True:
                reply = obj.recv(131072 * 10)
                reply = reply.decode()
                self.strRecv += reply
                listReply = self.strRecv.split('}{')
                with lockDfOrder:
                    listTemp = dfOrder[dfOrder['状态'] == '未成交']['代码'].tolist()
                if len(listReply) > 1:
                    for eachNum in range(len(listReply)):
                        if eachNum == 0:
                            strReply = listReply[eachNum] + '}'
                            self.strRecv = self.strRecv[len(strReply):]
                            dictTemp = eval(strReply.replace("datetime.", ""))
                            instrument = dictTemp['goods_code'].split('.')[0]
                            if instrument in listTemp:
                                self.queueRecv.put((2, str(dictTemp)))
                            else:
                                self.queueRecv.put((3, str(dictTemp)))
                        elif eachNum == len(listReply) - 1:
                            strReply = '{' + listReply[eachNum]
                            if strReply[-1] == '}':
                                self.strRecv = self.strRecv[len(strReply):]
                                dictTemp = eval(strReply.replace("datetime.", ""))
                                instrument = dictTemp['goods_code'].split('.')[0]
                                if instrument in listTemp:
                                    self.queueRecv.put((2, str(dictTemp)))
                                else:
                                    self.queueRecv.put((3, str(dictTemp)))
                        else:
                            strReply = '{' + listReply[eachNum] + '}'
                            self.strRecv = self.strRecv[len(strReply):]
                            dictTemp = eval(strReply.replace("datetime.", ""))
                            instrument = dictTemp['goods_code'].split('.')[0]
                            if instrument in listTemp:
                                self.queueRecv.put((2, str(dictTemp)))
                            else:
                                self.queueRecv.put((3, str(dictTemp)))
                else:
                    self.strRecv = self.strRecv[len(listReply[0]):]
                    strReply = listReply[0]
                    dictTemp = eval(strReply.replace("datetime.", ""))
                    instrument = dictTemp['goods_code'].split('.')[0]
                    if instrument in listTemp:
                        self.queueRecv.put((2, str(dictTemp)))
                    else:
                        self.queueRecv.put((3, str(dictTemp)))
        except ConnectionRefusedError as err:
            downLogProgram(str(err))

    def execOnBar(self):
        # 执行每个频段每个品种的最后 onBar 操作
        while True:
            try:
                strReplyTemp = self.queueRecv.get(timeout=2)
                strReplyTemp = strReplyTemp[1]
                downLogTradeRecord(strReplyTemp)
                onBar(strReplyTemp)
                self.queueRecv.task_done()
            except Empty:
                pass
    # endregion

    # region qtimer 的事件处理
    def flushPosition(self):  # 确认所有的持仓单都会挂止盈单
        now = datetime.now()
        if self.loginTime != None and (now - self.loginTime) > timedelta(minutes=1):
            if not judgeExecTimer():
                return
            with lockDictFreqOrder:
                dictFreqOrderTemp = dictFreqOrder.copy()
            with lockDictFreqPosition:
                dictFreqPositionTemp = dictFreqPosition.copy()
            nowTime = now.time()
            for freq in listFreq:
                for instrument in dictFreqPositionTemp[freq]['代码'].tolist():
                    goodsCode = getGoodsCode(instrument)
                    if goodsCode in dictFreqUnGoodsCode[freq]:
                        downLogProgram("在CTA{} 中的 {} 不进行频段品种交易，需要手动平仓".format(freq, instrument))
                        continue
                    if judgeInTradeTime(goodsCode):  # 确认在 goodsCode 的交易时间内
                        indexGoods = listGoods.index(goodsCode)
                        for i in range(len(dictFreqGoodsClose[freq][goodsCode])):
                            if i == len(dictFreqGoodsClose[freq][goodsCode]) - 1:
                                iN = 0
                            else:
                                iN = i + 1
                            if dictFreqGoodsClose[freq][goodsCode][i] < dictFreqGoodsClose[freq][goodsCode][iN]:
                                if dictFreqGoodsClose[freq][goodsCode][i] <= nowTime < dictFreqGoodsClose[freq][goodsCode][iN]:
                                    indexBar = i
                                    break
                            else:
                                if dictFreqGoodsClose[freq][goodsCode][i] <= nowTime or nowTime < dictFreqGoodsClose[freq][goodsCode][iN]:
                                    indexBar = i
                                    break
                        else:
                            return
                        orderRef = theTradeDay.strftime('%Y%m%d') + '.' + str(freq) + '.' + str(indexGoods) + '.' + str(indexBar) + '.'
                        dfFreqOrder = dictFreqOrderTemp[freq].copy()
                        if (orderRef not in dfFreqOrder['本地下单码'].str[:-1].tolist()) and (orderRef + '2' not in listStopProfit):
                            listTemp = []  # 主要避免相同本地下单码重复下单
                            # 主要对之前编号进行撤单操作
                            preOrderRef = ''
                            if dfFreqOrder.shape[0] > 0:
                                for i in dfFreqOrder['本地下单码'][pd.DataFrame(dfFreqOrder['本地下单码'].str.split('.').tolist())[2] == str(indexGoods)].index:
                                    if dfFreqOrder['状态'][i] not in ["全部成交", "全部成交报单已提交"] and dfFreqOrder['状态'][i][:3] != "已撤单":
                                        if dfFreqOrder['本地下单码'][i] not in listTemp:
                                            listTemp.append(dfFreqOrder['本地下单码'][i])
                                            downLogBarDeal("品种：{} 之前的编号 {} 进行撤单操作".format(instrument, dfFreqOrder['本地下单码'][i]), freq)
                                            cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                                            cancelEvent.dict_['orderref'] = dfFreqOrder['本地下单码'][i]
                                            preOrderRef = dfFreqOrder['本地下单码'][i]
                                            ee.put(cancelEvent)
                            getOrder(freq, goodsCode, orderRef, preOrderRef, True)

            # 到点需要测试周交易明细表
            if self.checkInstrumentChg and time(14, 59) <= now.time() <= time(15):
                self.checkInstrumentChg = False
                self.td.t.ReqQryDepthMarketData()

    def showPosition(self):  # 查看持仓记录
        if self.loginTime != None:
            if not judgeExecTimer():  # 执行的时间
                return
            if not judgeInTradeTimeTotal():
                return
            with lockDictFreqPosition:
                dictFreqPositionTemp = dictFreqPosition.copy()
            # 建立显示的 dataFrame
            dfShow = pd.DataFrame(columns=['合约(周)', '频段(周)', '开仓时间(周)', '合约(持)', '频段(持)', '开仓时间(持)'])
            print("持仓显示")
            for freq in listFreq:
                dfFreqPosition = dictFreqPositionTemp[freq]  # 实际的持仓情况
                for goodsCode in dictGoodsName.keys():
                    if goodsCode in dictFreqUnGoodsCode[freq]:
                        continue
                    goodsName = dictGoodsName[goodsCode]
                    theWeek = dictData[freq][goodsName + '_周交易明细表'].iloc[-1]  # 周交易明细表
                    openTime = theWeek['开仓时间']
                    if theWeek['开平仓标识多'] == 1:
                        dictTemp = {}
                        dictTemp['合约(周)'] = theWeek['交易合约号']
                        dictTemp['频段(周)'] = freq
                        dictTemp['开仓时间(周)'] = openTime
                        if goodsName in dfFreqPosition['名称'].tolist():
                            index = dfFreqPosition['名称'].tolist().index(goodsName)
                            dictTemp['合约(持)'] = dfFreqPosition['代码'][index]
                            dictTemp['频段(持)'] = freq
                            dictTemp['开仓时间(持)'] = dfFreqPosition['时间'][index]
                            dfShow.loc[dfShow.shape[0]] = dictTemp
                        else:
                            dictTemp['合约(持)'] = None
                            dictTemp['频段(持)'] = None
                            dictTemp['开仓时间(持)'] = None
                            dfShow.loc[dfShow.shape[0]] = dictTemp
                    elif theWeek['开平仓标识空'] == 1:
                        dictTemp = {}
                        dictTemp['合约(周)'] = theWeek['交易合约号']
                        dictTemp['频段(周)'] = freq
                        dictTemp['开仓时间(周)'] = openTime
                        if goodsName in dfFreqPosition['名称'].tolist():
                            index = dfFreqPosition['名称'].tolist().index(goodsName)
                            dictTemp['合约(持)'] = dfFreqPosition['代码'][index]
                            dictTemp['频段(持)'] = freq
                            dictTemp['开仓时间(持)'] = dfFreqPosition['时间'][index]
                            dfShow.loc[dfShow.shape[0]] = dictTemp
                        else:
                            dictTemp['合约(持)'] = None
                            dictTemp['频段(持)'] = None
                            dictTemp['开仓时间(持)'] = None
                            dfShow.loc[dfShow.shape[0]] = dictTemp
                    elif goodsName in dfFreqPosition['名称'].tolist():
                        index = dfFreqPosition['名称'].tolist().index(goodsName)
                        dictTemp = {}
                        dictTemp['合约(周)'] = None
                        dictTemp['频段(周)'] = None
                        dictTemp['开仓时间(周)'] = None
                        dictTemp['合约(持)'] = dfFreqPosition['代码'][index]
                        dictTemp['频段(持)'] = freq
                        dictTemp['开仓时间(持)'] = dfFreqPosition['时间'][index]
                        dfShow.loc[dfShow.shape[0]] = dictTemp
            print(dfShow)
            # 交易指令显示
            print('指令显示')
            with lockDfCommand:
                dfCommandTemp = dfCommand.copy()
            dfCommandTemp = dfCommandTemp[~dfCommandTemp['合约号'].str.endswith('.')]  # 我需要获取有效的指令
            dfCommandTemp = dfCommandTemp[['发单时间', '本地下单码', '合约号', '应开多手数', '应开空手数']]
            print(dfCommandTemp)
    # endregion

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ui = RdMd()
    sys.exit(app.exec_())

