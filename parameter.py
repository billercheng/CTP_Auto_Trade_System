from datetime import *
import pandas as pd
from sqlalchemy import *
from py_ctp.ctp_struct import *
from py_ctp.eventEngine import *
from py_ctp.eventType import *
import logging
import os
import numpy as np
import http.client
import time as ttt
import threading
import pymongo

# region 基本处理方法
def getWebServerTime(host):  # 启动时自动修正本地时间
    conn = http.client.HTTPConnection(host)
    conn.request("GET", "/")
    r = conn.getresponse()
    ts = r.getheader('date')  # 获取http头date部分
    # 将GMT时间转换成北京时间
    ltime = datetime.strptime(ts[5:25], "%d %b %Y %H:%M:%S") + timedelta(hours=8, seconds=1.2)
    dat = "date %u-%02u-%02u" % (ltime.year, ltime.month, ltime.day)
    tm = "time %02u:%02u:%02u" % (ltime.hour, ltime.minute, ltime.second)
    os.system(dat)
    os.system(tm)

def judgeCodeValue(code, dt):
    listCode = code.split('.')
    theDate = pd.to_datetime(listCode[0])
    theDateIndex = tradeDatetime.index(theDate)
    freq = int(listCode[1])
    if freq not in listFreq:
        return False
    goodsCode = listGoods[int(listCode[2])]
    indexBar = int(listCode[3])
    # start = dictFreqGoodsClose[freq][goodsCode][indexBar]
    end = dictFreqGoodsClose[freq][goodsCode][indexBar + 1 if indexBar + 1 != len(dictFreqGoodsClose[freq][goodsCode]) else 0]
    # if start > time(20) or start < time(3) or (start == dictFreqGoodsClose[freq][goodsCode][-1]):
    #     if start > time(20) or start == dictFreqGoodsClose[freq][goodsCode][-1]:
    #         startTime = tradeDatetime[theDateIndex - 1] + timedelta(hours=start.hour, minutes=start.minute)
    #     elif start < time(3):
    #         startTime = tradeDatetime[theDateIndex - 1] + timedelta(hours=start.hour, minutes=start.minute) + timedelta(days=1)
    # else:
    #     startTime = theDate + timedelta(hours=start.hour, minutes=start.minute)
    if end > time(20) or end < time(3):
        if end > time(20):
            endTime = tradeDatetime[theDateIndex - 1] + timedelta(hours=end.hour, minutes=end.minute)
        elif end < time(3):
            endTime = tradeDatetime[theDateIndex - 1] + timedelta(hours=end.hour, minutes=end.minute) + timedelta(days=1)
    else:
        endTime = theDate + timedelta(hours=end.hour, minutes=end.minute)
    if dt >= endTime:  # 你就不能想一想等于的情况吗？ 等于 的话，当然是不应该开仓的吧
        return False
    else:
        return True

def readMongo(name, db):  # 读取 mongodb， 的数据库
    cursor = db[name].find()  # 读取 mongodb， 因为一个软件只使用一个数据库吧
    df = pd.DataFrame(list(cursor))
    df.drop(['_id'], axis=1, inplace = True)
    return df

def downLogProgram(log):
    event = Event(type_=EVENT_LOGProgram)
    event.dict_['log'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "......" + str(log)
    ee.put(event)

def downLogBarDeal(log):
    event = Event(type_=EVENT_LOGLogBarDeal)
    event.dict_['log'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "......" + str(log)
    ee.put(event)

def downLogTradeRecord(log):
    event = Event(type_=EVENT_LOGTradeRecord)
    event.dict_['log'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "......" + str(log)
    ee.put(event)

def readMongoNum(db, name, num):  # 读取 mongodb， 的数据库
    cursor = db[name].find(limit = num, sort = [("trade_time", pymongo.DESCENDING)])  # 读取 mongodb， 因为一个软件只使用一个数据库吧
    df = pd.DataFrame(list(cursor))
    df.drop(['_id'], axis=1, inplace = True)
    return df

def getGoodsCode(instrument):  # 从 品种合约 到 品种名称
    if instrument[-4:].isdigit():
        goodsCode = instrument[:-4] + '.' + dictGoodsChg[instrument[:-4]]
    else:
        goodsCode = instrument[:-3] + '.' + dictGoodsChg[instrument[:-3]]
    return goodsCode

def getLoseData(goodsCode, freq, startTime, endTime):  # 得到理论上，我们在这个时间范围上应该获取的数据， 包括 startTime， 不包括 endTime
    seriesTradeDay = tradeDatetime.copy()
    theStartTime = startTime.strftime('%Y-%m-%d')
    theEndtime = endTime.strftime('%Y-%m-%d')
    if startTime.isoweekday() == 6:
        listTemp = [seriesTradeDay[(seriesTradeDay <= pd.to_datetime(theStartTime))].iat[-1]]
        listTemp.extend(seriesTradeDay[(seriesTradeDay >= pd.to_datetime(theStartTime))
                                        & (seriesTradeDay <= pd.to_datetime(theEndtime))].tolist())
        seriesTradeDay = listTemp
    else:
        seriesTradeDay = seriesTradeDay[(seriesTradeDay >= pd.to_datetime(theStartTime))
                                        & (seriesTradeDay <= pd.to_datetime(theEndtime))].tolist()
    listTradeTime = []
    for eachDay in seriesTradeDay:
        if eachDay not in listHolidayDatetime:
            listTradeTime.extend(list(map(lambda x:eachDay + timedelta(days = 1 if x.hour in [0, 1, 2] else 0, hours=x.hour, minutes=x.minute), dictFreqGoodsCloseNight[freq][goodsCode])))
        else:
            listTradeTime.extend(list(map(lambda x:eachDay + timedelta(days = 1 if x.hour in [0, 1, 2] else 0, hours=x.hour, minutes=x.minute),
                                          dictFreqGoodsCloseNight[freq][goodsCode][:dictFreqGoodsCloseNight[freq][goodsCode].index(dictFreqGoodsClose[1][goodsCode][-1]) + 1])))
    seriesTradeTime = pd.Series(listTradeTime).sort_values()
    seriesTradeTime = seriesTradeTime[(seriesTradeTime > startTime) & (seriesTradeTime <= endTime)].reset_index(drop = True)
    return seriesTradeTime
# endregion

# 更改本地时间
getWebServerTime('www.baidu.com')

# 建立主引擎
ee = EventEngine()

# region 读取帐号的基本信息
dictLoginInformation = {}
listFreq = []
defaultFreqSet = []
with open('RD files\\LoginInformation.txt', 'r', encoding='UTF-8') as f:
    for row in f:
        if 'userid' in row:
            dictLoginInformation['userid'] = row.split('：')[1].strip()
        if 'password' in row:
            dictLoginInformation['password'] = row.split('：')[1].strip()
        if 'broker' in row:
            dictLoginInformation['broker'] = row.split('：')[1].strip()
        if 'front_addr' in row:
            dictLoginInformation['front_addr'] = row.split('：')[1].strip()
        if 'product_info' in row:
            dictLoginInformation['product_info'] = row.split('：')[1].strip()
        if 'app_id' in row:
            dictLoginInformation['app_id'] = row.split('：')[1].strip()
        if 'auth_code' in row:
            dictLoginInformation['auth_code'] = row.split('：')[1].strip()
        if 'listFreq' in row:
            strTemp = row.split('：')[1].strip()[1:-1]
            for each in strTemp.split(','):
                listFreq.append(int(each))
        if 'programName' in row:
            programName = row.split('：')[1].strip()
        if 'defaultFreqSet' in row:
            strTemp = row.split('：')[1].strip()[1:-1]
            for each in strTemp.split(','):
                each = each.strip()
                if each in ['True', 'False']:
                    defaultFreqSet.append(bool(each))
                else:
                    defaultFreqSet.append(each)
listFreqPlus = listFreq.copy()
listFreqPlus.insert(0, 1)
# endregion

# 读取公共参数
GoodsTab = pd.read_excel('RD files\\公共参数.xlsx', sheet_name='品种信息', index_col='品种名称')
dfCapital = pd.read_excel('RD files\\公共参数.xlsx', sheet_name='账户资金表') # 读取帐号资金量
dictCloseTimeClose = pd.read_pickle('pickle\\dictCloseTimeClose.pkl')  # 时间区间
dictCloseTimeCloseNight = pd.read_pickle('pickle\\dictCloseTimeCloseNight.pkl')  # 时间区间
dictGoodsName = {}
listGoods = []
dictGoodsChg = {}
dictGoodsCheng = {}
dictGoodsFirst = {}
dictGoodsUnit = {}
dictFreqGoodsClose = {}  # 夜盘开始，15：00结束
dictFreqGoodsCloseNight = {}  # 日盘开始，夜盘交易时间结束
dictGoodsSend = {}  # 发送记录时间
dictGoodsLast = {}  # 夜盘收盘时间
dictGoodsLastWord = {}  # 夜盘收盘时间中文显示
for num in range(GoodsTab.shape[0]):
    goodsCode = GoodsTab['品种代码'][num]
    dictGoodsName[goodsCode] = GoodsTab.index[num]
    dictGoodsCheng[goodsCode] = GoodsTab['合约乘数'][num]
    dictGoodsUnit[goodsCode] = GoodsTab['最小变动单位'][num]
    dictGoodsChg[goodsCode.split('.')[0]] = goodsCode.split('.')[1]
    dictGoodsFirst[goodsCode] = True
    dictGoodsLastWord[goodsCode] = GoodsTab['交易时间类型'][num]
    listGoods.append(goodsCode)
    if goodsCode.split('.')[1] != "CFE":
        dictGoodsSend[goodsCode] = [time(10, 15), time(11, 30), time(15)]
    else:
        dictGoodsSend[goodsCode] = [time(11, 30), time(15, 15)]
    if dictGoodsLastWord[goodsCode] == '23.00收盘':
        dictGoodsSend[goodsCode].append(time(23))
    elif dictGoodsLastWord[goodsCode] == '23.30收盘':
        dictGoodsSend[goodsCode].append(time(23, 30))
    elif dictGoodsLastWord[goodsCode] == '1.00收盘':
        dictGoodsSend[goodsCode].append(time(1))
    elif dictGoodsLastWord[goodsCode] == '2.30收盘':
        dictGoodsSend[goodsCode].append(time(2, 30))
for freq in listFreqPlus:
    dictFreqGoodsClose[freq] = {}
    dictFreqGoodsCloseNight[freq] = {}
    for goodsCode in listGoods:
        if dictGoodsLastWord[goodsCode] == '15.00收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(15)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(15)]
        elif dictGoodsLastWord[goodsCode] == '15.15收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(15, 15)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(15, 15)]
        elif dictGoodsLastWord[goodsCode] == '23.00收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(23)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(23)]
        elif dictGoodsLastWord[goodsCode] == '23.30收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(23, 30)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(23, 30)]
        elif dictGoodsLastWord[goodsCode] == '1.00收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(1)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(1)]
        elif dictGoodsLastWord[goodsCode] == '2.30收盘':
            dictFreqGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(2, 30)]
            dictFreqGoodsCloseNight[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(2, 30)]
        if freq == 1:
            dictGoodsLast[goodsCode] = dictFreqGoodsCloseNight[1][goodsCode][-1]

# 交易参数综合表
ParTab = {}
for freq in listFreq:
    ParTab[freq] = pd.read_excel('RD files\\CTA交易参数表-综合版.xlsx',
                                 sheet_name='CTA{}'.format(freq)).set_index('品种名称')
# 将不交易的品种写出来吧
dictFreqUnGoodsCode = {}
setTheGoodsCode = set()  # 这些是交易的品种
for freq in listFreqPlus:
    dictFreqUnGoodsCode[freq] = []
    if freq != 1:
        for goodsCode in dictGoodsName.keys():
            DayTradeEnable = ParTab[freq]["日盘交易标识"][dictGoodsName[goodsCode]]  # 日盘交易标识
            NightTradeEnable = ParTab[freq]["夜盘交易标识"][dictGoodsName[goodsCode]]  # 夜盘交易标识
            if DayTradeEnable == 0 and NightTradeEnable == 0:
                dictFreqUnGoodsCode[freq].append(goodsCode)
            else:
                setTheGoodsCode.add(goodsCode)

# 记录品种数值
dictGoodsAdj = {}  # 记录主力合约表
dictGoodsInstrument = {}  # 记录主力合约操作
listInstrument = []  # 主力合约的数列
dictInstrumentUpDownPrice = {}  # 合约的止盈与止损价格
dictInstrumentPrice = {}  # 从tick数据上接收到最新价格

# 创建 mongodb 数据库，使用 Freq 与 建立数据库
dictFreqDb = {}
dictFreqDoc = {}  # 频段 对应 数据库中表格
for freq in listFreq:
    mongodbName = 'CTA{}_'.format(freq) + programName  # 数据库名称使用 CTA{}_测1
    con = pymongo.MongoClient("mongodb://localhost:27017/")  # 建立连接
    dictFreqDb[freq] = con[mongodbName]  # 建立 runoobdb 数据库连接
    dictFreqDoc[freq] = dictFreqDb[freq].list_collection_names()  # 数据库对应表格名称

# 持仓量
listFreqPosition = ['代码', '名称', '方向', '数量', '时间', '价格', '持仓盈亏']
dictFreqPosition = {}
dictFreqPositionCon = {}
for freq in listFreq:
    if '频段持仓' in dictFreqDoc[freq]:
        dictFreqPosition[freq] = readMongo('频段持仓', dictFreqDb[freq])
        if dictFreqPosition[freq].shape[0] == 0:
            dictFreqPosition[freq] = pd.DataFrame(columns=listFreqPosition)
    else:
        dictFreqPosition[freq] = pd.DataFrame(columns=listFreqPosition)
    dictFreqPositionCon[freq] = dictFreqDb[freq]['频段持仓']
# 委托单
listOrderSourceColumns = []
for each in CThostFtdcOrderField._fields_:
    listOrderSourceColumns.append(each[0])
listTradeSourceColumns = []
for each in CThostFtdcTradeField._fields_:
    listTradeSourceColumns.append(each[0])
listFreqOrder = ['本地下单码', '时间', '代码', '方向', '价格', '数量', '状态', '已成交', '成交均价', '拒绝原因']
dfOrder = pd.DataFrame(columns=listFreqOrder)  # 所有的委托单
dfOrderSource = pd.DataFrame(columns=listOrderSourceColumns)  # 所有的委托单（原始的）
dictFreqOrder = {}
dictFreqOrderSource = {}
dictFreqOrderCon = {}
for freq in listFreq:
    # 建立表格
    dictFreqOrder[freq] = pd.DataFrame(columns=listFreqOrder)
    dictFreqOrderSource[freq] = pd.DataFrame(columns=listOrderSourceColumns)
    # 建立连接
    db = dictFreqDb[freq]
    if '委托记录' in dictFreqDoc[freq]:
        temp = db['委托记录']
        temp.drop()
    dictFreqOrderCon[freq] = db['委托记录']
# 成交单
listFreqTrade = ['本地下单码', '时间', '代码', '名称', '方向', '价格', '数量']
dictFreqTrade = {}
dictFreqTradeSource = {}
dictFreqTradeCon = {}
for freq in listFreq:
    # 建立表格
    dictFreqTrade[freq] = pd.DataFrame(columns=listFreqTrade)
    dictFreqTradeSource[freq] = pd.DataFrame(columns=listTradeSourceColumns)
    # 建立连接
    db = dictFreqDb[freq]
    if '成交记录' in dictFreqDoc[freq]:
        temp = db['成交记录']
        temp.drop()
    dictFreqTradeCon[freq] = db['成交记录']
# 错误单
listFreqError = ['本地下单码', '时间', '代码', '名称', '方向', '价格', '数量']
dictFreqError = {}
dictFreqErrorCon = {}
for freq in listFreq:
    # 建立表格
    dictFreqError[freq] = pd.DataFrame(columns=listFreqError)
    # 建立连接
    db = dictFreqDb[freq]
    if '错误记录' in dictFreqDoc[freq]:
        temp = db['错误记录']
        temp.drop()
    dictFreqErrorCon[freq] = db['错误记录']

# 建立线程锁
lockDfOrder = threading.Lock()
lockDictFreqOrder = threading.Lock()
lockDictFreqPosition = threading.Lock()  # 指令的持仓情况
lockDfOrderDB = threading.Lock()

# 交易日
dfDatetime = pd.read_csv('RD files\\tradeDay.csv', parse_dates=['tradeDatetime'])
tradeDatetime = dfDatetime['tradeDatetime'].tolist()
listHolidayDatetime = dfDatetime[dfDatetime['holiday'] == 1]['tradeDatetime'].dt.date.tolist()
tradeDate = pd.Series(tradeDatetime).dt.date
now = datetime.now()
s = dfDatetime['tradeDatetime'].copy()
theTradeDay = s[s >= now - timedelta(hours=17, minutes=15)].iat[0]  # 获取当前的交易日名称

# 全部指令单
dfOrderDB = pd.read_pickle('pickle\\dfOrderDB.pkl')
dfOrderDBDrop = dfOrderDB[0:0].copy()
dfOrderDB = dfOrderDB.reset_index(drop=True)
for num in range(dfOrderDB.shape[0]):
    if '.' not in dfOrderDB['合约号'][num]:
        if not judgeCodeValue(dfOrderDB['本地下单码'][num], now):
            dfOrderDBDrop.loc[dfOrderDBDrop.shape[0]] = dfOrderDB.loc[num]
            dfOrderDB.drop([num], inplace=True)
    else:
        dfOrderDBDrop.loc[dfOrderDBDrop.shape[0]] = dfOrderDB.loc[num]
        dfOrderDB.drop([num], inplace = True)
dfOrderDB = dfOrderDB.reset_index(drop=True)
dfOrderDB.to_pickle('pickle\\dfOrderDB.pkl')

# 关于dictOrderRef保存记录
listTradeID = pd.read_pickle('pickle\\listTradeID.pkl')
dictOrderRef = pd.read_pickle('pickle\\dictOrderRef.pkl')
dictRefOrder = pd.read_pickle('pickle\\dictRefOrder.pkl')
s = dfDatetime['tradeDatetime'].copy()
s = s[s <= now].iloc[-2:]
listDayTemp = [theTradeDay.strftime('%m%d')]
for each in list(dictOrderRef.keys()):
    if dictOrderRef[each][4:8] not in listDayTemp:
        dictOrderRef.pop(each)
for each in list(dictRefOrder.keys()):
    if each not in dictOrderRef.values():
        dictRefOrder.pop(each)
for each in list(listTradeID):
    if each[4:8] not in listDayTemp:
        listTradeID.remove(each)
dictPreOrderRefOrder = {}  # 建立监测到撤单后，才进行下单的情况
pd.to_pickle(dictOrderRef, 'pickle\\dictOrderRef.pkl')
pd.to_pickle(dictRefOrder, 'pickle\\dictRefOrder.pkl')
pd.to_pickle(listTradeID, 'pickle\\listTradeID.pkl')

# 均值，重叠度，周交易明细表基本定义
mvlenvector = [80, 100, 120, 140, 160, 180, 200, 220, 240, 260]
listDrop = ['id']  # 删除重叠度的某些列
for mvl in mvlenvector:
    listDrop.extend(
        ['StdMux高均值_{}'.format(mvl), 'StdMux低均值_{}'.format(mvl), 'StdMux收均值_{}'.format(mvl),
         '重叠度高收益_{}'.format(mvl), '重叠度低收益_{}'.format(mvl), '重叠度收收益_{}'.format(mvl)])
MaxLossPerCTA = 0.001  # 最大回撤阈值
StdMuxMinValue = 1  # 开平仓线时，开仓倍数的比较值
StopAbtainInBarMux = 2
StopLossInBarMux = 2
InBarCloseAtNMuxFlag = "1"
InBarStopLossFlag = "1"
PricUnreachableHighPrice = 999999  # 下单时，保证价格无效的最大价格
PricUnreachableLowPrice = -1  # 下单时，保证价格无效的最大价格
dictData = {}  # 数据储存的字典
listMin = ['goods_code', 'goods_name', 'open', 'high', 'low', 'close', 'volume', 'amt', 'oi']
listAdjust = ['goods_code', 'goods_name', 'adjdate', 'adjinterval']
listMa = ['goods_code', 'goods_name', 'open', 'high', 'low', 'close']
for vector in mvlenvector:
    listMa.extend(['maprice_{}'.format(vector), 'stdprice_{}'.format(vector), 'stdmux_{}'.format(vector), 'highstdmux_{}'.format(vector), 'lowstdmux_{}'.format(vector)])
listOverLap = ['goods_code', 'goods_name', 'open', 'high', 'low', 'close']
for vector in mvlenvector:
    listOverLap.extend(['重叠度高_{}'.format(vector), '重叠度低_{}'.format(vector), '重叠度收_{}'.format(vector)])

# socket 通迅
host = '192.168.1.121'
port = 8080

# 本周的数据
now = datetime.now()
DfWeek = pd.read_excel('RD files\\公共参数.xlsx', sheet_name='周时间序列表')
DfWeek['起始时间'] = DfWeek['起始时间'] + timedelta(hours=6)
DfWeek['结束时间'] = DfWeek['结束时间'] + timedelta(hours=8)
for num in range(DfWeek.shape[0]):
    if DfWeek['起始时间'][num] <= now:
        week = DfWeek['周次'][num]
        weekStartTime = DfWeek['起始时间'][num]
        weekEndTime = DfWeek['结束时间'][num]
thisWeekDay = tradeDate[(tradeDate >= weekStartTime.date()) & (tradeDate <= weekEndTime.date())]
weekLastDay = thisWeekDay.iat[-1]

# 数据源的连接（从哪里获取分钟数据）
dataSourseIp = 'localhost'
con = pymongo.MongoClient("mongodb://{}:27017/".format(dataSourseIp))  # 建立连接


# 下单日志
loggingPath = 'log\\{}'.format(theTradeDay.strftime('%Y-%m-%d'))
os.makedirs(loggingPath, exist_ok=True)
logRuida = logging.getLogger("ruida")
fileHandle = logging.FileHandler(loggingPath + '\\ruida.txt')
fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logRuida.addHandler(fileHandle)
logRuida.setLevel(logging.INFO)
os.makedirs(loggingPath + '\\tick', exist_ok=True)
logTick = logging.getLogger('tick')
fileHandle = logging.FileHandler(loggingPath + '\\tick\\{}.txt'.format(theTradeDay.strftime('%Y-%m-%d')))
fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logTick.addHandler(fileHandle)
logTick.setLevel(logging.INFO)
dictFreqLog = {}
for freq in listFreq:
    theLog = logging.getLogger('CTA{}'.format(freq))
    fileHandle = logging.FileHandler(loggingPath + '\\CTA{}.txt'.format(freq))
    fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    theLog.addHandler(fileHandle)
    theLog.setLevel(logging.INFO)
    dictFreqLog[freq] = theLog  # 对应的分钟处理方法

# 将易的品种写出来吧
dictFreqUnGoodsCode = {}
setUnGoodsCode = set()
for freq in listFreqPlus:
    dictFreqUnGoodsCode[freq] = []
    if freq != 1:
        for goodsCode in dictGoodsName.keys():
            DayTradeEnable = ParTab[freq]["日盘交易标识"][dictGoodsName[goodsCode]]  # 日盘与夜盘开仓的情况，真的很重要的吧
            NightTradeEnable = ParTab[freq]["夜盘交易标识"][dictGoodsName[goodsCode]]
            if DayTradeEnable == 0 and NightTradeEnable == 0:
                dictFreqUnGoodsCode[freq].append(goodsCode)
            else:
                setUnGoodsCode.add(goodsCode)

# 从 mongodb 上获取数据库写入内存













































