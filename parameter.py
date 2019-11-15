from datetime import *
import pandas as pd
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
def getWebServerTime(host):  # 启动时通过 web 修改本地时间
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

def judgeCodeValue(code, dt):  # 本地下单码的有效时间，与本地时间进行比较
    listCode = code.split('.')
    theDate = pd.to_datetime(listCode[0])
    listTradeDatetime = tradeDatetime.tolist()
    theDateIndex = listTradeDatetime.index(theDate)
    freq = int(listCode[1])
    if freq not in listFreq:
        return False
    goodsCode = listGoods[int(listCode[2])]
    indexBar = int(listCode[3])
    end = dictFreqGoodsClose[freq][goodsCode][indexBar + 1 if indexBar + 1 != len(dictFreqGoodsClose[freq][goodsCode]) else 0]
    if end > time(20) or end < time(3):
        if end > time(20):
            endTime = listTradeDatetime[theDateIndex - 1] + timedelta(hours=end.hour, minutes=end.minute)
        elif end < time(3):
            endTime = listTradeDatetime[theDateIndex - 1] + timedelta(hours=end.hour, minutes=end.minute) + timedelta(days=1)
    else:
        endTime = theDate + timedelta(hours=end.hour, minutes=end.minute)
    if dt >= endTime:
        return False
    else:
        return True

def readMongo(name, db):  # 读取 db 数据库， 表名为 name ， 的所有数据
    cursor = db[name].find()  # 读取 mongodb， 因为一个软件只使用一个数据库吧
    df = pd.DataFrame(list(cursor))
    if df.shape[0] > 0:
        df.drop(['_id'], axis=1, inplace = True)
    return df

def readMongoNum(db, name, num):  # 读取 db 数据库， 表名为 name ，的后 num 条数据
    cursor = db[name].find(limit = num, sort = [("trade_time", pymongo.DESCENDING)])
    df = pd.DataFrame(list(cursor))
    if df.shape[0] > 0:
        df.drop(['_id'], axis=1, inplace = True)
    return df

def insertDbChg(dict):  # 当数据插入到 mongodb 时，字典的数据需要更改数据类型
    for each in dict.keys():
        if isinstance(dict[each], np.int64):
            dict[each] = int(dict[each])
        elif isinstance(dict[each], np.int32):
            dict[each] = int(dict[each])
        elif isinstance(dict[each], np.float64):
            dict[each] = float(dict[each])
    return dict

def downLogProgram(log):
    print(str(log))
    logProgram.info(str(log))

def downLogBarDeal(log, freq):
    dictFreqLog[freq].info(str(log))

def downLogTradeRecord(log):  # 记录  Tick 数据上处理， 和 order , trade , error 的数据
    logTradeRecord.info(str(log))

def getGoodsCode(instrument):  # 从 品种合约 到 品种代码
    if instrument[-4:].isdigit():
        goodsCode = instrument[:-4] + '.' + dictGoodsChg[instrument[:-4]]
    else:
        goodsCode = instrument[:-3] + '.' + dictGoodsChg[instrument[:-3]]
    return goodsCode

def getLoseData(goodsCode, freq, startTime, endTime):  # 得到理论上，我们在这个时间范围上应该获取的数据， 不包括 startTime， 包括 endTime
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

def getNextOrderDatetime(goodsCode, dt):  # 获取 交易时间内的 下一分钟开始，比如 2019-11-5 11：30：00 下一分钟为 2019-11-5 13：30：00
    tempClose = dictFreqGoodsClose[1][goodsCode]
    nextTime = tempClose[((tempClose.index(dt.time()) + 1) % len(tempClose))]
    if dt.time() != dictGoodsSend[goodsCode][-1]:
        return datetime(dt.year, dt.month, dt.day, nextTime.hour, nextTime.minute) - timedelta(minutes=1)
    else:
        tempDatetime = tradeDatetime[tradeDatetime >= (dt - timedelta(hours=3))].iat[0]
        return datetime(tempDatetime.year, tempDatetime.month, tempDatetime.day, nextTime.hour, nextTime.minute) - timedelta(minutes=1)

def getNextOrderDatetimeLast(goodsCode, dt, freq):  # 获取交易时间内 下一个Bar的结束时间
    tempClose = dictFreqGoodsClose[freq][goodsCode]
    nextTime = tempClose[((tempClose.index(dt.time()) + 1) % len(tempClose))]
    if dt.time() != dictGoodsSend[goodsCode][-1]:
        return datetime(dt.year, dt.month, dt.day, nextTime.hour, nextTime.minute)
    else:
        tempDatetime = tradeDatetime[tradeDatetime >= (dt - timedelta(hours=3))].iat[0]
        return datetime(tempDatetime.year, tempDatetime.month, tempDatetime.day, nextTime.hour, nextTime.minute)

def changePriceLine(price, MinChangUnit, DuoOrKong, OpenOrClose):  # 将价格取整操作
    if DuoOrKong == '多':
        if OpenOrClose in ['止盈', '开仓']:
            return round(price * (1 / MinChangUnit)) * MinChangUnit if round(
                price * (1 / MinChangUnit)) * MinChangUnit >= price else round(
                price * (1 / MinChangUnit)) * MinChangUnit + MinChangUnit
        else:
            return round(price * (1 / MinChangUnit)) * MinChangUnit if round(
                price * (1 / MinChangUnit)) * MinChangUnit <= price else round(
                price * (1 / MinChangUnit)) * MinChangUnit - MinChangUnit
    else:
        if OpenOrClose in ['止盈', '开仓']:
            return round(price * (1 / MinChangUnit)) * MinChangUnit if round(
                price * (1 / MinChangUnit)) * MinChangUnit <= price else round(
                price * (1 / MinChangUnit)) * MinChangUnit - MinChangUnit
        else:
            return round(price * (1 / MinChangUnit)) * MinChangUnit if round(
                price * (1 / MinChangUnit)) * MinChangUnit >= price else round(
                price * (1 / MinChangUnit)) * MinChangUnit + MinChangUnit

def judgeExecTimer():  # qtimer 的执行时间，不应该在切割点上执行
    now = datetime.now()
    if now.second > 57 or now.second < 20:
        return False
    else:
        return True

def judgeInTradeTime(goodsCode):  # 判断当前时间是否在 goodsCode 的交易时间内
    now = datetime.now()
    if (now - timedelta(hours=3)).date() in tradeDate.tolist():
        now += timedelta(minutes=1)
        nowTime = time(now.hour, now.minute)
        if nowTime not in dictFreqGoodsClose[1][goodsCode]:
            return False
        else:
            return True
    else:
        return False
# endregion

# 更改本地时间
getWebServerTime('www.baidu.com')

# 建立主引擎
ee = EventEngine()

# region 读取帐号的基本信息
dictLoginInformation = {}
listFreq = []
defaultFreqSet = []
staticMaxVolume = 10  # 最大的开仓次数
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
        if 'uniformCode' in row:
            uniformCode = row.split('：')[1].strip()
        if 'staticMaxVolume' in row:
            staticMaxVolume = int(row.split('：')[1].strip())
listFreqPlus = listFreq.copy()
listFreqPlus.insert(0, 1)
dictFreqSet = {}
for freq in listFreq:
    dictFreqSet[freq] = defaultFreqSet
# endregion

# 读取公共参数
GoodsTab = pd.read_excel('RD files\\公共参数.xlsx', sheet_name='品种信息', index_col='品种名称')
dfCapital = pd.read_excel('RD files\\公共参数.xlsx', sheet_name='账户资金表') # 读取帐号资金量
captitalRate = dfCapital[dfCapital['账户名'] == defaultFreqSet[5]]['资金'].iat[0] / dfCapital['资金'].sum()  # 资金占比率
dictCloseTimeClose = pd.read_pickle('pickle\\dictCloseTimeClose.pkl')  # 时间区间
dictCloseTimeCloseNight = pd.read_pickle('pickle\\dictCloseTimeCloseNight.pkl')  # 时间区间
dictGoodsName = {}
listGoods = []
dictGoodsChg = {}
dictGoodsCheng = {}
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
# 获取风险系数
now = datetime.now()
dateMark = now.isoweekday()
isOpenPosition = {}
for goodsCode in dictGoodsName.keys():
    isOpenPosition[goodsCode] = True
if now.time() > time(16):
    dateMark += 0.5
dfCapital['风险系数'] = dfCapital[dateMark]
if dateMark in []:
    for goodsCode in dictGoodsName.keys():
        isOpenPosition[goodsCode] = False

# region 读取本地文件
# 交易日
dfDatetime = pd.read_csv('RD files\\tradeDay.csv', parse_dates=['tradeDatetime'])
tradeDatetime = dfDatetime['tradeDatetime']
listHolidayDatetime = dfDatetime[dfDatetime['holiday'] == 1]['tradeDatetime'].dt.date.tolist()
tradeDate = tradeDatetime.dt.date
now = datetime.now()
s = dfDatetime['tradeDatetime'].copy()
theTradeDay = s[s >= now - timedelta(hours=17, minutes=15)].iat[0]  # 获取当前的交易日名称
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
thisWeekDay = tradeDate[(tradeDate >= weekStartTime.date()) & (tradeDate <= weekEndTime.date())]  # 本周有哪些的交易日
weekLastDay = thisWeekDay.iat[-1]  # 本周交易日的最后一天
# 交易参数综合表
ParTab = {}
for freq in listFreq:
    ParTab[freq] = pd.read_excel('RD files\\CTA交易参数表-综合版.xlsx',
                                 sheet_name='CTA{}'.format(freq)).set_index('品种名称')  # CTA交易参数表-综合表
dictFreqUnGoodsCode = {}  # 不进行交易的大类品种
setTheGoodsCode = set()  # 进行交易的品种代码
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
# endregion

dictGoodsAdj = {}  # 记录主力合约表
dictGoodsInstrument = {}  # 记录主力合约操作
listInstrument = []  # 主力合约的数列
dictInstrumentUpDownPrice = {}  # 合约的止盈与止损价格
dictInstrumentPrice = {}  # 从tick数据上接收到最新价格

# region 建立表格名称
dictFreqDb = {}
dictFreqDoc = {}  # 频段 对应 数据库中所有表格名称
for freq in listFreq:
    mongodbName = 'cta{}_'.format(freq) + programName  # 数据库名称使用 cta{}_测1
    con = pymongo.MongoClient("mongodb://localhost:27017/")  # 建立本地mongodb连接，用于储存持仓信息，交易记录，委托记录
    dictFreqDb[freq] = con[mongodbName]  # 建立 runoobdb 数据库连接，数据库名称
    dictFreqDoc[freq] = dictFreqDb[freq].list_collection_names()  # 数据库对应表格名称
# 持仓量
listFreqPosition = ['代码', '名称', '方向', '数量', '时间', '价格', '持仓盈亏']  # 持仓信息
dictFreqPosition = {}
for freq in listFreq:
    if '频段持仓' in dictFreqDoc[freq]:
        dictFreqPosition[freq] = readMongo('频段持仓', dictFreqDb[freq])
        if dictFreqPosition[freq].shape[0] == 0:
            dictFreqPosition[freq] = pd.DataFrame(columns=listFreqPosition)
    else:
        dictFreqPosition[freq] = pd.DataFrame(columns=listFreqPosition)
# 委托单
listOrderSourceColumns = []
for each in CThostFtdcOrderField._fields_:
    listOrderSourceColumns.append(each[0])
listTradeSourceColumns = []
for each in CThostFtdcTradeField._fields_:
    listTradeSourceColumns.append(each[0])
listFreqOrder = ['id', '本地下单码', '时间', '代码', '方向', '价格', '数量', '状态', '已成交', '成交均价', '拒绝原因']
dfOrder = pd.DataFrame(columns=listFreqOrder)  # 所有的委托单
dfOrderSource = pd.DataFrame(columns=listOrderSourceColumns)  # 所有的委托单（原始的）
dictFreqOrder = {}
dictFreqOrderSource = {}
for freq in listFreq:
    # 建立表格
    dictFreqOrder[freq] = pd.DataFrame(columns=listFreqOrder)
    dictFreqOrderSource[freq] = pd.DataFrame(columns=listOrderSourceColumns)
    # 建立连接
    db = dictFreqDb[freq]
    if '委托记录' in dictFreqDoc[freq]:  # 如果有表格的话，那就删除
        temp = db['委托记录']
        temp.drop()
# 成交单
listFreqTrade = ['id', '本地下单码', '时间', '代码', '名称', '方向', '价格', '数量']
dictFreqTrade = {}
dictFreqTradeSource = {}
for freq in listFreq:
    # 建立表格
    dictFreqTrade[freq] = pd.DataFrame(columns=listFreqTrade)
    dictFreqTradeSource[freq] = pd.DataFrame(columns=listTradeSourceColumns)
    # 建立连接
    db = dictFreqDb[freq]
    if '频段成交' in dictFreqDoc[freq]:
        temp = db['频段成交']
        temp.drop()
# 错误单
listFreqError = ['本地下单码', '代码', '名称', '方向', '价格', '数量', '错误原因', '时间']
dictFreqError = {}
for freq in listFreq:
    # 建立表格
    dictFreqError[freq] = pd.DataFrame(columns=listFreqError)
    # 建立连接
    db = dictFreqDb[freq]
    if '错误委托单' in dictFreqDoc[freq]:
        temp = db['错误委托单']
        temp.drop()
# 周交易明细表
listWeekTradeTab = ['交易时间', '品种名称', '交易合约号', '周次', '开仓时间', '平仓时间', '开平仓标识多', '单笔浮赢亏多', '开平仓标识空', '单笔浮赢亏空', '总净值浮赢亏', '总净值最大回撤',
                    '开仓线多', '止盈线多', '止损线多', '开仓线空', '止盈线空', '止损线空', '重叠度标识多', '重叠度标识空', '均值', '标准差', '最高价', '最低价', '仓位多', '仓位空', '标准差倍数',
                    '标准差倍数高', '标准差倍数低', '做多参数', '做空参数', '参数编号', '参数', '时间段序号']
# 全部指令单
listCommand = ['发单时间', "本地下单码", '合约号', '持有多手数', '持有空手数',
               '应开多手数', '应开空手数', '多开仓线', '多止损线', '多止盈线',
               '空开仓线', '空止盈线', '空止损线']  # 指令单的列
dfCommand = pd.read_pickle('pickle\\dfCommand.pkl')
dfCommandDrop = dfCommand[0:0].copy()
dfCommand = dfCommand.reset_index(drop=True)
for num in range(dfCommand.shape[0]):
    if '.' not in dfCommand['合约号'][num]:
        if not judgeCodeValue(dfCommand['本地下单码'][num], now):
            dfCommandDrop.loc[dfCommandDrop.shape[0]] = dfCommand.loc[num]
            dfCommand.drop([num], inplace=True)
    else:
        dfCommandDrop.loc[dfCommandDrop.shape[0]] = dfCommand.loc[num]
        dfCommand.drop([num], inplace = True)
dfCommand = dfCommand.reset_index(drop=True)
dfCommand.to_pickle('pickle\\dfCommand.pkl')
# 建立线程锁
lockDfOrder = threading.Lock()
lockDictFreqOrder = threading.Lock()
lockDictFreqPosition = threading.Lock()  # 指令的持仓情况
lockDfCommand = threading.Lock()
# endregion

# 关于dictOrderRef保存记录
if 'listTradeID.pkl' in os.listdir('pickle'):
    listTradeID = pd.read_pickle('pickle\\listTradeID.pkl')
else:
    listTradeID = []
if 'dictOrderRef.pkl' in os.listdir('pickle'):
    dictOrderRef = pd.read_pickle('pickle\\dictOrderRef.pkl')
else:
    dictOrderRef = {}
if 'dictRefOrder.pkl' in os.listdir('pickle'):
    dictRefOrder = pd.read_pickle('pickle\\dictRefOrder.pkl')
else:
    dictRefOrder = {}
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
listStopProfit = []  # 记录哪些委托单是止盈的
pd.to_pickle(dictOrderRef, 'pickle\\dictOrderRef.pkl')
pd.to_pickle(dictRefOrder, 'pickle\\dictRefOrder.pkl')
pd.to_pickle(listTradeID, 'pickle\\listTradeID.pkl')

# 数据源的连接（从哪里获取分钟数据）
dataSourseIp = 'localhost'
con = pymongo.MongoClient("mongodb://{}:27017/".format(dataSourseIp))  # 建立连接
# 分钟数据, 均值，重叠度，周交易明细表基本定义
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
host = 'localhost'
port = 8888

# 下单日志
loggingPath = 'log\\{}'.format(theTradeDay.strftime('%Y-%m-%d'))
os.makedirs(loggingPath, exist_ok=True)
logProgram = logging.getLogger("ruida")
fileHandle = logging.FileHandler(loggingPath + '\\ruida.txt')
fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logProgram.addHandler(fileHandle)
logProgram.setLevel(logging.INFO)
os.makedirs(loggingPath + '\\tick', exist_ok=True)
logTradeRecord = logging.getLogger('tick')
fileHandle = logging.FileHandler(loggingPath + '\\tick\\{}.txt'.format(theTradeDay.strftime('%Y-%m-%d')))
fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logTradeRecord.addHandler(fileHandle)
logTradeRecord.setLevel(logging.INFO)
dictFreqLog = {}
for freq in listFreq:
    theLog = logging.getLogger('CTA{}'.format(freq))
    fileHandle = logging.FileHandler(loggingPath + '\\CTA{}.txt'.format(freq))
    fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    theLog.addHandler(fileHandle)
    theLog.setLevel(logging.INFO)
    dictFreqLog[freq] = theLog  # 对应的分钟处理方法

# 预下单， 基本用于平仓操作， 用于预下单操作
dfInstrumentNextOrder = pd.DataFrame(columns=['合约号', '均值大小', '开始时间', '结束时间', '事件'])
















































