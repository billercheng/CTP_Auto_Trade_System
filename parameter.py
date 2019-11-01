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
    theDate = pd.Timestamp(listCode[0])
    theDateIndex = tradeDatetime.index(theDate)
    freq = int(listCode[1])
    if freq not in listFreq:
        return False
    goodsCode = listGoods[int(listCode[2])]
    indexBar = int(listCode[3])
    # start = dictGoodsClose[freq][goodsCode][indexBar]
    end = dictGoodsClose[freq][goodsCode][indexBar + 1 if indexBar + 1 != len(dictGoodsClose[freq][goodsCode]) else 0]
    # if start > time(20) or start < time(3) or (start == dictGoodsClose[freq][goodsCode][-1]):
    #     if start > time(20) or start == dictGoodsClose[freq][goodsCode][-1]:
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
dictCloseTimeClose = pd.read_pickle('pickle\\dictCloseTimeClose.pkl')  # 时间区间
dictCloseTimeCloseNight = pd.read_pickle('pickle\\dictCloseTimeCloseNight.pkl')  # 时间区间
dictGoodsName = {}
listGoods = []
dictGoodsChg = {}
dictGoodsCheng = {}
dictGoodsFirst = {}
dictGoodsUnit = {}
dictGoodsClose = {}  # 夜盘开始，15：00结束
dictFreqGoodsMin = {}  # 日盘开始，夜盘交易时间结束
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
    dictGoodsClose[freq] = {}
    dictFreqGoodsMin[freq] = {}
    for goodsCode in listGoods:
        if dictGoodsLastWord[goodsCode] == '15.00收盘':
            dictGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(15)]
            dictFreqGoodsMin[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(15)]
        elif dictGoodsLastWord[goodsCode] == '15.15收盘':
            dictGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(15, 15)]
            dictFreqGoodsMin[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(15, 15)]
        elif dictGoodsLastWord[goodsCode] == '23.00收盘':
            dictGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(23)]
            dictFreqGoodsMin[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(23)]
        elif dictGoodsLastWord[goodsCode] == '23.30收盘':
            dictGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(23, 30)]
            dictFreqGoodsMin[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(23, 30)]
        elif dictGoodsLastWord[goodsCode] == '1.00收盘':
            dictGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(1)]
            dictFreqGoodsMin[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(1)]
        elif dictGoodsLastWord[goodsCode] == '2.30收盘':
            dictGoodsClose[freq][goodsCode] = dictCloseTimeClose[freq][time(2, 30)]
            dictFreqGoodsMin[freq][goodsCode] = dictCloseTimeCloseNight[freq][time(2, 30)]
        if freq == 1:
            dictGoodsLast[goodsCode] = dictFreqGoodsMin[1][goodsCode][-1]

# 交易参数综合表
ParTab = {}
for freq in listFreq:
    ParTab[freq] = pd.read_excel('RD files\\CTA交易参数表-综合版.xlsx',
                                 sheet_name='CTA{}'.format(freq)).set_index('品种名称')
# 将不交易的品种写出来吧
dictFreqUnGoodsCode = {}
setTheGoodsCode = set()
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
dictFreqDoc = {}
for freq in listFreq:
    mongodbName = 'CTA{}_'.format(freq) + programName  # 数据库名称
    con = pymongo.MongoClient("mongodb://localhost:27017/")  # 建立连接
    dictFreqDb[freq] = con[mongodbName]  # 建立 runoobdb 数据库
    dictFreqDoc[freq] = dictFreqDb[freq].list_collection_names()

# 持仓量
listFreqPosition = ['代码', '名称', '方向', '数量', '时间', '价格', '持仓盈亏']
dictFreqPosition = {}
dictFreqPositionCon = {}
for freq in listFreq:
    if '频段持仓' in dictFreqDoc[freq]:
        dictFreqPosition[freq] = readMongo('频段持仓', dictFreqDb[freq])
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




















































