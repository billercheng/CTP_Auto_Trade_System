import socket
from mdApi import MdApi
from tdApi import *
from orderUi import *
from onBar import *
import queue
from PyQt5.QtWidgets import QApplication
import sys

class RdMd():

    def __init__(self):
        self.registerEngine()
        threading.Thread(target=self.getData, daemon=True).start()
    
    def getData(self):
        self.queueRecv = queue.PriorityQueue()
        self.strRecv = ""
        downLogProgram("将数据库数据写入内存上")
        for freq in listFreqPlus:
            dictData[freq] = {}
            downLogProgram("将频段 {} 数据库数据写入内存".format(freq))
            db = con['cta{}_trade'.format(freq)]
            for goodsCode in dictGoodsName.keys():
                if goodsCode in dictFreqUnGoodsCode[freq]:
                    continue
                goodsName = dictGoodsName[goodsCode]
                if freq < 60:
                    num = len(dictFreqGoodsClose[freq][goodsCode]) * 5 + mvlenvector[-1] + 10
                else:
                    num = len(dictFreqGoodsClose[freq][goodsCode]) * 30 + int(mvlenvector[-1] * 1.3) + 10

                dictData[freq][goodsName + '_调整表'] = readMongoNum(db, '{}_调整表'.format(goodsName), num).set_index('trade_time').sort_index()
                dictData[freq][goodsName + '_调整表'] = dictData[freq][goodsName + '_调整表'][listMin]

                if freq != 1:

                    dictData[freq][goodsName + '_均值表'] = readMongoNum(db, '{}_均值表'.format(goodsName), num).set_index('trade_time').sort_index()
                    dictData[freq][goodsName + '_均值表'] = dictData[freq][goodsName + '_均值表'][listMa]

                    dictData[freq][goodsName + '_重叠度表'] = readMongoNum(db, '{}_重叠度表'.format(goodsName), num).set_index('trade_time').sort_index()
                    dictData[freq][goodsName + '_重叠度表'] = dictData[freq][goodsName + '_重叠度表'][listOverLap]

                    dictData[freq][goodsName + '_周交易明细表'] = readMongo(goodsName + '_周交易明细表', db)

                    getWeekTradeTab(goodsCode, freq)
        self.getZhuli()  # 获取品种的调整时刻表
    
    def getZhuli(self):
        downLogProgram("从 CTA{} 上读取主力合约".format(1))
        for goodsCode in dictGoodsName.keys():
            goodsName = dictGoodsName[goodsCode]
            dictGoodsAdj[goodsCode] = readMongo(goodsName + '_调整时刻表', con['cta1_trade'])
            dictGoodsAdj[goodsCode]['adjdate'] = pd.to_datetime(dictGoodsAdj[goodsCode]['adjdate']) + timedelta(
                hours=16)
            dictGoodsInstrument[goodsCode] = dictGoodsAdj[goodsCode].index[-1]
            instrument = dictGoodsAdj[goodsCode].index[-1].split('.')[0]
            listInstrument.append(instrument)
            dictInstrumentPrice[instrument] = 0
            dictInstrumentUpDownPrice[instrument] = [0, 0]
        # 如果指令单上的合约不在主力合约的话，那么自动切换为主力合约吧
        for i in range(dfOrderDB.shape[0]):
            instrument = dfOrderDB['合约号'].iat[i]
            if instrument not in listInstrument:
                dfOrderDB['合约号'].iat[i] = dictGoodsInstrument[getGoodsCode(instrument)].split('.')[0]
            else:
                # 如果新的周数不需要这个频段品种的话，直接加上 '.'
                goodsCode = getGoodsCode(instrument)
                freq = int(dfOrderDB['本地下单码'][i].split('.')[1])
                if goodsCode in dictFreqUnGoodsCode[freq]:
                    dfOrderDB['合约号'].iat[i] += '.'
        dfOrderDB.to_pickle('pickle\\dfOrderDB.pkl')



if __name__ == '__main__':
    app = QApplication(sys.argv)
    ui = RdMd()
    ui.show()
    sys.exit(app.exec_())

