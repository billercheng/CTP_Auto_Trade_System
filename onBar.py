from parameter import *
import numpy as np
from math import *

def onBar(dictTemp):  # 接收一分钟数据
    dictTemp = eval(dictTemp.replace("datetime.", ""))  # 将接收的 str 字符串转化为 dict 类型
    tradeTime = dictTemp['trade_time']
    goodsCode = dictTemp['theCode']
    goodsName = dictGoodsName[goodsCode]
    # 如果是该品种的第一次执行的话，需要检测内存数据的完整
    instrument = dictTemp['goods_code'].split('.')[0]
    dictData[1][goodsName + '_调整表'].loc[tradeTime] = [dictTemp[column] for column in listMin]  # 将数据写入到 dictData 上
    for freq in listFreq:
        if tradeTime.time() in dictFreqGoodsClose[freq][goodsCode]:  # 符合分割点
            # 对bar数据进行撤单操作，编号：时间（8）频段（2）品种（2）第几个bar（2）开仓（1）平仓（0）市价平仓（9）
            if goodsCode in dictFreqUnGoodsCode[freq]:
                continue
            downLogBarDeal("--------------------------------------------------------------------------------", freq)
            downLogBarDeal("品种：{} 频段：{} 时间为：{} 数据处理".
                               format(instrument, freq, tradeTime.strftime("%Y-%m-%d %H:%M:%S")), freq)
            #region
            indexGoods = listGoods.index(goodsCode)
            indexBar = dictFreqGoodsClose[freq][goodsCode].index(tradeTime.time())
            # 如果刚好是15：00 或者是15：15的话，取下一日的交易日吧
            # 1：交易时间 . 2：freq . 3： indexGoods 4： indexBar
            if tradeTime.time() == dictFreqGoodsClose[1][goodsCode][-1]:
                temp = dfDatetime['tradeDatetime'][dfDatetime['tradeDatetime'] > tradeTime].iat[0]
                orderRef = temp.strftime('%Y%m%d') + '.' + str(freq) + '.' + str(indexGoods) + '.' + str(indexBar) + '.'  # 建立 本地下单码
            else:
                temp = dfDatetime['tradeDatetime'][dfDatetime['tradeDatetime'] > tradeTime - timedelta(hours=17, minutes=10)].iat[0]
                orderRef = temp.strftime('%Y%m%d') + '.' + str(freq) + '.' + str(indexGoods) + '.' + str(indexBar) + '.'  # 建立 本地下单码 ， 在下午收盘时间时用到
            # 进行撤单操作
            preOrderRef = orderCancelGoodsCode(goodsCode, freq, tradeTime)
            #endregion
            #region 对新的bar数据进行 均值表 重叠度表 周交易明细表计算
            if tradeTime.time() == dictFreqGoodsClose[freq][goodsCode][-1]:
                tBehind = dictFreqGoodsClose[freq][goodsCode][-1]
                tFront = dictFreqGoodsClose[freq][goodsCode][-2]
                minDelay = dictFreqGoodsClose[1][goodsCode].index(tBehind) - dictFreqGoodsClose[1][goodsCode].index(tFront)
                df = dictData[1][goodsName + '_调整表'][minDelay * (-1):].copy()  # 合成最新的 Bar 分钟数据
            else:
                df = dictData[1][goodsName + '_调整表'][freq * (-1):].copy()  # 合成最新的 Bar 分钟数据
            dictFreq = {}
            dictFreq['goods_code'] = dictTemp['goods_code']
            dictFreq['goods_name'] = goodsName
            dictFreq['open'] = df['open'][0]
            dictFreq['high'] = df['high'].max()
            dictFreq['low'] = df['low'].min()
            dictFreq['close'] = df['close'][-1]
            dictFreq['volume'] = df['volume'].sum()
            dictFreq['amt'] = df['amt'].sum()
            dictFreq['oi'] = df['oi'][-1]
            dictData[freq][goodsName + '_调整表'].loc[tradeTime] = dictFreq
            downLogBarDeal("均值", freq)
            getOneMa(freq, goodsCode, tradeTime)
            downLogBarDeal("不需要重叠度", freq)
            dictTemp = {}.fromkeys(list(dictData[freq][goodsName + '_重叠度表'].columns))
            dictData[freq][goodsName + '_重叠度表'].loc[tradeTime] = dictTemp
            downLogBarDeal("周交易明细表", freq)
            getTheOneWeekTradeTab(freq, goodsCode, tradeTime)
            downLogBarDeal("实时计算Bar", freq)
            getCommand(freq, goodsCode, orderRef, preOrderRef, True)

# region 撤单操作
def orderCancelGoodsCode(goodsCode, freq, tradeTime):  # 选择一种持仓类型进行撤单，一次类平一个策略类型的委托单
    indexGoods = listGoods.index(goodsCode)
    if tradeTime.time() in dictGoodsSend[goodsCode]:  # 如果撤单的时间刚好为收盘时间，改为预下单操作
        downLogBarDeal("进行预撤单操作", freq)
        theCancel = EVENT_ORDERCANCELPARK  # 进行预撤单操作
    else:
        downLogBarDeal("进行即时撤单操作", freq)
        theCancel = EVENT_ORDERCANCEL  # 进行立即撤单操作
    # 撤止盈单操作
    with lockDictFreqOrder:
        dfFreqOrder = dictFreqOrder[freq].copy()
    dfFreqOrder = dfFreqOrder[~dfFreqOrder['本地下单码'].duplicated(keep='last')].reset_index(drop=True)
    preOrderRef = ''
    if dfFreqOrder.shape[0] > 0:
        for index in dfFreqOrder['本地下单码'][pd.DataFrame(dfFreqOrder['本地下单码'].str.split('.').tolist())[2] == str(indexGoods)].index:
            if dfFreqOrder['状态'][index] not in ["全部成交", "全部成交报单已提交"] and dfFreqOrder['状态'][index][:3] != "已撤单":  # 如果还没有完全成交，或者 没有 已撤单 的话，那么进行撤单操作
                downLogBarDeal("品种：{} 之前的编号 {} 进行撤单操作".format(goodsCode, dfFreqOrder['本地下单码'][index]), freq)
                cancelEvent = Event(type_=theCancel)
                cancelEvent.dict_['orderRef'] = dfFreqOrder['本地下单码'][index]
                preOrderRef = dfFreqOrder['本地下单码'][index]
                ee.put(cancelEvent)
    return preOrderRef
# endregion

#region 均值处理
def getOneMa(freq, goodsCode, CurrentTradeTime):
    goodsName = dictGoodsName[goodsCode]
    dfFreqAll = dictData[freq][goodsName + '_调整表'].copy()  # 获取调整表数据
    dfFreqAll = dfFreqAll[dfFreqAll.index <= CurrentTradeTime]
    dictTemp = {}.fromkeys(dictData[freq][goodsName + '_均值表'].columns)
    dictTemp['goods_code'] = dfFreqAll['goods_code'][-1]
    dictTemp['goods_name'] = dfFreqAll['goods_name'][-1]
    dictTemp['open'] = dfFreqAll['open'][-1]
    dictTemp['high'] = dfFreqAll['high'][-1]
    dictTemp['low'] = dfFreqAll['low'][-1]
    dictTemp['close'] = dfFreqAll['close'][-1]
    dfAdjustAll = dictGoodsAdj[goodsCode].copy()
    # 调整时刻表
    dfAdj = dfAdjustAll[
        (dfAdjustAll['adjdate'] > dfFreqAll.index[0]) & (dfAdjustAll['adjdate'] < dfFreqAll.index[-1])]
    StdMvLen = ParTab[freq]['均值滑动长度'][goodsName]
    ODMvLen = ParTab[freq]['重叠度滑动长度'][goodsName]
    for i in range(dfAdj.shape[0]):
        loc = dfFreqAll[dfFreqAll.index < dfAdj['adjdate'].iat[i]].shape[0]  # 调整时刻位置
        dfFreqAll['close'][:loc] += dfAdj['adjinterval'].iat[i]
    dfFreqAll['amt'] = dfFreqAll['close'] * dfFreqAll['volume']
    for mvl in mvLenVector:
        if mvl in [StdMvLen, ODMvLen]:
            dfFreq = dfFreqAll[(-1) * mvl:]
            dictTemp['maprice_{}'.format(mvl)] = dfFreq['amt'].sum() / dfFreq['volume'].sum()
            dictTemp['stdprice_{}'.format(mvl)] = dfFreq['close'].std()
            dictTemp['stdmux_{}'.format(mvl)] = (dfFreq['close'][-1] - dictTemp['maprice_{}'.format(mvl)]) / dictTemp[
                'stdprice_{}'.format(mvl)]
            dictTemp['highstdmux_{}'.format(mvl)] = (dfFreq['high'][-1] - dictTemp['maprice_{}'.format(mvl)]) / dictTemp[
                'stdprice_{}'.format(mvl)]
            dictTemp['lowstdmux_{}'.format(mvl)] = (dfFreq['low'][-1] - dictTemp['maprice_{}'.format(mvl)]) / dictTemp[
                'stdprice_{}'.format(mvl)]
    dictData[freq][goodsName + '_均值表'].loc[CurrentTradeTime] = dictTemp
#endregion

#region 重叠度处理
def getOneOverLapDegree(freq, goodsCode, CurrentTradeTime):
    goodsName = dictGoodsName[goodsCode]
    dfMaALL = dictData[freq][goodsName + '_均值表'].copy()  # 获取均值表数据
    dfMaALL = dfMaALL[dfMaALL.index <= CurrentTradeTime]
    dictTemp = {}.fromkeys(dictData[freq][goodsName + '_重叠度表'].columns)
    dictTemp['goods_code'] = dfMaALL['goods_code'][-1]
    dictTemp['goods_name'] = dfMaALL['goods_name'][-1]
    dictTemp['high'] = dfMaALL['high'][-1]
    dictTemp['low'] = dfMaALL['low'][-1]
    dictTemp['close'] = dfMaALL['close'][-1]
    dfAdjustAll = dictGoodsAdj[goodsCode].copy()
    dfAdj = dfAdjustAll[
        (dfAdjustAll['adjdate'] > dfMaALL.index[0]) & (dfAdjustAll['adjdate'] < dfMaALL.index[-1])]
    ODMvLen = ParTab[freq]['重叠度滑动长度'][goodsName]
    for i in range(dfAdj.shape[0]):
        loc = dfMaALL[dfMaALL.index < dfAdj['adjdate'].iat[i]].shape[0]
        dfMaALL['high'][:loc] += dfAdj['adjinterval'].iat[i]
        dfMaALL['low'][:loc] += dfAdj['adjinterval'].iat[i]
    for mvl in mvLenVector:
        if mvl == ODMvLen:
            dfMa = dfMaALL[(-1) * mvl:].copy()
            num = mvl // 10
            HighPriceSortedTab = dfMa['high'].nlargest(num, keep='last')
            HighStdSortedTab = dfMa['highstdmux_{}'.format(mvl)].nlargest(num, keep='last')
            if (HighStdSortedTab < 0).all():
                dictTemp['重叠度高_{}'.format(mvl)] = -100
            else:
                dictTemp['重叠度高_{}'.format(mvl)] = len(HighPriceSortedTab.index.intersection(HighStdSortedTab.index)) / num
            LowPriceSortedTab = dfMa['low'].nsmallest(num, keep='last')
            LowStdSortedTab = dfMa['lowstdmux_{}'.format(mvl)].nsmallest(num, keep='last')
            if (LowStdSortedTab > 0).all():
                dictTemp['重叠度低_{}'.format(mvl)] = -100
            else:
                dictTemp['重叠度低_{}'.format(mvl)] = len(LowPriceSortedTab.index.intersection(LowStdSortedTab.index)) / num
    dictData[freq][goodsName + '_重叠度表'].loc[CurrentTradeTime] = dictTemp
#endregion

#region 周交易明细表处理
def getWeekTradeTab(goodsCode, freq):
    goodsName = dictGoodsName[goodsCode]
    selectWeekTradeTab = dictData[freq][goodsName + '_周交易明细表'].copy()
    selectWeekTradeTab = selectWeekTradeTab[(selectWeekTradeTab.index > weekStartTime)
                                            & (selectWeekTradeTab.index < weekEndTime)]
    if selectWeekTradeTab.shape[0] == 0:
        # 删除周交易明细表表格
        table = dictFreqDb[freq]['{}_周交易明细表'.format(goodsName)]
        table.drop()
        dictData[freq][goodsName + '_周交易明细表'] = dictData[freq][goodsName + '_周交易明细表'][0:0]
        EndODtime = dictData[freq][goodsName + '_重叠度表'][dictData[freq][goodsName + '_重叠度表'].index < weekStartTime].index[-1]
        getTheOneWeekTradeTab(freq, goodsCode, EndODtime)
    LastODT = dictData[freq][goodsName + '_重叠度表'].index[-1]
    LastWeekTradeTime = dictData[freq][goodsName + '_周交易明细表'].index[-1]
    if LastWeekTradeTime < LastODT:
        listTradeTime = list(dictData[freq][goodsName + '_重叠度表'][dictData[freq][goodsName + '_重叠度表'].index > LastWeekTradeTime].index)
        for tempTime in listTradeTime:
            getTheOneWeekTradeTab(freq, goodsCode, tempTime)

def getTheOneWeekTradeTab(freq, goodsCode, CurrentTradeTime):
    goodsName = dictGoodsName[goodsCode]
    MinChangUnit = GoodsTab['最小变动单位'][goodsName]
    OpenCloseLineMux = GoodsTab['开平仓阈值系数'][goodsName]
    StdMvLen = ParTab[freq]['均值滑动长度'][goodsName]
    ODMvLen = ParTab[freq]['重叠度滑动长度'][goodsName]
    ODth = ParTab[freq]['重叠度阈值'][goodsName]
    AbtainLossRate = ParTab[freq]['盈亏比'][goodsName]
    preWeekTradeTab = dictData[freq][goodsName + '_周交易明细表'].copy()
    preWeekTradeTab = preWeekTradeTab[preWeekTradeTab.index < CurrentTradeTime][-1:].copy()
    dr = {}
    dr = dr.fromkeys(dictData[freq][goodsName + '_周交易明细表'].columns)
    dfOverLap = dictData[freq][goodsName + '_重叠度表'][dictData[freq][goodsName + '_重叠度表'].index == CurrentTradeTime].copy()
    dfOverLap = dfOverLap.rename(columns={'重叠度高_{}'.format(ODMvLen): '重叠度高',
                                                  '重叠度低_{}'.format(ODMvLen): '重叠度低',
                                                  '重叠度收_{}'.format(ODMvLen): '重叠度收'})
    # region 做多
    CurrentHighOD = dfOverLap['重叠度高'][0]
    StdData = dictData[freq][goodsName + '_均值表'][dictData[freq][goodsName + '_均值表'].index <= CurrentTradeTime][StdMvLen * (-1):].copy()
    StdData = StdData.rename(columns={'high': '最高价', 'low': '最低价', 'close': '收盘价', 'maprice_{}'.format(StdMvLen): '均值',
                                      'stdprice_{}'.format(StdMvLen): '标准差', 'highstdmux_{}'.format(StdMvLen): '标准差倍数高', 'lowstdmux_{}'.format(StdMvLen): '标准差倍数低'})
    StdData = StdData.sort_index(ascending=False)
    HighStdList = StdData['标准差倍数高'].tolist()
    HighQ1 = np.percentile(HighStdList, 90)
    HighQ1MeanList = StdData['标准差倍数高'][StdData['标准差倍数高'] >= HighQ1]
    OpenMux = max(HighQ1MeanList.mean(), StdMuxMinValue)  # 做多开仓倍数
    StopAbtainMux = OpenMux + OpenCloseLineMux * max(1.2 * (HighQ1MeanList.max() - OpenMux),
                                                     StdMuxMinValue)
    StopLossMux = OpenMux - AbtainLossRate * (StopAbtainMux - OpenMux)
    MaPrice = StdData['均值'][0]
    StdPrice = StdData['标准差'][0]
    HighPrice = StdData['最高价'][0]
    LowPrice = StdData['最低价'][0]
    ClosePrice = StdData['收盘价'][0]
    PreClosePrice = StdData['收盘价'][1]
    # dictFreqSet 参数影响周交易明细表
    dfMa = dictData[freq]['{}_均值表'.format(goodsName)].copy()  # 需要判断上一个 Bar 的情况，是否符合条件
    dfMa = dfMa[dfMa.index < CurrentTradeTime]  # 不包括当前的 CurrentTradeTime
    dfAdjustAll = dictGoodsAdj[goodsCode].copy()
    dfAdj = dfAdjustAll[
        (dfAdjustAll['adjdate'] > dfMa.index[0]) & (dfAdjustAll['adjdate'] < dfMa.index[-1])]
    for i in range(dfAdj.shape[0]):
        loc = dfMa[dfMa.index < dfAdj['adjdate'].iat[i]].shape[0]
        # dfMa['high'][:loc] += dfAdj['adjinterval'].iat[i]
        # dfMa['low'][:loc] += dfAdj['adjinterval'].iat[i]
        # dfMa['maprice_{}'.format(StdMvLen)][:loc] += dfAdj['adjinterval'].iat[i]
        # dfMa['maprice_{}'.format(ODMvLen)][:loc] += dfAdj['adjinterval'].iat[i]
    MaWithStd = dfMa['maprice_{}'.format(StdMvLen)][-1]
    MaWithODLen = dfMa['maprice_{}'.format(ODMvLen)][-1]
    if freq < 60:
        stdMa = dfMa['maprice_{}'.format(StdMvLen)][int(StdMvLen * 0.5 * (-1)) - 1]  # 了解这一个规律吧，和前...比较就是 减去 40 个的数进行比较吧
        odMa = dfMa['maprice_{}'.format(ODMvLen)][int(ODMvLen * 1 * (-1)) - 1]
    else:
        stdMa = dfMa['maprice_{}'.format(StdMvLen)][int(StdMvLen * 0.5 * (-1)) -1]
        odMa = dfMa['maprice_{}'.format(ODMvLen)][int(ODMvLen * 1.3 * (-1)) -1]
    HighLastPrice = dfMa['high'][-1]
    LowLastPrice = dfMa['low'][-1]
    OpenPrice = MaPrice + OpenMux * StdPrice  # 开仓线
    StopAbtainPrice = MaPrice + StopAbtainMux * StdPrice  # 止盈线
    StopLossPrice = MaPrice + StopLossMux * StdPrice  # 上损线
    dr['交易时间'] = CurrentTradeTime
    dr['周次'] = week
    dr["品种名称"] = goodsName
    dr["交易合约号"] = goodsCode
    dr["开仓线多"] = OpenPrice
    dr["止盈线多"] = StopAbtainPrice
    dr["止损线多"] = StopLossPrice
    dr["重叠度标识多"] = 1
    dr["均值"] = MaPrice
    dr["标准差"] = StdPrice
    dr["最高价"] = HighPrice
    dr["最低价"] = LowPrice
    dr["标准差倍数高"] = StdData['标准差倍数高'][0]
    dr['做多参数'] = "{},{},{}".format(round(OpenMux, 4), round(StopAbtainMux, 4), round(StopLossMux, 4))
    dr['参数编号'] = 1
    dr['参数'] = "{}-{}-{}-{}".format(StdMvLen, AbtainLossRate, ODMvLen, ODth)
    if preWeekTradeTab.shape[0] > 0:
        PreTradeDuoFlag = 0  # 开平仓多标志
        CangweiDuo = 0  # 仓位多
        PreOpenTime = ""
        if str(preWeekTradeTab['开平仓标识多'][0]) not in ['nan', 'NaT', 'None', 'NaN']:
            PreTradeDuoFlag = preWeekTradeTab['开平仓标识多'][0]
        if str(preWeekTradeTab['仓位多'][0]) not in ['nan', 'NaT', 'None', 'NaN']:
            CangweiDuo = preWeekTradeTab['仓位多'][0]
        if str(preWeekTradeTab['开仓时间'][0]) not in ['nan', 'NaT', 'None','NaN']:
            PreOpenTime = preWeekTradeTab['开仓时间'][0]
        PreDuoODFlag = preWeekTradeTab['重叠度标识多'][0]
        # 根据上一条的开仓线推出下一条数据是否进行开仓的操作吧：
        PreOpenLine = changePriceLine(preWeekTradeTab['开仓线多'][0], MinChangUnit, "多", "开仓")
        PreStopAbtainLine = changePriceLine(preWeekTradeTab['止盈线多'][0], MinChangUnit, "多", "止盈")
        PreStopLossLine = changePriceLine(preWeekTradeTab['止损线多'][0], MinChangUnit, "多", "止损")
        if PreTradeDuoFlag != 1:
            # 判断是否满足开仓操作
            if PreDuoODFlag == 1:  # 是否满足重叠度标识符号
                if HighPrice >= PreOpenLine and LowPrice <= PreOpenLine \
                        and (CurrentTradeTime - preWeekTradeTab.index[0] < timedelta(days=1) or dictFreqGoodsCloseNight[freq][goodsCode][-1] == dictFreqGoodsCloseNight[1][goodsCode][-1]):  # 跨周 Bar 不开仓处理
                    isOpenDuo = True
                    # 做多参数
                    if abs(PreOpenLine - PreStopLossLine) <= 5 * dictGoodsUnit[goodsCode]:
                        isOpenDuo = False
                    if dictFreqSet[freq][1] and preWeekTradeTab['做多参数'][0].split(',')[0] == 1:
                        isOpenDuo = False
                    if dictFreqSet[freq][2] and HighLastPrice < MaWithODLen:
                        isOpenDuo = False
                    if dictFreqSet[freq][3] and (MaWithODLen < odMa or MaWithStd < stdMa):
                        isOpenDuo = False
                    if isOpenDuo:
                        dr["开平仓标识多"] = 1
                        dr["开仓时间"] = CurrentTradeTime
                        CangweiDuo = (-1) * MaxLossPerCTA / ((PreStopLossLine - PreOpenLine) / PreOpenLine)
                        dr["仓位多"] = CangweiDuo
                        dr["单笔浮赢亏多"] = CangweiDuo * (ClosePrice - OpenPrice) / OpenPrice
                    else:
                        dr["开平仓标识多"] = PreTradeDuoFlag
                else:
                    dr["开平仓标识多"] = PreTradeDuoFlag
            else:
                dr["开平仓标识多"] = PreTradeDuoFlag
        else:
            # 判断是否满足平仓操作
            CloseFlag = False
            if HighPrice >= PreStopAbtainLine and LowPrice <= PreStopAbtainLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识多"] = -1
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (PreStopAbtainLine - PreClosePrice) / PreClosePrice
            elif LowPrice > PreStopAbtainLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识多"] = -1
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (LowPrice - PreClosePrice) / PreClosePrice
            # 止损判断
            if HighPrice >= PreStopLossLine and LowPrice <= PreStopLossLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识多"] = -2
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (PreStopLossLine - PreClosePrice) / PreClosePrice
            elif HighPrice < PreStopLossLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识多"] = -2
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (HighPrice - PreClosePrice) / PreClosePrice
            if not CloseFlag:
                dr["开仓时间"] = PreOpenTime
                dr["开平仓标识多"] = PreTradeDuoFlag
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (ClosePrice - PreClosePrice) / PreClosePrice
    # endregion
    # region 做空
    CurrentLowOD = dfOverLap['重叠度低'][0]
    LowStdList = StdData['标准差倍数低']
    LowQ1 = np.percentile(LowStdList, 10)
    LowQ1MeanList = LowStdList[LowStdList <= LowQ1]
    OpenMux = min(LowQ1MeanList.mean(), StdMuxMinValue * (-1))
    StopAbtainMux = OpenMux + OpenCloseLineMux * min(1.2 * (LowQ1MeanList.min() - OpenMux),
                                                     StdMuxMinValue * (-1))
    StopLossMux = OpenMux - AbtainLossRate * (StopAbtainMux - OpenMux)
    OpenPrice = MaPrice + OpenMux * StdPrice
    StopAbtainPrice = MaPrice + StopAbtainMux * StdPrice
    StopLossPrice = MaPrice + StopLossMux * StdPrice
    dr["重叠度标识空"] = 1
    dr["开仓线空"] = OpenPrice
    dr["止盈线空"] = StopAbtainPrice
    dr["止损线空"] = StopLossPrice
    dr["标准差倍数低"] = StdData['标准差倍数低'][0]
    dr["做空参数"] = "{},{},{}".format(round(OpenMux, 4), round(StopLossMux, 4), round(StopAbtainMux, 4))
    if preWeekTradeTab.shape[0] > 0:
        PreTradeKongFlag = 0  # 开平仓空标志
        CangweiKong = 0  # 仓位空
        PreOpenTime = ""
        if str(preWeekTradeTab['开平仓标识空'][0]) not in ['nan', 'NaT', 'None', 'NaN']:
            PreTradeKongFlag = preWeekTradeTab['开平仓标识空'][0]
        if str(preWeekTradeTab['仓位空'][0]) not in ['nan', 'NaT', 'None', 'NaN']:
            CangweiKong = preWeekTradeTab['仓位空'][0]
        if str(preWeekTradeTab['开仓时间'][0]) not in ['nan', 'NaT', 'None','NaN']:
            PreOpenTime = preWeekTradeTab['开仓时间'][0]
        PreKongODFlag = preWeekTradeTab['重叠度标识空'][0]
        PreOpenLine = changePriceLine(preWeekTradeTab['开仓线空'][0], MinChangUnit, "空", "开仓")
        PreStopAbtainLine = changePriceLine(preWeekTradeTab['止盈线空'][0], MinChangUnit, "空", "止盈")
        PreStopLossLine = changePriceLine(preWeekTradeTab['止损线空'][0], MinChangUnit, "空", "止损")
        if PreTradeKongFlag != 1:
            if PreKongODFlag == 1:
                if HighPrice >= PreOpenLine and LowPrice <= PreOpenLine \
                        and (CurrentTradeTime - preWeekTradeTab.index[0] < timedelta(days=1) or dictFreqGoodsCloseNight[freq][goodsCode][-1] == dictFreqGoodsCloseNight[1][goodsCode][-1]):
                    isOpenKong = True
                    if abs(PreOpenLine - PreStopLossLine) <= 5 * dictGoodsUnit[goodsCode]:
                        isOpenKong = False
                    if dictFreqSet[freq][1] and preWeekTradeTab['做空参数'][0].split(',')[0] == -1:
                        isOpenKong = False
                    if dictFreqSet[freq][2] and LowLastPrice > MaWithODLen:
                        isOpenKong = False
                    if dictFreqSet[freq][3] and (MaWithODLen > odMa or MaWithStd > stdMa):
                        isOpenKong = False
                    if isOpenKong:
                        dr["开平仓标识空"] = 1
                        dr["开仓时间"] = CurrentTradeTime
                        CangweiKong = MaxLossPerCTA / ((PreStopLossLine - PreOpenLine) / PreOpenLine)
                        dr["仓位空"] = CangweiKong
                        dr["单笔浮赢亏空"] = (-1) * CangweiKong * (ClosePrice - PreOpenLine) / PreOpenLine
                    else:
                        dr["开平仓标识空"] = PreTradeKongFlag
                else:
                    dr["开平仓标识空"] = PreTradeKongFlag
            else:
                dr["开平仓标识空"] = PreTradeKongFlag
        else:
            CloseFlag = False
            # 止盈判断
            if HighPrice >= PreStopAbtainLine and LowPrice <= PreStopAbtainLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识空"] = -1
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位空"] = CangweiKong
                dr["单笔浮赢亏空"] = (-1) * CangweiKong * (PreStopAbtainLine - PreClosePrice) / PreClosePrice
            elif HighPrice < PreStopAbtainLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识空"] = -1
                dr["仓位空"] = CangweiKong
                dr["平仓时间"] = CurrentTradeTime
                dr["单笔浮赢亏空"] = (-1) * CangweiKong * (LowPrice - PreClosePrice) / PreClosePrice
            # 止损判断
            if HighPrice >= PreStopLossLine and LowPrice <= PreStopLossLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识空"] = -2
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位空"] = CangweiKong
                dr["单笔浮赢亏空"] = -CangweiKong * (PreStopLossLine - PreClosePrice) / PreClosePrice
            elif LowPrice > PreStopLossLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识空"] = -2
                dr["仓位空"] = CangweiKong
                dr["平仓时间"] = CurrentTradeTime
                dr["单笔浮赢亏空"] = -CangweiKong * (HighPrice - PreClosePrice) / PreClosePrice
            if not CloseFlag:
                dr["开仓时间"] = PreOpenTime
                dr["开平仓标识空"] = PreTradeKongFlag
                dr["仓位空"] = CangweiKong
                dr["单笔浮赢亏空"] = -CangweiKong * (ClosePrice - PreClosePrice) / PreClosePrice
    # endregion
    dictData[freq][goodsName + '_周交易明细表'].loc[CurrentTradeTime] = dr
    dictFreqDb[freq][goodsName + '_周交易明细表'].insert_one(dr)
    downLogBarDeal(str(dr), freq)
#endregion

# region 交易指令表
def getCommand(freq, goodsCode, orderRef, preOrderRef, isOrderCommand):  # 生成指令记录表
    goodsName = dictGoodsName[goodsCode]  # 品种名称
    instrument = dictGoodsInstrument[goodsCode]  # 合约名称
    CapitalMaxLossRate = GoodsTab["资产回撤率"][goodsName]  # 资产回撤率，一般为 0.001 ， 用于乘 风险系数
    ChengShu = GoodsTab["合约乘数"][goodsName]
    MinChangUnit = GoodsTab["最小变动单位"][goodsName]
    DayTradeEnable = ParTab[freq]["日盘交易标识"][goodsName]
    NightTradeEnable = ParTab[freq]["夜盘交易标识"][goodsName]
    ODMvLen = ParTab[freq]["重叠度滑动长度"][goodsName]
    StdMvLen = ParTab[freq]['均值滑动长度'][goodsName]
    # 下午结束Bar
    finalAfternoon = dictFreqGoodsClose[freq][goodsCode][-1]  # 下午倒数第一个结束 bar
    # 下午倒数第一交易时间
    secondFinalAfternoon = dictFreqGoodsClose[freq][goodsCode][-2]  # 下午倒数第二个结束 bar
    # 下午倒数第二交易时间
    if len(dictFreqGoodsClose[freq][goodsCode]) == 2:  # 避免出现 190， 200 这样，超大的频段
        thirdFinalAfternoon = secondFinalAfternoon
    else:
        thirdFinalAfternoon = dictFreqGoodsClose[freq][goodsCode][-3]
    # 夜盘结束Bar
    finalNight = dictFreqGoodsCloseNight[freq][goodsCode][-1]
    # 夜盘最后交易时间
    secondFinalNight = dictFreqGoodsCloseNight[freq][goodsCode][-2]
    # 夜盘倒数第二交易时间
    if len(dictFreqGoodsCloseNight[freq][goodsCode]) == 2:
        thirdFinalNight = secondFinalNight
    else:
        thirdFinalNight = dictFreqGoodsCloseNight[freq][goodsCode][-3]
    # 根据最新周交易状态，进行下单
    thisWeekTradeTab = dictData[freq]['{}_周交易明细表'.format(goodsName)][-1:]
    TradeTime = thisWeekTradeTab.index[0]  # 指令时间
    # 进行 dfMa 的调整表操作
    dfMa = dictData[freq]['{}_均值表'.format(goodsName)].copy()
    dfMa = dfMa[dfMa.index <= TradeTime]
    dfAdjustAll = dictGoodsAdj[goodsCode].copy()
    dfAdj = dfAdjustAll[
        (dfAdjustAll['adjdate'] > dfMa.index[0]) & (dfAdjustAll['adjdate'] < dfMa.index[-1])]
    for i in range(dfAdj.shape[0]):
        loc = dfMa[dfMa.index < dfAdj['adjdate'].iat[i]].shape[0]
        # dfMa['maprice_{}'.format(StdMvLen)][:loc] += dfAdj['adjinterval'].iat[i]  # 均值数据补全数据操作
        # dfMa['maprice_{}'.format(ODMvLen)][:loc] += dfAdj['adjinterval'].iat[i]
    MaWithODLen = dfMa['maprice_{}'.format(ODMvLen)][-1]
    # 当前Bar与第0.5倍与第1倍bar的均值进行比较
    LongParStr = thisWeekTradeTab['做多参数'][0]
    ShortParStr = thisWeekTradeTab['做空参数'][0]
    LongParList = LongParStr.split(',')
    ShortParList = ShortParStr.split(',')
    MaWithStd = thisWeekTradeTab['均值'][0]
    #region 做多线
    # 最新周交易状态的信息
    LastDuoFlag = thisWeekTradeTab['开平仓标识多'][0]
    LastKongFlag = thisWeekTradeTab['开平仓标识空'][0]
    ODduoFlag = thisWeekTradeTab['重叠度标识多'][0]
    ODkongFlag = thisWeekTradeTab['重叠度标识空'][0]
    # 获取当前策略的持仓信息
    pos1 = 0  # 多头的持仓数
    pos2 = 0  # 空头的持仓数
    with lockDictFreqPosition:
        dfFreqPosition = dictFreqPosition[freq].copy()
    if instrument in dfFreqPosition['代码'].values:
        theIndex = dfFreqPosition['代码'].tolist().index(instrument)
        volumeGoods = dfFreqPosition["数量"].iat[theIndex]
        if volumeGoods > 0:  # 如果 数量 大于 0，表示 为 多头
            pos1 = volumeGoods
        else:  # 否则 为 空头
            pos2 = abs(volumeGoods)
    downLogBarDeal("{} 持多仓手数：{}".
                       format(instrument, pos1), freq)
    downLogBarDeal("{} 持空仓手数：{}".
                       format(instrument, pos2), freq)
    HighPrice = thisWeekTradeTab["最高价"][0]
    LowPrice = thisWeekTradeTab["最低价"][0]
    OpenLongPrice = thisWeekTradeTab['开仓线多'][0]
    LongStopProfit = thisWeekTradeTab['止盈线多'][0]
    LongStopLoss = thisWeekTradeTab['止损线多'][0]
    accountName = dictFreqSet[freq][5]  # 账号名称
    accountCapital = dfCapital[dfCapital['账户名'] == accountName]['资金'].iat[0]  # 账号资金
    RiskRate = dfCapital[dfCapital['账户名'] == accountName]['风险系数'].iat[0]  # 账号风险系数
    CangWei = CapitalMaxLossRate / ((OpenLongPrice - LongStopLoss) / OpenLongPrice)  # 账号仓位
    DuoCountMux = 1  # 多开仓系数
    if dictFreqSet[freq][4]:
        if LowPrice > (OpenLongPrice + LongStopLoss) / 2:  # 最低价在做多的开仓线和止损线之上，则仅开一半仓位
            downLogBarDeal("最低价在做多的开仓线和止损线之上，则仅开一半仓位  {} > ({} + {}) / 2  ".format(LowPrice, OpenLongPrice, LongStopLoss), freq)
            DuoCountMux = 0.5
    AccoutRate = 1
    DuoBuyCount = (AccoutRate * accountCapital * CangWei * RiskRate) / (OpenLongPrice * ChengShu)  # (账号资金 * 仓位 * 风险系数) / (开仓线多 * 乘数)
    Duovolume = max(floor(DuoBuyCount * DuoCountMux), 1)  # 计算多手数
    maxVolume = (dictData[freq]['{}_调整表'.format(goodsName)]['volume'][-1:].sum() * captitalRate / freq) // 4  # 关于 最大手数的分配
    maxVolume = min(maxVolume, staticMaxVolume)  # 计算出 最多的手数
    downLogBarDeal("计算最大的多手数为：{}".format(maxVolume), freq)
    Duovolume = min(maxVolume, Duovolume)
    downLogBarDeal("计算多手数过程 {} = round({} * {})".format(Duovolume, DuoBuyCount, DuoCountMux), freq)
    # 进行价格取整操作
    OpenLongPrice = changePriceLine(OpenLongPrice, MinChangUnit, "多", "开仓")
    LongStopProfit = changePriceLine(LongStopProfit, MinChangUnit, "多", "止盈")
    LongStopLoss = changePriceLine(LongStopLoss, MinChangUnit, "多", "止损")
    #endregion
    #region 做空线
    OpenShortPrice = thisWeekTradeTab["开仓线空"][0]
    ShortStopProfit = thisWeekTradeTab["止盈线空"][0]
    ShortStopLoss = thisWeekTradeTab['止损线空'][0]
    KongCountMux = 1
    if dictFreqSet[freq][4]:
        if HighPrice < (ShortStopLoss + OpenShortPrice) / 2:  # 最高价在做空的开仓线和止损线中线之下，则仅开一半空仓位
            downLogBarDeal("最高价在做空的开仓线和止损线中线之下，则仅开一半空仓位  {} < ({} + {}) / 2  ".format(OpenShortPrice, ShortStopProfit, ShortStopLoss), freq)
            KongCountMux = 0.5
    CangWei = CapitalMaxLossRate / ((ShortStopLoss - OpenShortPrice) / OpenShortPrice)
    KongBuyCount = (AccoutRate * accountCapital * CangWei * RiskRate) / (OpenShortPrice * ChengShu)
    Kongvolume = max(floor(KongBuyCount * KongCountMux), 1)
    downLogBarDeal("计算最大的空手数为：{}".format(maxVolume), freq)
    Kongvolume = min(maxVolume, Kongvolume)
    downLogBarDeal("计算空手数过程 {} = round({} * {})".format(Kongvolume, KongBuyCount, KongCountMux), freq)
    # 进行价格取整操作
    OpenShortPrice = changePriceLine(OpenShortPrice, MinChangUnit, "空", "开仓")
    ShortStopProfit = changePriceLine(ShortStopProfit, MinChangUnit, "空", "止盈")
    ShortStopLoss = changePriceLine(ShortStopLoss, MinChangUnit, "空", "止损")
    # endregion
    #region 高低价是否满足重叠度长度对应的均值条件
    if dictFreqSet[freq][2]:
        downLogBarDeal('需要判断 重叠度对应的均值与高低价关系', freq)
        if HighPrice < MaWithODLen:  # MaWithODLen 不指定的均值
            Duovolume = 0
            downLogBarDeal('HighPrice < MaWithODLen Duovolume = 0', freq)
        if LowPrice > MaWithODLen:
            Kongvolume = 0
            downLogBarDeal('LowPrice > MaWithODLen Kongvolume = 0', freq)
    else:
        downLogBarDeal('无需判断 重叠度对应的均值与高低价关系', freq)
    if dictFreqSet[freq][3]:
        downLogBarDeal('需要判断 当前MA是否满足均值与重叠度斜率', freq)
        stdMa = dfMa['maprice_{}'.format(StdMvLen)][int(StdMvLen * 0.5 * (-1)) - 1]
        odMa = dfMa['maprice_{}'.format(ODMvLen)][int(ODMvLen * 1 * (-1)) - 1]
        if MaWithODLen < odMa or MaWithStd < stdMa:
            Duovolume = 0
            downLogBarDeal('{} < {} or {} < {}  Duovolume = 0'.format(MaWithODLen, odMa, MaWithStd, stdMa), freq)
        if MaWithODLen > odMa or MaWithStd > stdMa:
            Kongvolume = 0
            downLogBarDeal('{} > {} or {} > {}  Kongvolume = 0'.format(MaWithODLen, odMa, MaWithStd, stdMa), freq)
    else:
        downLogBarDeal('无需判断 当前MA是否满足均值与重叠度斜率', freq)
    LongSign = LastDuoFlag
    ShortSign = LastKongFlag
    sendTradeDr = {}
    sendTradeDr = sendTradeDr.fromkeys(listCommand)  # 计算交易指令
    sendTradeDr["本地下单码"] = '         '
    sendTradeDr["发单时间"] = TradeTime
    sendTradeDr["合约号"] = instrument
    sendTradeDr["持有多手数"] = pos1
    sendTradeDr["持有空手数"] = pos2
    sendTradeDr["应开多手数"] = Duovolume
    sendTradeDr["应开空手数"] = Kongvolume
    # region 做多持仓判断
    if LongSign != 1:  # 多侍开仓
        if pos1 == 0:  # 正常多开单
            if ODduoFlag == 1:  # 满足重叠度条件,则正常开多单
                TradeDuoOkStatus = 1  # 标识多正常下单
            else:
                TradeDuoOkStatus = 3  # 标识多不下单
        else:  # 本应该开多，但还有剩余仓位，则不开多，分批处理函数
            if ODduoFlag == 1:
                TradeDuoOkStatus = 4
            else:
                TradeDuoOkStatus = 2  # 不满足重叠度条件
    else:  # 平仓判断
        if pos1 == 0:  # 本应该 平多仓，但实际 未持多仓
            TradeDuoOkStatus = 5
        else:
            TradeDuoOkStatus = 1
    if TradeDuoOkStatus == 1 or TradeDuoOkStatus == 4:  # 状态为1，4，则保留正常开平仓线，手数
        sendTradeDr["多开仓线"] = OpenLongPrice
        sendTradeDr["多止损线"] = LongStopLoss
        sendTradeDr["多止盈线"] = LongStopProfit
    elif TradeDuoOkStatus == 2 or TradeDuoOkStatus == 3 or TradeDuoOkStatus == 5:  # 强平多仓
        sendTradeDr["多开仓线"] = 1
        sendTradeDr["多止损线"] = 0
        sendTradeDr["多止盈线"] = 2
    # endregion

    #region 做空持仓判断
    if ShortSign != 1: # 空待开仓，即 ShortSign 为 0
        if pos2 == 0: # 正常开空单
            if ODkongFlag == 1: # 满足重叠度条件, 则正常开空单
                TradeKongOkStatus = 1 # 正常下单
            else:
                TradeKongOkStatus = 3 # 不下单
        else: # 本应开空，但已经持有空，则不开空
            if ODkongFlag == 1: # 满足重叠度条件, 则正常开空单
                TradeKongOkStatus = 4 # 正常下单
            else:
                TradeKongOkStatus = 2 # 不下单
    else:
        if pos2 == 0: # 本应该持空仓，但实际为无持仓，则不发单
            TradeKongOkStatus = 5
        else: # 有持仓，则正常的空线
            TradeKongOkStatus = 1
    if TradeKongOkStatus == 1 or TradeKongOkStatus == 4:
        sendTradeDr["空开仓线"] = OpenShortPrice
        sendTradeDr["空止损线"] = ShortStopLoss
        sendTradeDr["空止盈线"] = ShortStopProfit
    elif TradeKongOkStatus == 2 or TradeKongOkStatus == 3 or TradeKongOkStatus == 5:
        sendTradeDr["空开仓线"] = 1
        sendTradeDr["空止损线"] = 2
        sendTradeDr["空止盈线"] = 0
    #endregion
    downLogBarDeal("品种为：{} 多标志状态为：TradeDuoOkStatus = {}, 空标志状态为：TradeKongOkStatus = {}"
                       .format(instrument, TradeDuoOkStatus, TradeKongOkStatus), freq)
    #region 计算一个指令的 止损幅度， 如果 止损幅度 少于 5个最小变量， 则不需要这个指令
    if ShortSign != 1 and LongSign != 1:
        if sendTradeDr["多开仓线"] != 1:
            if dictFreqSet[freq][0]:
                if OpenLongPrice - LongStopLoss <= 5 * MinChangUnit:  # 开仓价 - 止损价 <= 5 * Min
                    LongOpenCloseMux = round((OpenLongPrice - LongStopLoss) / MinChangUnit)
                    downLogBarDeal("{} 做多线 OpenLongPrice = {}, LongStopLoss = {}, 相差价位数 = {}, 不符合开仓条件"
                                       .format(instrument, OpenLongPrice, LongStopLoss, LongOpenCloseMux), freq)
                    sendTradeDr["多开仓线"] = 1
                    sendTradeDr["多止损线"] = 0
                    sendTradeDr["多止盈线"] = 2
            else:
                downLogBarDeal("无须判断止损幅度与最小变量单位的大小", freq)
        if sendTradeDr["空开仓线"] != 1:
            if dictFreqSet[freq][0]:
                if ShortStopLoss - OpenShortPrice <= 5 * MinChangUnit:
                    ShortOpenCloseMux = round((ShortStopLoss - OpenShortPrice) / MinChangUnit)
                    downLogBarDeal("{} 做空线 OpenShortPrice = {}, ShortStopLoss = {}, 相差价位数 = {}, 不符合开仓条件"
                                       .format(instrument, OpenShortPrice, ShortStopLoss, ShortOpenCloseMux), freq)
                    sendTradeDr["空开仓线"] = 1
                    sendTradeDr["空止损线"] = 2
                    sendTradeDr["空止盈线"] = 0
            else:
                downLogBarDeal("无须判断止损幅度与最小变量单位的大小", freq)
    #endregion
    # 整个下单判断
    if TradeDuoOkStatus != 5 and TradeKongOkStatus != 5:
        if TradeDuoOkStatus == 3 and TradeKongOkStatus == 3:
            downLogBarDeal("{} 做多与做空的OD均不满足重叠度条件，不下单".format(instrument), freq)
        else:
            # 判断做多：
            if TradeDuoOkStatus == 2 or TradeDuoOkStatus == 4:
                downLogBarDeal("{} 不应该持仓，但实际持多仓，平 {} 手，市价平仓编号为 {}".format(instrument, pos1, orderRef + '9'), freq)
                orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                orderEvent.dict_['InstrumentID'] = instrument
                orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                orderEvent.dict_['LimitPrice'] = 0
                orderEvent.dict_['orderRef'] = orderRef + '9'
                orderEvent.dict_['VolumeTotalOriginal'] = abs(pos1)
                orderEvent.dict_['preOrderRef'] = preOrderRef
                if TradeTime.time() not in dictGoodsSend[goodsCode]:
                    if isOrderCommand:
                        ee.put(orderEvent)
                else:
                    downLogBarDeal("{} 将委托单记录，待进行交易时间后，才进行下单操作".format(instrument), freq)
                    dfInstrumentNextOrder.loc[dfInstrumentNextOrder.shape[0]] = [instrument, freq, getTradeTimeNextBar(goodsCode, 1, TradeTime) - timedelta(minutes=1), getTradeTimeNextBar(goodsCode, freq, TradeTime), orderEvent]
                sendTradeDr["持有多手数"] = 0
            if TradeDuoOkStatus == 1 or TradeDuoOkStatus == 4:
                if LongSign != 1:
                    sendTradeDr['本地下单码'] = orderRef + '1'
                    if dictFreqSet[freq][1]:
                        if LongParList[0] == "1":
                            downLogBarDeal("{} 做多，开仓线参数 = {}，所以不会进行开仓操作".format(instrument, LongParList[0]), freq)
                            sendTradeDr["多开仓线"] = 1
                            sendTradeDr["多止损线"] = 0
                            sendTradeDr["多止盈线"] = 2
                    else:
                        downLogBarDeal("无须判断开仓倍数为1时，是否开仓", freq)
                    if sendTradeDr["多开仓线"] != 1:
                        # bar内止盈，则按止盈倍数进行调整
                        downLogBarDeal("{} 进行 bar 内做多止赢与止损的调整".format(instrument), freq)
                        # Bar内的处理方法
                        IntervalPrice = OpenLongPrice + (LongStopProfit - OpenLongPrice) * StopAbtainInBarMux
                        LongStopProfit = changePriceLine(IntervalPrice, MinChangUnit, "多", "止盈")
                        sendTradeDr["多止盈线"] = LongStopProfit
                        # bar内止损，则按止损倍数进行调整
                        IntervalPrice = OpenLongPrice - (OpenLongPrice - LongStopLoss) * StopLossInBarMux
                        LongStopLoss = changePriceLine(IntervalPrice, MinChangUnit, "多", "止损")
                        sendTradeDr["多止损线"] = LongStopLoss
                else:
                    sendTradeDr['本地下单码'] = orderRef + '0'
            if TradeKongOkStatus == 2 or TradeKongOkStatus == 4:
                downLogBarDeal("{} 不应该持仓，但实际持空仓，平 {} 手，市价平仓编号为 {}".format(instrument, pos2, orderRef + '9'), freq)
                orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                orderEvent.dict_['InstrumentID'] = instrument
                orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                orderEvent.dict_['LimitPrice'] = 0
                orderEvent.dict_['orderRef'] = orderRef + '9'
                orderEvent.dict_['VolumeTotalOriginal'] = abs(pos2)
                orderEvent.dict_['preOrderRef'] = preOrderRef
                if TradeTime.time() not in dictGoodsSend[goodsCode]:
                    if isOrderCommand:
                        ee.put(orderEvent)
                else:
                    downLogBarDeal("{} 将委托单记录，待进行交易时间后，才进行下单操作".format(instrument), freq)
                    dfInstrumentNextOrder.loc[dfInstrumentNextOrder.shape[0]] = [instrument, freq, getTradeTimeNextBar(goodsCode, 1, TradeTime) - timedelta(minutes=1), getTradeTimeNextBar(goodsCode, freq, TradeTime), orderEvent]
                sendTradeDr["持有空手数"] = 0
            if TradeKongOkStatus == 1 or TradeKongOkStatus == 4:
                if ShortSign != 1:
                    if sendTradeDr['本地下单码'] == None or sendTradeDr['本地下单码'][-1] != '0':
                        sendTradeDr['本地下单码'] = orderRef + '1'
                    if dictFreqSet[freq][1]:
                        if ShortParList[0] == "-1":
                            downLogBarDeal("{} 做空，开仓线参数 = {}，所以不会进行开仓操作".format(instrument, ShortParList[0]), freq)
                            sendTradeDr["空开仓线"] = 1
                            sendTradeDr["空止损线"] = 0
                            sendTradeDr["空止盈线"] = 2
                    else:
                        downLogBarDeal("无须判断开仓倍数为1时，是否开仓", freq)
                    if sendTradeDr["空开仓线"] != 1:
                        downLogBarDeal("{} 进行 bar 内做空止赢与止损的调整".format(instrument), freq)
                        # bar内止盈，则按止盈倍数进行调整
                        IntervalPrice = OpenShortPrice + (ShortStopProfit - OpenShortPrice) * StopAbtainInBarMux
                        ShortStopProfit = changePriceLine(IntervalPrice, MinChangUnit, "空", "止盈")
                        sendTradeDr["空止盈线"] = ShortStopProfit
                        # bar内止损，则按止损倍数进行调整
                        IntervalPrice = OpenShortPrice - (OpenShortPrice - ShortStopLoss) * StopLossInBarMux
                        ShortStopLoss = changePriceLine(IntervalPrice, MinChangUnit, "空", "止损")
                        sendTradeDr["空止损线"] = ShortStopLoss
                else:
                    sendTradeDr['本地下单码'] = orderRef + '0'
            if sendTradeDr['本地下单码'][-1:] == '1':  # 这个指令单为 开仓单
                downLogBarDeal("本频段操作为开仓单", freq)
                if TradeTime.date() in listHolidayDate:  # 如果这个 指令单 的时间为 secondFinalAfternoon, thirdFinalAfternoon
                    if TradeTime.time() in [thirdFinalAfternoon, secondFinalAfternoon]:
                        downLogBarDeal("{} 今天为节假日前一日, 没有夜盘数据, "
                                               "这个Bar数据为倒数第二个Bar数据, 或者最后一个bar, 所以不进行开仓操作".format(instrument, TradeTime), freq)
                        return
                elif (TradeTime - timedelta(hours=4)).date() == thisWeekDay.iat[-1]:
                    if dictGoodsLast[goodsCode] in [time(15), time(15, 15)]:
                        if TradeTime.time() in [thirdFinalAfternoon, secondFinalAfternoon]:
                            downLogBarDeal("{} 本周倒数第二bar时间: 或者最后一个bar, {} ,且均待开仓，则不再开仓！ 所以不进行开仓操作".format(instrument,
                                                                                                     TradeTime), freq)
                            return
                    else:
                        if finalNight == dictGoodsLast[goodsCode]:
                            if TradeTime.time() in [thirdFinalNight, secondFinalNight]:
                                downLogBarDeal("{} 本周倒数第二bar时间或者最后一个bar: {},且均待开仓，则不再开仓！, 所以不进行开仓操作".format(
                                    instrument,
                                    TradeTime), freq)
                                return
                        else:
                            if TradeTime.time() in [finalNight, secondFinalNight]:
                                downLogBarDeal("{} 本周最后一个bar时间: {} ,且均待开仓，则不再开仓！ 所以不进行开仓操作".format(
                                    instrument,
                                    TradeTime), freq)
                                return
                # 多待开仓，下单手数为0，则调整做多开平仓线
                if LongSign != 1 and Duovolume == 0:
                    sendTradeDr["多开仓线"] = 1
                    sendTradeDr["多止盈线"] = 2
                    sendTradeDr["多止损线"] = 0
                if ShortSign != 1 and Kongvolume == 0:
                    sendTradeDr["空开仓线"] = 1
                    sendTradeDr["空止盈线"] = 0
                    sendTradeDr["空止损线"] = 2
                if sendTradeDr["多开仓线"] == 1 and sendTradeDr["空开仓线"] == 1:  # 不符合指令条件的，不需要下指令单
                    downLogBarDeal("{} 多开仓线 = 1 空开仓线 = 1".format(instrument), freq)
                    return
                else:
                    if DayTradeEnable == 1 and NightTradeEnable != 1:  #
                        if (TradeTime.time() < time(16) and TradeTime.time() >= time(8)) or TradeTime.time() == dictGoodsLast[goodsCode]:
                            # 有夜盘的同时， TradeTime.time() == finalAfternoon 不开外，其它开
                            if not ((TradeTime.time() == finalAfternoon) and (dictGoodsLast[goodsCode] not in [time(15), time(15, 15)])):
                                downLogBarDeal("{} 夜盘不开仓，仅平仓；日盘可开平，现在是日盘，可进行开平仓".format(instrument), freq)
                                downLogBarDeal(str(sendTradeDr), freq)
                                event = Event(type_ = EVENT_SHOWCOMMAND)
                                event.dict_ = sendTradeDr
                                ee.put(event)
                            else:
                                downLogBarDeal("{} 有夜盘的同时，tradeTime 到达 15:00 所有不开仓操作".format(instrument), freq)
                        else:
                            downLogBarDeal("{} 夜盘不开仓，仅平仓；日盘可开平，现在有夜盘，不进行开仓".format(instrument), freq)
                    elif DayTradeEnable != 1 and NightTradeEnable == 1:
                        if TradeTime.time() > time(16) and TradeTime.time() < time(8):
                            if TradeTime != finalNight:
                                downLogBarDeal("{} 日盘不开仓，仅平仓；夜盘可开平，现在是夜盘，可进行开仓".format(instrument), freq)
                                downLogBarDeal(str(sendTradeDr), freq)
                                event = Event(type_ = EVENT_SHOWCOMMAND)
                                event.dict_ = sendTradeDr
                                ee.put(event)
                            else:
                                downLogBarDeal("{} 日盘不开仓，仅平仓；夜盘可开平，可能在日盘开仓，不进行开仓".format(instrument), freq)
                        else:
                            downLogBarDeal("{} 日盘不开仓，仅平仓；夜盘可开平，现在是日盘，不进行开仓".format(instrument), freq)
                    elif DayTradeEnable == 1 and NightTradeEnable == 1:
                        downLogBarDeal("{} 日盘夜盘均可开平".format(instrument), freq)
                        downLogBarDeal(str(sendTradeDr), freq)
                        event = Event(type_ = EVENT_SHOWCOMMAND)
                        event.dict_ = sendTradeDr
                        ee.put(event)
                    elif DayTradeEnable != 1 and NightTradeEnable != 1:
                        downLogBarDeal("{} 日夜盘均不下单".format(instrument), freq)
            elif sendTradeDr['本地下单码'][-1:] == '0':  # 委托单为 止盈止损单
                downLogBarDeal("本频段操作为止盈止损单", freq)
                if pos1 > 0:  # 如果为多头，挂多止盈
                    theDirectionType = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                    theOffsetFlagType = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                    thePrice = sendTradeDr['多止盈线']
                    thePos = pos1
                elif pos2 > 0:  # 如果为空头，挂空止盈
                    theDirectionType = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                    theOffsetFlagType = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                    thePrice = sendTradeDr['空止盈线']
                    thePos = pos2
                # region 进行，市价下单操作，一般是最后交易时间进行的操作
                if TradeTime.date() in listHolidayDate:  # 判断此交易日是否有 夜盘
                    if TradeTime.time() >= secondFinalAfternoon:  # 没有夜盘的情况
                        downLogBarDeal("进行市价平仓操作", freq)
                        orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                        orderEvent.dict_['InstrumentID'] = instrument
                        orderEvent.dict_['Direction'] = theDirectionType
                        orderEvent.dict_['CombOffsetFlag'] = theOffsetFlagType
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                        orderEvent.dict_['LimitPrice'] = 0
                        orderEvent.dict_['orderRef'] = sendTradeDr['本地下单码'][:-1] + '9'
                        orderEvent.dict_['VolumeTotalOriginal'] = thePos
                        orderEvent.dict_['preOrderRef'] = preOrderRef
                        if isOrderCommand:
                            ee.put(orderEvent)
                        return
                elif (TradeTime - timedelta(hours=4)).date() == thisWeekDay.iat[-1]:  # 有夜盘的情况
                    if dictGoodsLast[goodsCode] in [time(15), time(15, 15)]:  # 这个本来就没有夜盘的
                        if TradeTime.time() >= secondFinalAfternoon:
                            downLogBarDeal("进行市价平仓操作", freq)
                            orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                            orderEvent.dict_['InstrumentID'] = instrument
                            orderEvent.dict_['Direction'] = theDirectionType
                            orderEvent.dict_['CombOffsetFlag'] = theOffsetFlagType
                            orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                            orderEvent.dict_['LimitPrice'] = 0
                            orderEvent.dict_['orderRef'] = sendTradeDr['本地下单码'][:-1] + '9'
                            orderEvent.dict_['VolumeTotalOriginal'] = thePos
                            orderEvent.dict_['preOrderRef'] = preOrderRef
                            if isOrderCommand:
                                ee.put(orderEvent)
                            return
                    else:
                        if finalNight == dictGoodsLast[goodsCode]:
                            if TradeTime.time() == finalNight or TradeTime.time() == secondFinalNight:
                                downLogBarDeal("{} 本周倒数第二bar时间或者最后一个bar: {},且均待开仓，则不再开仓！, 所以不进行开仓操作".format(
                                    instrument,
                                    TradeTime), freq)
                                downLogBarDeal("进行市价平仓操作", freq)
                                orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                                orderEvent.dict_['InstrumentID'] = instrument
                                orderEvent.dict_['Direction'] = theDirectionType
                                orderEvent.dict_['CombOffsetFlag'] = theOffsetFlagType
                                orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                                orderEvent.dict_['LimitPrice'] = 0
                                orderEvent.dict_['orderRef'] = sendTradeDr['本地下单码'][:-1] + '9'
                                orderEvent.dict_['VolumeTotalOriginal'] = thePos
                                orderEvent.dict_['preOrderRef'] = preOrderRef
                                if isOrderCommand:
                                    ee.put(orderEvent)
                                return
                        else:
                            if TradeTime.time() == finalNight:
                                downLogBarDeal("{} 本周最后一个bar时间: {} ,且均待开仓，则不再开仓！ 所以不进行开仓操作".format(instrument,TradeTime), freq)
                                downLogBarDeal("进行市价平仓操作", freq)
                                orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                                orderEvent.dict_['InstrumentID'] = instrument
                                orderEvent.dict_['Direction'] = theDirectionType
                                orderEvent.dict_['CombOffsetFlag'] = theOffsetFlagType
                                orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                                orderEvent.dict_['LimitPrice'] = 0
                                orderEvent.dict_['orderRef'] = sendTradeDr['本地下单码'][:-1] + '9'
                                orderEvent.dict_['VolumeTotalOriginal'] = thePos
                                orderEvent.dict_['preOrderRef'] = preOrderRef
                                if isOrderCommand:
                                    ee.put(orderEvent)
                                return
                # endregion

                # region 为开仓单下止盈与止损单操作
                if pos1 > 0:
                    downLogBarDeal("因为这个下单为止盈止损单，现在持有多仓，现在下多止盈单",freq)
                    orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                    orderEvent.dict_['InstrumentID'] = instrument
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                    orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                    orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                    orderEvent.dict_['LimitPrice'] = thePrice
                    orderEvent.dict_['orderRef'] = sendTradeDr['本地下单码'][:-1] + '2'
                    orderEvent.dict_['VolumeTotalOriginal'] = int(pos1)
                    orderEvent.dict_['preOrderRef'] = preOrderRef
                    if TradeTime.time() not in dictGoodsSend[goodsCode]:
                        if isOrderCommand:
                            ee.put(orderEvent)
                    else:
                        downLogBarDeal("{} 将委托单记录，待进行交易时间后，才进行下单操作".format(instrument), freq)
                        dfInstrumentNextOrder.loc[dfInstrumentNextOrder.shape[0]] = [instrument, freq, getTradeTimeNextBar(goodsCode, 1, TradeTime) - timedelta(minutes=1), getTradeTimeNextBar(goodsCode, freq, TradeTime), orderEvent]
                elif pos2 > 0:
                    downLogBarDeal("因为这个下单为止盈止损单，现在持有空仓，现在下空止盈单", freq)
                    orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                    orderEvent.dict_['InstrumentID'] = instrument
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                    orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                    orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                    orderEvent.dict_['LimitPrice'] = thePrice
                    orderEvent.dict_['orderRef'] = sendTradeDr['本地下单码'][:-1] + '2'
                    orderEvent.dict_['VolumeTotalOriginal'] = int(pos2)
                    orderEvent.dict_['preOrderRef'] = preOrderRef
                    if TradeTime.time() not in dictGoodsSend[goodsCode]:
                        if isOrderCommand:
                            ee.put(orderEvent)
                    else:
                        downLogBarDeal("{} 将委托单记录，待进行交易时间后，才进行下单操作".format(instrument), freq)
                        dfInstrumentNextOrder.loc[dfInstrumentNextOrder.shape[0]] = [instrument, freq, getTradeTimeNextBar(goodsCode, 1, TradeTime) - timedelta(minutes=1), getTradeTimeNextBar(goodsCode, freq, TradeTime), orderEvent]
                # endregion
                event = Event(type_=EVENT_SHOWCOMMAND)  # 将指令插入到指令表上
                event.dict_ = sendTradeDr
                ee.put(event)
    else:
        if TradeDuoOkStatus == 5:
            downLogBarDeal("{} 周交易明细表持多仓位，实际没持仓，不开仓".format(instrument), freq)
        elif TradeKongOkStatus == 5:
            downLogBarDeal("{} 周交易明细表持空仓位，实际没持仓，不开仓".format(instrument), freq)
# endregion



