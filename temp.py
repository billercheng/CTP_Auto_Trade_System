from WindPy import *
from chgAdj import *
from completeDB import *
from function import *
from openpyxl import *

# 求出做多开仓系数
def getDuoOpenMux(listTemp):
    seriesTemp = pd.Series(listTemp)
    HighQ1 = np.percentile(seriesTemp, 90)  # 取 90 的分位线
    HighQ1MeanList = seriesTemp[seriesTemp >= HighQ1]  # 大于 90 的分位线
    OpenMux = max(HighQ1MeanList.mean(), 1)  # 取 平均值 与 1 的最大值
    return OpenMux

# 求出做空开仓系数
def getKongOpenMux(listTemp):
    seriesTemp = pd.Series(listTemp)
    LowQ1 = np.percentile(listTemp, 10)  # 取 10 的分位线
    LowQ1MeanList = seriesTemp[seriesTemp <= LowQ1]  # 小于 10 的分位线
    OpenMux = min(LowQ1MeanList.mean(), -1)  # 取 平均值 与 1 的最大值
    return OpenMux

# 开仓线，止盈线，止损线的价格调整
def changePriceLine(price, MinChangUnit, DuoOrKong, OpenOrClose):
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

# 做多分析图，求交易明细表， 表名称， 调整表， 重叠度表格， 重叠度长度， 重叠度阈值
def completeDuo(dfTrade, AdjTab, MaDataTab, ODDataTab,StdMvLen, ODMvLen, ODth, theWeek, freq, dfDecompTitleFilter):
    # region 计算做多开平仓线及其收益
    OpenFlag = False  # 开仓标识
    BuyPrice = 0  # 开仓时记录开仓价
    MaxPrice = 0  # 开仓后，记录到止盈止损时的最高价格
    MaxLoss1 = 0  # 开仓1对应的全局最大回撤
    CangWei = 0  # 仓位
    MaDataTab['high'] = MaDataTab['high'].shift(1)  # 最高价向下移动一位
    MaDataTab['low'] = MaDataTab['low'].shift(1)  # 最低价向下移动一位
    mapriceODMvLen = MaDataTab['maprice_{}'.format(ODMvLen)].shift(1)  # 均值向下移动一位
    stdMa = MaDataTab['maprice_{}'.format(StdMvLen)].shift(abs(int(StdMvLen * 0.5 * (-1)) - 1))  # 直接向下移动 (StdMvLen * 0.5 * (-1) - 1)
    odMa = MaDataTab['maprice_{}'.format(ODMvLen)].shift(abs(int(ODMvLen * 1 * (-1)) - 1))  # 直接向下移动 (ODMvLen * 1 * (-1) - 1)
    goodsName = dfTrade['品种名称'][0]
    goodsCode = dfTrade['品种代码'][0]
    # endregion
    for i in range(dfTrade.shape[0]):
        HighPrice = dfTrade['最高价'][i]
        LowPrice = dfTrade['最低价'][i]
        ClosePrice = dfTrade['实时价'][i]
        openingTime = dfTrade.index[i]  # 获取完整时间
        HighLastPrice = MaDataTab['high'][openingTime]
        LowLastPrice = MaDataTab['low'][openingTime]
        if not OpenFlag:  # 如果还未开仓，则进行开仓判断
            if LowPrice <= dfTrade['开仓线(多)'][i] and HighPrice >= dfTrade['开仓线(多)'][i]:  # 是否符合开仓线情况
                if goodsCode.split('.')[1] == 'CFE':  # 这个条件主要判断首周的第一个Bar有可能不会进行开仓操作
                    if i == 0 and (openingTime - timedelta(minutes=freq)).time() < time(9, 15):
                        continue
                else:
                    if i == 0 and (openingTime - timedelta(minutes=freq)).time() < time(9):
                        continue
                if i == dfTrade.shape[0] - 1:  # 如果是本周最后一笔交易，不开仓
                    continue
                LowOverlapDegree = ODDataTab['重叠度高_{}'.format(ODMvLen)][ODDataTab.index < openingTime][-1]  # 获取重叠库数值
                # 判断是否满足重叠度(不需要)
                # if ODth > 0:
                #     if LowOverlapDegree <= ODth:
                #         continue
                # else:
                #     if ODth == -100:
                #         if LowOverlapDegree != -100:
                #             continue
                # 判断当前是否为换主力合约
                if openingTime in AdjTab['adjdate'].tolist():
                    continue
                # 条件1：如果止损幅度小于5个单量，不进行开仓操作
                if abs(dfTrade['开仓线(多)'][i] - dfTrade['止损线(多)'][i]) <= 5 * dictGoodsUnit[goodsCode]:
                    continue
                # 条件2：开仓参数为1时，不会开仓
                if float(dfTrade['做多参数'][i].split(',')[0]) == 1:
                    continue
                # 条件3：最高价少于重叠度长度均值的话，不开仓
                if HighLastPrice < mapriceODMvLen[openingTime]:
                    continue
                # 条件4：

                if (dfTrade['均值'][i] < stdMa[openingTime] ) or (mapriceODMvLen[openingTime] < odMa[openingTime]):
                    continue
                dfTrade['开平仓标识(多)'][i] = 1
                OpenFlag = True
                BuyPrice = dfTrade['开仓线(多)'][i]
                BuyIndex = i
                CangWei = dfTrade['仓位(多)'][i]
                # 仓位减半调整
                if LowLastPrice > (dfTrade['开仓线(多)'][i] + dfTrade['止损线(多)'][i]) / 2:
                    CangWei /= 2
                dfTrade['浮赢亏(多)'][i] = CangWei * (ClosePrice - BuyPrice) / BuyPrice  # 凡是有仓位的，一定是使用百分比计算收益情况。
                MaxPrice = BuyPrice
                MaxLoss1 = 0
                if ClosePrice >= MaxPrice:
                    MaxPrice = ClosePrice
                    dfTrade['最大回撤(多)'][i] = min(MaxLoss1, 0)
                else:
                    dfTrade['最大回撤(多)'][i] = MaxLoss1 = min(MaxLoss1, CangWei * (ClosePrice - MaxPrice) / MaxPrice)
        else:
            # 计算多头时最大回撤
            if ClosePrice >= MaxPrice:
                MaxPrice = ClosePrice
                dfTrade['最大回撤(多)'][i] = min(MaxLoss1, 0)
            else:
                dfTrade['最大回撤(多)'][i] = MaxLoss1 = min(MaxLoss1, CangWei * (ClosePrice - MaxPrice) / MaxPrice)
            dfTrade['浮赢亏(多)'][i] = CangWei * (ClosePrice - dfTrade['开仓线(多)'][i]) / dfTrade['开仓线(多)'][i]
            # 统计平仓情况
            if HighPrice >= dfTrade['止损线(多)'][i] and LowPrice <= dfTrade['止损线(多)'][i]:  # 止损情况
                dfTrade['开平仓标识(多)'][i] = -2
                OpenFlag = False
                SellPrice = dfTrade['止损线(多)'][i]
                dfTrade['总收益(多)'][i] = CangWei * (dfTrade['止损线(多)'][i] - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(多)'][i] = CangWei * (dfTrade['止损线(多)'][i] - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
            elif HighPrice < dfTrade['止损线(多)'][i] and LowPrice < dfTrade['止损线(多)'][i]:  # 止损情况
                dfTrade['开平仓标识(多)'][i] = -2
                OpenFlag = False
                SellPrice = HighPrice
                dfTrade['总收益(多)'][i] = CangWei * (HighPrice - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(多)'][i] = CangWei * (HighPrice - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
            elif HighPrice >= dfTrade['止盈线(多)'][i] and LowPrice <= dfTrade['止盈线(多)'][i]:  # 止盈情况
                dfTrade['开平仓标识(多)'][i] = -1
                OpenFlag = False
                SellPrice = dfTrade['止盈线(多)'][i]
                dfTrade['总收益(多)'][i] = CangWei * (dfTrade['止盈线(多)'][i] - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(多)'][i] = CangWei * (dfTrade['止盈线(多)'][i] - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
            elif HighPrice > dfTrade['止盈线(多)'][i] and LowPrice > dfTrade['止盈线(多)'][i]:  # 止盈情况
                dfTrade['开平仓标识(多)'][i] = -1
                OpenFlag = False
                SellPrice = LowPrice
                dfTrade['总收益(多)'][i] = CangWei * (LowPrice - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(多)'][i] = CangWei * (LowPrice - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
            elif openingTime in AdjTab['adjdate'].tolist() or i == dfTrade.shape[0] - 1:  # 切换主力合约情况和周结束情况
                dfTrade['总收益(多)'][i] = CangWei * (ClosePrice - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(多)'][i] = CangWei * (ClosePrice - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
                if dfTrade['总收益(多)'][i] >= 0:
                    dfTrade['开平仓标识(多)'][i] = -1
                else:
                    dfTrade['开平仓标识(多)'][i] = -2
                OpenFlag = False
                SellPrice = dfTrade['实时价'][i]
            if dfTrade['开平仓标识(多)'][i] in [-1, -2]:
                if dfTrade['开平仓标识(多)'][i] == -1:
                    tradeResult = '止盈'
                else:
                    tradeResult = '止损'
                # 总收益（多减去手续费吧）, 1：判断为 万 或者为 手
                if dictGoodsTransaction[goodsCode][-1] == "万":
                    dfTrade['总收益(多)'][i] -= 2 * CangWei * float(dictGoodsTransaction[goodsCode][:-1]) * 0.0001 * 1.05
                elif dictGoodsTransaction[goodsCode][-1] == "手":
                    dfTrade['总收益(多)'][i] -= 2 * (CangWei * float(dictGoodsTransaction[goodsCode][:-1])/ (BuyPrice * dictGoodsCheng[goodsCode])) * 1.05
                dfDecompTitleFilter.loc[dfDecompTitleFilter.shape[0]] = [goodsName, dfTrade.index[BuyIndex], dfTrade.index[i], 0, 0, 0, 0, '多', tradeResult, dfTrade['参数'][i], dfTrade['均值'][BuyIndex],
                                                             dfTrade['标准差'][BuyIndex], float(dfTrade['做多参数'][BuyIndex].split(',')[0]), float(dfTrade['做多参数'][BuyIndex].split(',')[2]),
                                                             float(dfTrade['做多参数'][BuyIndex].split(',')[1]), dfTrade['均值'][i], dfTrade['标准差'][i], float(dfTrade['做多参数'][i].split(',')[0]),
                                                             float(dfTrade['做多参数'][i].split(',')[2]) - float(dfTrade['做多参数'][i].split(',')[0]), float(dfTrade['做多参数'][i].split(',')[2]) - float(dfTrade['做多参数'][i].split(',')[0]), CangWei,
                                                             BuyPrice, 0, 0, 0, dfTrade['总收益(多)'][i], '{}'.format(theWeek),
                                                             SellPrice, '', freq]
    # region 开始统计
    dictTemp = {}
    dictTemp['收益率(做多)'] = dfTrade["总收益(多)"].dropna().sum()
    dictTemp['回撤(做多)'] = dfTrade["最大回撤(多)"].dropna().min()
    dictTemp['频率(做多)'] = dfTrade["总收益(多)"].dropna().shape[0]
    if dictTemp['频率(做多)'] == 0:
        dictTemp['胜率(做多)'] = 0
    else:
        dictTemp['胜率(做多)'] = dfTrade["总收益(多)"][dfTrade["总收益(多)"] > 0].shape[0] / dictTemp['频率(做多)']
    dictTemp['持仓天数(多)'] = dfTrade["浮赢亏(多)"].dropna().shape[0]
    dictTemp['起点(多)'] = 1
    dictTemp['终点(多)'] = dfTrade.shape[0]
    TotalProfit = 1
    TotalProfitVector = [1]
    for each in dfTrade["浮赢亏(多)"].dropna().tolist():
        TotalProfit = TotalProfit * (1 + each)
        TotalProfitVector.append(each)
    dictTemp['净收益(多)'] = TotalProfit - 1
    dictTemp['净收益回撤(多)'] = 0
    # endregion
    return dictTemp

# 做空分析图
def completeKong(dfTrade, AdjTab, MaDataTab, ODDataTab,StdMvLen, ODMvLen, ODth, theWeek, freq, dfDecompTitleFilter):
    # region 计算做空开平仓线及其收益
    OpenFlag = False  # 开仓标识
    BuyPrice = 0  # 开仓时记录开仓价
    MinPrice = 0  # 开仓后，记录到止盈止损时的最高价格
    MaxLoss1 = 0  # 开仓1对应的全局最大回撤
    CangWei = 0  # 仓位
    MaDataTab['high'] = MaDataTab['high'].shift(1)  # 最高价向下移动一位
    MaDataTab['low'] = MaDataTab['low'].shift(1)  # 最高价向下移动一位
    mapriceODMvLen = MaDataTab['maprice_{}'.format(ODMvLen)].shift(1)  # 重叠度长度对应均值标识
    stdMa = MaDataTab['maprice_{}'.format(StdMvLen)].shift(abs(int(StdMvLen * 0.5 * (-1)) - 1))  # 直接向下移动 (StdMvLen * 0.5 * (-1) - 1)

    odMa = MaDataTab['maprice_{}'.format(ODMvLen)].shift(abs(int(ODMvLen * 1 * (-1))))  # 直接向下移动 (ODMvLen * 1 * (-1) - 1)

    goodsName = dfTrade['品种名称'][0]
    goodsCode = dfTrade['品种代码'][0]
    # endregion
    for i in range(dfTrade.shape[0]):
        HighPrice = dfTrade['最高价'][i]
        LowPrice = dfTrade['最低价'][i]
        ClosePrice = dfTrade['实时价'][i]
        openingTime = dfTrade.index[i]  # 获取当前Bar时间
        HighLastPrice = MaDataTab['high'][openingTime]
        LowLastPrice = MaDataTab['low'][openingTime]
        if not OpenFlag:  # 如果还未开仓，则进行开仓判断
            if LowPrice <= dfTrade['开仓线(空)'][i] and HighPrice >= dfTrade['开仓线(空)'][i]:  # 是否符合开仓线情况
                if goodsCode.split('.')[1] == 'CFE':  # 如果每一笔交易跨周的话，不进行开仓操作
                    if i == 0 and (openingTime - timedelta(minutes=freq)).time() < time(9, 15):
                        continue
                else:
                    if i == 0 and (openingTime - timedelta(minutes=freq)).time() < time(9):
                        continue
                if i == dfTrade.shape[0] - 1:  # 如果是本周最后一笔交易，不开仓
                    continue
                LowOverlapDegree = ODDataTab[ODDataTab.index < openingTime].iloc[-1]['重叠度低_{}'.format(ODMvLen)]  # 获取重叠库数值
                # 判断是否满足重叠度
                # if ODth > 0:
                #     if LowOverlapDegree <= ODth:
                #         continue
                # else:
                #     if ODth == -100:
                #         if LowOverlapDegree != -100:
                #             continue
                # 判断当前是否为换主力合约
                if openingTime in AdjTab['adjdate'].tolist():
                    continue
                # 条件1：如果止损幅度小于5个单量，不进行开仓操作
                if abs(dfTrade['开仓线(空)'][i] - dfTrade['止损线(空)'][i]) <= 5 * dictGoodsUnit[goodsCode]:
                    continue
                # 条件2：开仓参数为1时，不会开仓
                if float(dfTrade['做空参数'][i].split(',')[0]) == -1:
                    continue
                # 条件3：最高价少于重叠度长度均值的话，不开仓
                if LowLastPrice > mapriceODMvLen[openingTime]:
                    continue
                # 条件4：
                if (dfTrade['均值'][i] > stdMa[openingTime]) or (mapriceODMvLen[openingTime] > odMa[openingTime]):
                    continue
                dfTrade['开平仓标识(空)'][i] = 1
                OpenFlag = True
                BuyPrice = dfTrade['开仓线(空)'][i]
                BuyIndex = i
                CangWei = dfTrade['仓位(空)'][i]
                if HighLastPrice < (dfTrade['开仓线(空)'][i] + dfTrade['止损线(空)'][i]) / 2:
                    CangWei /= 2
                dfTrade['浮赢亏(空)'][i] = (-1) * CangWei * (ClosePrice - BuyPrice) / BuyPrice
                MinPrice = BuyPrice
                MaxLoss1 = 0
                if ClosePrice <= MinPrice:
                    MinPrice = ClosePrice
                    dfTrade['最大回撤(空)'][i] = min(MaxLoss1, 0)
                else:
                    dfTrade['最大回撤(空)'][i] = MaxLoss1 = min(MaxLoss1, CangWei * (ClosePrice - MinPrice) / MinPrice * (-1))
        else:
            # 计算空头时最大回撤
            if ClosePrice <= MinPrice:
                MinPrice = ClosePrice
                dfTrade['最大回撤(空)'][i] = min(MaxLoss1, 0)
            else:
                dfTrade['最大回撤(空)'][i] = MaxLoss1 = min(MaxLoss1, CangWei * (ClosePrice - MinPrice) / MinPrice * (-1))
            dfTrade['浮赢亏(空)'][i] = (-1) * CangWei * (ClosePrice - dfTrade['开仓线(空)'][i]) / dfTrade['开仓线(空)'][i]
            # 统计平仓情况
            if HighPrice >= dfTrade['止损线(空)'][i] and LowPrice <= dfTrade['止损线(空)'][i]:  # 止损情况
                dfTrade['开平仓标识(空)'][i] = -2
                OpenFlag = False
                SellPrice = dfTrade['止损线(空)'][i]
                dfTrade['总收益(空)'][i] = (-1) * CangWei * (dfTrade['止损线(空)'][i] - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(空)'][i] = (-1) * CangWei * (dfTrade['止损线(空)'][i] - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
            elif HighPrice > dfTrade['止损线(空)'][i] and LowPrice > dfTrade['止损线(空)'][i]:  # 止损情况
                dfTrade['开平仓标识(空)'][i] = -2
                OpenFlag = False
                SellPrice = HighPrice
                dfTrade['总收益(空)'][i] = (-1) * CangWei * (LowPrice - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(空)'][i] = (-1) * CangWei * (LowPrice - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
            elif HighPrice >= dfTrade['止盈线(空)'][i] and LowPrice <= dfTrade['止盈线(空)'][i]:  # 止盈情况
                dfTrade['开平仓标识(空)'][i] = -1
                OpenFlag = False
                SellPrice = dfTrade['止盈线(空)'][i]
                dfTrade['总收益(空)'][i] = (-1) * CangWei * (dfTrade['止盈线(空)'][i] - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(空)'][i] = (-1) * CangWei * (dfTrade['止盈线(空)'][i] - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
            elif HighPrice < dfTrade['止盈线(空)'][i] and LowPrice < dfTrade['止盈线(空)'][i]:  # 止盈情况
                dfTrade['开平仓标识(空)'][i] = -1
                OpenFlag = False
                SellPrice = HighPrice
                dfTrade['总收益(空)'][i] = (-1) * CangWei * (HighPrice - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(空)'][i] = (-1) * CangWei * (HighPrice - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
            elif openingTime in AdjTab['adjdate'].tolist() or i == dfTrade.shape[0] - 1:  # 切换主力合约情况和周结束情况
                dfTrade['总收益(空)'][i] = (-1) * CangWei * (ClosePrice - BuyPrice) / BuyPrice
                dfTrade['浮赢亏(空)'][i] = (-1) * CangWei * (ClosePrice - dfTrade['实时价'][i - 1]) / dfTrade['实时价'][i - 1]
                if dfTrade['总收益(空)'][i] >= 0:
                    dfTrade['开平仓标识(空)'][i] = -1
                else:
                    dfTrade['开平仓标识(空)'][i] = -2
                OpenFlag = False
                SellPrice = dfTrade['实时价'][i]
            if dfTrade['开平仓标识(空)'][i] in [-1, -2]:
                if dfTrade['开平仓标识(空)'][i] == -1:
                    tradeResult = '止盈'
                else:
                    tradeResult = '止损'
                # 计算空的手续费，手续费与做多的一样吧
                if dictGoodsTransaction[goodsCode][-1] == "万":
                    dfTrade['总收益(空)'][i] -= 2 * CangWei * float(dictGoodsTransaction[goodsCode][:-1]) * 0.0001 * 1.05
                elif dictGoodsTransaction[goodsCode][-1] == "手":
                    dfTrade['总收益(空)'][i] -= 2 * (CangWei * float(dictGoodsTransaction[goodsCode][:-1])/ (BuyPrice * dictGoodsCheng[goodsCode])) * 1.05
                dfDecompTitleFilter.loc[dfDecompTitleFilter.shape[0]] = [goodsName, dfTrade.index[BuyIndex], dfTrade.index[i], 0, 0, 0, 0, '空', tradeResult, dfTrade['参数'][i], dfTrade['均值'][BuyIndex],
                                                             dfTrade['标准差'][BuyIndex], float(dfTrade['做空参数'][BuyIndex].split(',')[0]), float(dfTrade['做空参数'][BuyIndex].split(',')[2]),
                                                             float(dfTrade['做空参数'][BuyIndex].split(',')[1]), dfTrade['均值'][i], dfTrade['标准差'][i], float(dfTrade['做空参数'][i].split(',')[0]),
                                                             float(dfTrade['做空参数'][i].split(',')[2]) - float(dfTrade['做空参数'][i].split(',')[0]), float(dfTrade['做空参数'][i].split(',')[2]) - float(dfTrade['做空参数'][i].split(',')[0]), CangWei,
                                                             BuyPrice, 0, 0, 0, dfTrade['总收益(空)'][i], '{}'.format(theWeek),
                                                             SellPrice, '', freq]
    # region 开始统计
    dictTemp = {}
    dictTemp['收益率(做空)'] = dfTrade["总收益(空)"].dropna().sum()
    dictTemp['回撤(做空)'] = dfTrade["最大回撤(空)"].dropna().min()
    dictTemp['频率(做空)'] = dfTrade["总收益(空)"].dropna().shape[0]
    if dictTemp['频率(做空)'] == 0:
        dictTemp['胜率(做空)'] = 0
    else:
        dictTemp['胜率(做空)'] = dfTrade["总收益(空)"][dfTrade["总收益(空)"] > 0].shape[0] / dictTemp['频率(做空)']
    dictTemp['持仓天数(空)'] = dfTrade["浮赢亏(空)"].dropna().shape[0]
    dictTemp['起点(空)'] = 1
    dictTemp['终点(空)'] = dfTrade.shape[0]
    TotalProfit = 1
    TotalProfitVector = [1]
    for each in dfTrade["浮赢亏(空)"].dropna().tolist():
        TotalProfit = TotalProfit * (1 + each)
        TotalProfitVector.append(each)
    dictTemp['净收益(空)'] = TotalProfit - 1
    dictTemp['净收益回撤(空)'] = 0
    # endregion
    return dictTemp

# 求出本周的收益分解表
def obtainTransactionRecords(week):
    # 读取 CTA交易参数表-综合表
    files = os.listdir('CTA交易参数表(计算本周交易记录)')
    # 读取本周的时间范围
    dfWeek = pd.read_excel("TableFile\\公共参数.xlsx", sheet_name="周时间序列表")
    dfWeek['起始时间'] += timedelta(hours=1)
    dfWeek['结束时间'] += timedelta(days=1, hours=23, minutes=59)
    week = str(week) + '周'
    for i in range(dfWeek.shape[0]):
        if dfWeek['类型'][i] == week:
            weekStartTime = dfWeek['起始时间'][i]
            weekEndTime = dfWeek['结束时间'][i]
    for file in files:  # 循环每一个文件
        ws = load_workbook('CTA交易参数表(计算本周交易记录)' + '\\' + file)  # 获取表名
        names = ws.get_sheet_names()  # 能够获取所有的表名
        # 收益分解表（优选频段）
        listDecompTitle = ['品种名称', '起始时间', '结束时间', '趋势收益', '波动收益', '预期收益', '最终收益', '多空方向', '止盈止损类型', '滑动区间长度',
                           '开仓时均值', '开仓时标准差', '开仓时开仓倍数', '开仓时止盈倍数', '开仓时止损倍数', '平仓时均值', '平仓时标准差', '平仓时开仓倍数', '平仓时止盈倍数', '平仓时止损倍数',
                           '仓位', '开仓线', '趋势差', '波动差', '预期差', '总收益', '周次', '平仓线', '持仓状态', '频段']
        dfDecompTitleFilter = pd.DataFrame(columns=listDecompTitle)
        dfDecompTitleFilterAll = pd.DataFrame(columns=listDecompTitle)
        # 交易明细表
        listTradeTitle = ['品种名称', '品种代码', '交易日期', '均值', '标准差', '实时价', '标准差倍数', '开仓线(空)', '止损线(空)', '止盈线(空)', '开平仓标识(空)', '总收益(空)', '最大回撤(空)',
                          '开仓线(多)', '止损线(多)', '止盈线(多)', '开平仓标识(多)', '总收益(多)', '最大回撤(多)', '参数编号', '参数', '浮赢亏(空)', '浮赢亏(多)', '标准差倍数(高)', '标准差倍数(低)', '最高价', '最低价', '做空参数', '做多参数',
                          '仓位(多)', '仓位(空)', '日滑动均值', '日滑动标准差', '日收盘价', '日标准差倍数', '周次', '持仓状态', '频段']
        dfTradeTitle = pd.DataFrame(columns=listTradeTitle).set_index('交易日期')  # 周交易显示表
        # 参数统计表
        listStatusDetailTitle = ['品种名称', '品种代码', '年份', '收益率(做多)', '回撤(做多)', '胜率(做多)', '频率(做多)','收益率(做空)',
                                 '回撤(做空)', '胜率(做空)', '频率(做空)','持仓天数(多)', '持仓天数(空)', '起点(多)', '终点(多)', '起点(空)',
                                 '终点(空)', '参数编号', '参数', '净收益(多)','净收益回撤(多)', '净收益(空)', '净收益回撤(空)', '周次']
        StatusDetailTitle = pd.DataFrame(columns=listStatusDetailTitle)
        for freq in listFreq[1:]:  # 不需要计算CTA1的交易记录
            dfParameter = pd.read_excel('CTA交易参数表(计算本周交易记录)' + '\\' + file, sheet_name='CTA{}'.format(freq))
            for i in range(dfParameter.shape[0]):
                goodsName = dfParameter['品种名称'][i]
                goodsCode = dfParameter['品种代码'][i]
                if goodsCode.split('.')[1] in ['DCE', 'SHF']:
                    goodsCode = goodsCode.split('.')[0].lower() + '.' + goodsCode.split('.')[1]
                else:
                    goodsCode = goodsCode.split('.')[0].upper() + '.' + goodsCode.split('.')[1]
                isTrading = True if dfParameter['日盘交易标识'][i] == 1 else False
                StdMvLen = dfParameter['均值滑动长度'][i]
                ODMvLen = dfParameter['重叠度滑动长度'][i]
                ODth = dfParameter['重叠度阈值'][i]
                if isTrading:  # 如何今天是有交易的话
                    # 获取均值的数据
                    dictTemp = dictData[freq]["{}_均值表".format(goodsName)]
                    MaDataTab = dictTemp[(dictTemp.index < weekStartTime)][-260:]
                    MaDataTab = MaDataTab.append(dictTemp[(dictTemp.index > weekStartTime)&(dictTemp.index < weekEndTime)])
                    # 获取重叠度数据
                    dictTempOD = dictData[freq]["{}_重叠度表".format(goodsName)]
                    ODDataTab = dictTempOD[(dictTempOD.index < weekStartTime)][-1:]
                    ODDataTab = ODDataTab.append(dictTempOD[(dictTempOD.index > weekStartTime) & (dictTempOD.index < weekEndTime)])
                    listTradeTime = list(dictTemp[(dictTemp.index > weekStartTime)&(dictTemp.index < weekEndTime)].index)
                    dfTrade = pd.DataFrame({'交易日期': listTradeTime}).set_index('交易日期')
                    dfTrade['品种名称'] = goodsName
                    dfTrade['品种代码'] = goodsCode
                    dfTrade['参数'] = "({},1)({},{})".format(StdMvLen, ODMvLen, ODth)
                    # 均值，标准差，标准差倍数，标准差倍数高，标准差倍数低
                    dfTrade['均值'] = MaDataTab['maprice_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['标准差'] = MaDataTab['stdprice_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['实时价'] = MaDataTab['close'].loc[listTradeTime]
                    dfTrade['标准差倍数'] = MaDataTab['stdmux_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['最高价'] = MaDataTab['high'].loc[listTradeTime]
                    dfTrade['标准差倍数(高)'] = MaDataTab['highstdmux_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['最低价'] = MaDataTab['low'].loc[listTradeTime]
                    dfTrade['标准差倍数(低)'] = MaDataTab['lowstdmux_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['日滑动均值'] = 0
                    dfTrade['日收盘价'] = 0
                    dfTrade['日标准差倍数'] = 0
                    dfTrade['周次'] = "{}".format(week)
                    dfTrade['持仓状态'] = ''
                    # region 开仓线多 做法
                    OpenMux = MaDataTab['highstdmux_{}'.format(StdMvLen)].rolling(StdMvLen).apply(getDuoOpenMux)
                    StopAbtainMux = OpenMux + ((MaDataTab['highstdmux_{}'.format(StdMvLen)].rolling(StdMvLen).max() - OpenMux) * 1.2).apply(lambda x: max(x, 1))
                    StopLossMux = OpenMux - (StopAbtainMux - OpenMux)
                    dfTrade['做多参数'] = (OpenMux.round(4).astype('str') + ', ' + StopLossMux.round(4).astype('str') + ', ' + StopAbtainMux.round(4).astype('str')).shift(1)
                    dfTrade['开仓线(多)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + OpenMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['止盈线(多)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + StopAbtainMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['止损线(多)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + StopLossMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['仓位(多)'] = ((-1) * 0.001 / ((dfTrade['止损线(多)'] - dfTrade['开仓线(多)']) / dfTrade['开仓线(多)']))
                    dfTrade['开仓线(多)'] = dfTrade['开仓线(多)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "多", "开仓"])
                    dfTrade['止盈线(多)'] = dfTrade['止盈线(多)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "多", "止盈"])
                    dfTrade['止损线(多)'] = dfTrade['止损线(多)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "多", "止损"])
                    # endregion
                    # region 开仓线空 做法
                    OpenMux = MaDataTab['lowstdmux_{}'.format(StdMvLen)].rolling(StdMvLen).apply(getKongOpenMux)
                    StopAbtainMux = OpenMux + ((MaDataTab['lowstdmux_{}'.format(StdMvLen)].rolling(StdMvLen).min() - OpenMux) * 1.2).apply(lambda x: min(x, -1))
                    StopLossMux = OpenMux - 1 * (StopAbtainMux - OpenMux)
                    dfTrade['做空参数'] = (OpenMux.round(4).astype('str') + ', ' + StopLossMux.round(4).astype('str') + ', ' + StopAbtainMux.round(4).astype('str')).shift(1)
                    dfTrade['开仓线(空)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + OpenMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['止盈线(空)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + StopAbtainMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['止损线(空)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + StopLossMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['仓位(空)'] = (0.001 / ((dfTrade['止损线(空)'] - dfTrade['开仓线(空)']) / dfTrade['开仓线(空)']))
                    dfTrade['开仓线(空)'] = dfTrade['开仓线(空)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "空", "开仓"])
                    dfTrade['止盈线(空)'] = dfTrade['止盈线(空)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "空", "止盈"])
                    dfTrade['止损线(空)'] = dfTrade['止损线(空)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "空", "止损"])
                    # endregion
                    dfTrade['开平仓标识(空)'] = np.NAN
                    dfTrade['总收益(空)'] = np.NAN
                    dfTrade['最大回撤(空)'] = np.NAN
                    dfTrade['开平仓标识(多)'] = np.NAN
                    dfTrade['总收益(多)'] = np.NAN
                    dfTrade['最大回撤(多)'] = np.NAN
                    dfTrade['浮赢亏(空)'] = np.NAN
                    dfTrade['浮赢亏(多)'] = np.NAN
                    # 进行各参数统计表进行统计
                    dictTemp = {}
                    dictTemp['品种名称'] = goodsName
                    dictTemp['品种代码'] = goodsCode
                    dictTemp["年份"] = "{}分钟".format(freq)
                    # dictData[1]['{}_调整时刻表'.format(goodsName)]， 需要注意一个COPY的改法，就影响这么多了吗？
                    dfGoodsAdj = dictData[1]['{}_调整时刻表'.format(goodsName)].copy()
                    if goodsCode.split('.')[1] != 'CFE':
                        dfGoodsAdj['adjdate'] -= timedelta(hours=1)
                    else:
                        dfGoodsAdj['adjdate'] -= timedelta(minutes=45)
                    dictTemp.update(completeDuo(dfTrade, dfGoodsAdj, MaDataTab.copy(), ODDataTab.copy(), StdMvLen, ODMvLen, ODth, week, freq, dfDecompTitleFilter))  # 需要注意的是，MaDataTab， ODDataTab 是引用传递的。
                    dictTemp.update(completeKong(dfTrade, dfGoodsAdj, MaDataTab.copy(), ODDataTab.copy(), StdMvLen, ODMvLen, ODth, week, freq, dfDecompTitleFilter))  # 需要注意的是，MaDataTab， ODDataTab 是引用传递的。
                    dictTemp["参数编号"] = 1
                    dictTemp["参数"] = "({},1)({},{})".format(StdMvLen, ODMvLen, ODth)
                    dictTemp["周次"] = "{}".format(week)
                    dfTrade["频段"] = freq
                    dfTradeTitle = dfTradeTitle.append(dfTrade)
                    # StatusDetailTitle.loc[StatusDetailTitle.shape[0]] = dictTemp
                else:
                    continue
                    # 获取均值的数据
                    dictTemp = dictData[freq]["{}_均值表".format(goodsName)]
                    MaDataTab = dictTemp[(dictTemp.index < weekStartTime)][-260:]
                    MaDataTab = MaDataTab.append(dictTemp[(dictTemp.index > weekStartTime) & (dictTemp.index < weekEndTime)])
                    # 获取重叠度数据
                    dictTempOD = dictData[freq]["{}_重叠度表".format(goodsName)]
                    ODDataTab = dictTempOD[(dictTempOD.index < weekStartTime)][-1:]
                    ODDataTab = ODDataTab.append(dictTempOD[(dictTempOD.index > weekStartTime) & (dictTempOD.index < weekEndTime)])
                    listTradeTime = list(dictTemp[(dictTemp.index > weekStartTime) & (dictTemp.index < weekEndTime)].index)
                    dfTrade = pd.DataFrame({'交易日期': listTradeTime}).set_index('交易日期')
                    dfTrade['品种名称'] = goodsName
                    dfTrade['品种代码'] = goodsCode
                    dfTrade['参数'] = "({},1)({},{})".format(StdMvLen, ODMvLen, ODth)
                    # 均值，标准差，标准差倍数，标准差倍数高，标准差倍数低
                    dfTrade['均值'] = MaDataTab['maprice_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['标准差'] = MaDataTab['stdprice_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['实时价'] = MaDataTab['close'].loc[listTradeTime]
                    dfTrade['标准差倍数'] = MaDataTab['stdmux_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['最高价'] = MaDataTab['high'].loc[listTradeTime]
                    dfTrade['标准差倍数(高)'] = MaDataTab['highstdmux_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['最低价'] = MaDataTab['low'].loc[listTradeTime]
                    dfTrade['标准差倍数(低)'] = MaDataTab['lowstdmux_{}'.format(StdMvLen)].shift(1).loc[listTradeTime]
                    dfTrade['日滑动均值'] = 0
                    dfTrade['日收盘价'] = 0
                    dfTrade['日标准差倍数'] = 0
                    dfTrade['周次'] = "{}".format(week)
                    dfTrade['持仓状态'] = ''
                    # region 开仓线多 做法
                    OpenMux = MaDataTab['highstdmux_{}'.format(StdMvLen)].rolling(StdMvLen).apply(getDuoOpenMux)
                    StopAbtainMux = OpenMux + ((MaDataTab['highstdmux_{}'.format(StdMvLen)].rolling(StdMvLen).max() - OpenMux) * 1.2).apply(lambda x: max(x, 1))
                    StopLossMux = OpenMux - (StopAbtainMux - OpenMux)
                    dfTrade['做多参数'] = (OpenMux.round(4).astype('str') + ', ' + StopLossMux.round(4).astype('str') + ', ' + StopAbtainMux.round(4).astype('str')).shift(1)
                    dfTrade['开仓线(多)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + OpenMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['止盈线(多)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + StopAbtainMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['止损线(多)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + StopLossMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['仓位(多)'] = ((-1) * 0.001 / ((dfTrade['止损线(多)'] - dfTrade['开仓线(多)']) / dfTrade['开仓线(多)']))
                    dfTrade['开仓线(多)'] = dfTrade['开仓线(多)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "多", "开仓"])
                    dfTrade['止盈线(多)'] = dfTrade['止盈线(多)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "多", "止盈"])
                    dfTrade['止损线(多)'] = dfTrade['止损线(多)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "多", "止损"])
                    # endregion
                    # region 开仓线空 做法
                    OpenMux = MaDataTab['lowstdmux_{}'.format(StdMvLen)].rolling(StdMvLen).apply(getKongOpenMux)
                    StopAbtainMux = OpenMux + ((MaDataTab['lowstdmux_{}'.format(StdMvLen)].rolling(StdMvLen).min() - OpenMux) * 1.2).apply(lambda x: min(x, -1))
                    StopLossMux = OpenMux - 1 * (StopAbtainMux - OpenMux)
                    dfTrade['做空参数'] = (OpenMux.round(4).astype('str') + ', ' + StopLossMux.round(4).astype('str') + ', ' + StopAbtainMux.round(4).astype('str')).shift(1)
                    dfTrade['开仓线(空)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + OpenMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['止盈线(空)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + StopAbtainMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['止损线(空)'] = (MaDataTab['maprice_{}'.format(StdMvLen)] + StopLossMux * MaDataTab['stdprice_{}'.format(StdMvLen)]).shift(1)
                    dfTrade['仓位(空)'] = (0.001 / ((dfTrade['止损线(空)'] - dfTrade['开仓线(空)']) / dfTrade['开仓线(空)']))
                    dfTrade['开仓线(空)'] = dfTrade['开仓线(空)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "空", "开仓"])
                    dfTrade['止盈线(空)'] = dfTrade['止盈线(空)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "空", "止盈"])
                    dfTrade['止损线(空)'] = dfTrade['止损线(空)'].apply(changePriceLine, args=[dictGoodsUnit[goodsCode], "空", "止损"])
                    # endregion
                    dfTrade['开平仓标识(空)'] = np.NAN
                    dfTrade['总收益(空)'] = np.NAN
                    dfTrade['最大回撤(空)'] = np.NAN
                    dfTrade['开平仓标识(多)'] = np.NAN
                    dfTrade['总收益(多)'] = np.NAN
                    dfTrade['最大回撤(多)'] = np.NAN
                    dfTrade['浮赢亏(空)'] = np.NAN
                    dfTrade['浮赢亏(多)'] = np.NAN
                    dfTrade['频段'] = freq
                    # 进行各参数统计表进行统计
                    dictTemp = {}
                    dictTemp['品种名称'] = goodsName
                    dictTemp['品种代码'] = goodsCode
                    dictTemp["年份"] = "{}分钟".format(freq)
                    # dictData[1]['{}_调整时刻表'.format(goodsName)]
                    dfGoodsAdj = dictData[1]['{}_调整时刻表'.format(goodsName)]
                    if goodsCode.split('.')[1] != 'CFE':
                        dfGoodsAdj['adjdate'] -= timedelta(hours=1)
                    else:
                        dfGoodsAdj['adjdate'] -= timedelta(minutes=45)
                    dictTemp.update(completeDuo(dfTrade, dfGoodsAdj, MaDataTab.copy(), ODDataTab.copy(), StdMvLen, ODMvLen, ODth, week, freq, dfDecompTitleFilterAll))  # 需要注意的是，MaDataTab， ODDataTab 是引用传递的。
                    dictTemp.update(completeKong(dfTrade, dfGoodsAdj, MaDataTab.copy(), ODDataTab.copy(), StdMvLen, ODMvLen, ODth, week, freq, dfDecompTitleFilterAll))  # 需要注意的是，MaDataTab， ODDataTab 是引用传递的。
                    dictTemp["参数编号"] = 1
                    dictTemp["参数"] = "({},1)({},{})".format(StdMvLen, ODMvLen, ODth)
                    dictTemp["周次"] = "{}".format(week)
                    # StatusDetailTitle.loc[StatusDetailTitle.shape[0]] = dictTemp
        # 记录数据操作
        dfDecompTitleFilterAll = dfDecompTitleFilterAll.append(dfDecompTitleFilter)
        os.makedirs("输出统计表\\{}".format(file.split('.')[0]), exist_ok=True)
        dfDecompTitleFilter.to_excel("输出统计表\\{}\\收益分解表(本周交易数据).xlsx".format(file.split('.')[0]), sheet_name='收益分解表(本周交易数据)', index=False)
        w = pd.ExcelWriter("输出统计表\\{}\\收益分解表(本周所有交易数据).xlsx".format(file.split('.')[0]))
        dfDecompTitleFilterAll.to_excel(w, "收益分解表", index=False)
        # StatusDetailTitle.to_excel(w, "收益统计表", index=False)
        w.save()
        listTradeTitle.remove('交易日期')
        dfTradeTitle = dfTradeTitle[listTradeTitle]
        dfTradeTitle.to_csv("输出统计表\\{}\\周交易明细表.csv".format(file.split('.')[0]), encoding='gbk')

if __name__ == '__main__':
    # 获取一分钟数据，将数据库数据写入内存
    theWeek = 250
    listFreq = [1, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
    for freq in listFreq:
        dictData[freq] = {}
        con = dictCon[freq]
        print("将频段 {} 的数据写入内存".format(freq))
        for goodsCode in dictGoodsName.keys():
            goodsName = dictGoodsName[goodsCode]
            if freq == 1:
                dictData[freq]['{}_调整时刻表'.format(goodsName)] = pd.read_sql('select * from {}_调整时刻表'.format(goodsName), con).set_index('goods_code')
                dictData[freq]['{}_调整时刻表'.format(goodsName)]['adjdate'] = pd.to_datetime(dictData[freq]['{}_调整时刻表'.format(goodsName)]['adjdate']) + timedelta(hours=16)
                df = pd.read_sql('select * from {}_调整表 order by trade_time desc limit 10000'.format(goodsName), con)
                df = df.drop(['id'], axis=1).set_index('trade_time')
                dictData[freq]['{}_调整表'.format(goodsName)] = df.sort_index()
            else:
                dictData[freq][goodsName] = pd.read_sql(
                    "select * from {} order by trade_time desc limit {}".format(goodsName, 1000), con).set_index(
                    'trade_time').sort_index()
                dictData[freq][goodsName] = dictData[freq][goodsName].drop(['id'], axis=1)
                dictData[freq][goodsName + '_调整表'] = pd.read_sql(
                    "select * from {}_调整表 order by trade_time desc limit {}".format(goodsName, 1000), con).set_index(
                    'trade_time').sort_index()
                if 'open' in list(dictData[freq][goodsName + '_调整表'].columns):
                    dictData[freq][goodsName + '_调整表'] = dictData[freq][goodsName + '_调整表'].drop(['open'], axis = 1)
                dictData[freq][goodsName + '_调整表'] = dictData[freq][goodsName + '_调整表'].drop(['id'],axis=1)
                dictData[freq][goodsName + '_均值表'] = pd.read_sql(
                    "select * from {}_均值表 order by trade_time desc limit {}".format(goodsName, 1000), con).set_index(
                    'trade_time').sort_index()
                dictData[freq][goodsName + '_均值表'] = dictData[freq][goodsName + '_均值表'].drop(['id'], axis=1)
                dictData[freq][goodsName + '_重叠度表'] = pd.read_sql(
                    "select * from {}_重叠度表 order by trade_time desc limit {}".format(goodsName, 1000), con).set_index('trade_time').sort_index()
                listDrop = ['id']
                for eachMvl in mvlenvector:
                    listDrop.extend(
                        ['StdMux高均值_{}'.format(eachMvl), 'StdMux低均值_{}'.format(eachMvl), 'StdMux收均值_{}'.format(eachMvl),
                         '重叠度高收益_{}'.format(eachMvl), '重叠度低收益_{}'.format(eachMvl), '重叠度收收益_{}'.format(eachMvl)])
                dictData[freq][goodsName + '_重叠度表'] = dictData[freq][goodsName + '_重叠度表'].drop(listDrop, axis=1)
    print("计算CTA交易参数表（本周交易记录和统计）")
    obtainTransactionRecords(theWeek)







