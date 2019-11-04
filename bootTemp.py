import socket
from mdApi import MdApi
from tdApi import *
from orderUi import *
from onBar import *
import queue

class RdMdUi(QMainWindow):

    def __init__(self):
        super().__init__()
        self.getUi()
        self.registerEngine()
        threading.Thread(target=self.getData, daemon=True).start()

    # region 获取UI界面
    def getUi(self):
        self.font = QFont('微软雅黑', 16)
        self.setFont(self.font)
        self.setWindowTitle("CTA交易系统（" + programName + "）")
        self.setWindowIcon(QIcon('material\\icon.png'))
        self.setGeometry(200, 50, 1500, 1000)

        # region 内部Layout
        vbox0 = QVBoxLayout()
        gbox1 = QGroupBox('频段')
        gbox2 = QGroupBox('基本日志记录')
        vbox0.addWidget(gbox1, stretch=2)
        vbox0.addWidget(gbox2, stretch=1)
        # endregion

        # region 频段
        tab = QTabWidget()
        self.TableFreqDuo = {}
        self.TableFreqKong = {}
        self.TableFreqPosition = {}
        self.dictFreqOrderTable = {}
        self.TableFreqTrade = {}
        for freq in listFreq:
            tabSub = QTabWidget()
            # region 做多与做空
            tabSub1 = QWidget()
            tableDuo = QTableWidget(0, len(listDuoKong), self)
            self.TableFreqDuo[freq] = tableDuo
            tableDuo.setHorizontalHeaderLabels(listDuoKong)
            tableDuo.verticalHeader().setVisible(False)
            tableDuo.setFont(self.font)
            tableDuo.resizeColumnsToContents()
            tableKong = QTableWidget(0, len(listDuoKong), self)
            self.TableFreqKong[freq] = tableKong
            tableKong.setHorizontalHeaderLabels(listDuoKong)
            tableKong.verticalHeader().setVisible(False)
            tableKong.setFont(self.font)
            tableKong.resizeColumnsToContents()
            vbox = QVBoxLayout()
            vbox.addWidget(QLabel('做多', self))
            vbox.addWidget(tableDuo)
            vbox.addWidget(QLabel('做空', self))
            vbox.addWidget(tableKong)
            tabSub1.setLayout(vbox)
            # endregion
            # region 频段的持仓
            tabSub2 = QWidget()
            tablePosition = QTableWidget(0, len(listFreqPosition), self)
            tablePosition.setHorizontalHeaderLabels(listFreqPosition)
            for num in range(len(listFreqPosition)):
                tablePosition.horizontalHeaderItem(num).setFont(self.font)
            tablePosition.setFont(self.font)
            tablePosition.resizeColumnsToContents()
            tablePosition.verticalHeader().setVisible(False)
            self.TableFreqPosition[freq] = tablePosition
            dfTemp = dictFreqPosition[freq].copy()
            for row in range(dfTemp.shape[0]):
                self.TableFreqPosition[freq].setRowCount(self.TableFreqPosition[freq].rowCount() + 1)
                for column in range(len(listFreqPosition)):
                    self.TableFreqPosition[freq].setItem(self.TableFreqPosition[freq].rowCount() - 1, column,
                                                         QTableWidgetItem(str(dfTemp.iat[row, column])))
            self.TableFreqPosition[freq].resizeColumnsToContents()
            hbox = QHBoxLayout()
            hbox.addWidget(tablePosition)
            tabSub2.setLayout(hbox)
            # endregion
            # region 频段的成交
            tabSub3 = QWidget()
            tableTrade = QTableWidget(0, len(listFreqTrade), self)
            tableTrade.setHorizontalHeaderLabels(listFreqTrade)
            for num in range(len(listFreqTrade)):
                tableTrade.horizontalHeaderItem(num).setFont(self.font)
            tableTrade.setFont(self.font)
            tableTrade.resizeColumnsToContents()
            tableTrade.verticalHeader().setVisible(False)
            self.TableFreqTrade[freq] = tableTrade
            hbox = QHBoxLayout()
            hbox.addWidget(tableTrade)
            tabSub3.setLayout(hbox)
            # endregion
            # region 频段的委托
            tabSub4 = QWidget()
            tempTableOrder = QTableWidget(0, len(listFreqOrder), self)
            tempTableOrder.setHorizontalHeaderLabels(listFreqOrder)
            for num in range(len(listFreqOrder)):
                tempTableOrder.horizontalHeaderItem(num).setFont(self.font)
            tempTableOrder.setFont(self.font)
            tempTableOrder.resizeColumnsToContents()
            tempTableOrder.verticalHeader().setVisible(False)
            self.dictFreqOrderTable[freq] = tempTableOrder
            hbox = QHBoxLayout()
            hbox.addWidget(tempTableOrder)
            tabSub4.setLayout(hbox)
            # endregion
            tabSub.addTab(tabSub1, '做多与做空')
            tabSub.addTab(tabSub2, '频段持仓')
            tabSub.addTab(tabSub3, '频段成交')
            tabSub.addTab(tabSub4, '频段委托')
            tab.addTab(tabSub, 'CTA' + str(freq))
        hbox = QHBoxLayout()
        hbox.addWidget(tab)
        gbox1.setLayout(hbox)
        # endregion

        # region 基本日志记录
        hbox = QHBoxLayout()
        tab2 = QTabWidget()
        self.txtLog = QTextEdit(self)
        self.txtLog.setEnabled(True)
        tab2.addTab(self.txtLog, '程序日志')

        self.tableOrderDB = QTableWidget(0, len(listOrderDB), self)
        self.tableOrderDB.setHorizontalHeaderLabels(listOrderDB)
        for num in range(len(listOrderDB)):
            self.tableOrderDB.horizontalHeaderItem(num).setFont(self.font)
        self.tableOrderDB.setFont(self.font)
        self.tableOrderDB.resizeColumnsToContents()
        self.tableOrderDB.verticalHeader().setVisible(False)
        self.tableOrderDB.setEnabled(True)
        for eachRow in range(dfOrderDB.shape[0]):
            self.tableOrderDB.setRowCount(self.tableOrderDB.rowCount() + 1)
            for eachColumn in range(len(listOrderDB)):
                self.tableOrderDB.setItem(eachRow, eachColumn,
                                          QTableWidgetItem(str(dfOrderDB.iat[eachRow, eachColumn])))
        self.tableOrderDB.resizeColumnsToContents()
        tab2.addTab(self.tableOrderDB, '指令下单记录')

        self.tableOrder = QTableWidget(0, len(listFreqOrder), self)
        self.tableOrder.setHorizontalHeaderLabels(listFreqOrder)
        for num in range(len(listFreqOrder)):
            self.tableOrder.horizontalHeaderItem(num).setFont(self.font)
        self.tableOrder.setFont(self.font)
        self.tableOrder.resizeColumnsToContents()
        self.tableOrder.verticalHeader().setVisible(False)
        self.tableOrder.setEnabled(True)
        tab2.addTab(self.tableOrder, '所有委托单')

        self.tableError = QTableWidget(0, len(listError), self)
        self.tableError.setHorizontalHeaderLabels(listError)
        for num in range(len(listError)):
            self.tableError.horizontalHeaderItem(num).setFont(self.font)
        self.tableError.setFont(self.font)
        self.tableError.resizeColumnsToContents()
        self.tableError.verticalHeader().setVisible(False)
        self.tableError.setEnabled(True)
        tab2.addTab(self.tableError, '错误委托单')

        listPositionPlus = listPosition.copy()
        listPositionPlus.append('市价平仓')
        self.tablePosition = QTableWidget(0, len(listPositionPlus), self)
        self.tablePosition.setHorizontalHeaderLabels(listPositionPlus)
        for num in range(len(listPositionPlus)):
            self.tablePosition.horizontalHeaderItem(num).setFont(self.font)
        self.tablePosition.setFont(self.font)
        self.tablePosition.resizeColumnsToContents()
        self.tablePosition.verticalHeader().setVisible(False)
        self.tablePosition.setEnabled(True)
        tab2.addTab(self.tablePosition, '账户持仓')

        self.tableAccount = QTableWidget(0, len(listAccount), self)
        self.tableAccount.setHorizontalHeaderLabels(listAccount)
        for num in range(len(listAccount)):
            self.tableAccount.horizontalHeaderItem(num).setFont(self.font)
        self.tableAccount.setFont(self.font)
        self.tableAccount.resizeColumnsToContents()
        self.tableAccount.verticalHeader().setVisible(False)
        self.tableAccount.setEnabled(True)
        tab2.addTab(self.tableAccount, '账户资金')
        tab2.currentChanged.connect(self.switchTab2)
        hbox.addWidget(tab2)
        gbox2.setLayout(hbox)
        # endregion

        # region QTime 的使用
        self.timer0 = QTimer(self)
        self.timer0.timeout.connect(self.flushMarketOrder)  # 进行市价单重复下单操作

        self.timer1 = QTimer(self)
        self.timer1.timeout.connect(self.flushDuoKong)  # 刷新多空持仓显示

        self.timer2 = QTimer(self)
        self.timer2.timeout.connect(self.flushPosition)  # 定时刷新止盈单数据
        # endregion

        # region 菜单栏
        self.listMenuButton = []
        menuRoot = self.menuBar()
        menuRoot.setFont(self.font)
        setInit = menuRoot.addMenu('设置')
        self.setItem = QAction('初始化设置', self)
        self.setItem.setFont(self.font)
        self.setItem.triggered.connect(self.setShow)
        self.setItem.setEnabled(False)
        self.setItem2 = QAction('刷新周交易明细表', self)
        self.setItem2.setFont(self.font)
        self.setItem2.triggered.connect(self.setShow2)
        self.setItem2.setEnabled(True)
        self.setItem3 = QAction('生成本周交易记录', self)
        self.setItem3.setFont(self.font)
        self.setItem3.triggered.connect(self.recordTrade)
        setInit.addAction(self.setItem)
        setInit.addAction(self.setItem2)
        setInit.addAction(self.setItem3)

        orderhandle = menuRoot.addMenu('手动下单')
        ordering = QAction('下单', self)
        ordering.setFont(self.font)
        # ordering.triggered.connect(self.orderShow)
        self.listMenuButton.append(ordering)
        orderhandle.addAction(ordering)

        orderingCancel = QAction('撤单', self)
        orderingCancel.setFont(self.font)
        orderingCancel.triggered.connect(self.orderCancelShow)
        self.listMenuButton.append(orderingCancel)
        orderhandle.addAction(orderingCancel)

        orderingPark = QAction('预下单', self)
        orderingPark.setFont(self.font)
        # orderingPark.triggered.connect(self.orderParkShow)
        self.listMenuButton.append(orderingPark)
        orderhandle.addAction(orderingPark)

        orderingCancelPark = QAction('预撤单', self)
        orderingCancelPark.setFont(self.font)
        orderingCancelPark.triggered.connect(self.orderCancelParkShow)
        self.listMenuButton.append(orderingCancelPark)
        orderhandle.addAction(orderingCancelPark)

        orderMarketHandle = menuRoot.addMenu("频段强平")
        for freq in listFreq:
            freq = freq
            freqTemp = QAction('CTA' + str(freq), self)
            freqTemp.setFont(self.font)
            freqTemp.triggered.connect(self.orderMarket)
            self.listMenuButton.append(freqTemp)
            orderMarketHandle.addAction(freqTemp)

        orderPositionHandle = menuRoot.addMenu("频段仓位调整")
        for freq in listFreq:
            freq = freq
            freqTemp = QAction('CTA' + str(freq), self)
            freqTemp.setFont(self.font)
            freqTemp.triggered.connect(self.orderPosition)
            self.listMenuButton.append(freqTemp)
            orderPositionHandle.addAction(freqTemp)

        showAllPositionHandle = menuRoot.addMenu("显示所有仓位")
        self.showAllPosition = QAction('显示所有仓位', self)
        self.showAllPosition.setFont(self.font)
        self.showAllPosition.triggered.connect(self.getPositionUI)
        self.listMenuButton.append(self.showAllPosition)
        showAllPositionHandle.addAction(self.showAllPosition)

        start = menuRoot.addMenu('交易执行')
        self.startItem = QAction('开始交易', self)
        self.startItem.setFont(self.font)
        self.startItem.triggered.connect(self.getTrade)
        self.listMenuButton.append(self.startItem)
        start.addAction(self.startItem)
        for eachMenuButton in self.listMenuButton:
            eachMenuButton.setEnabled(False)

        # 暂停某一个品种操作
        stopOpen = menuRoot.addMenu("停止品种开仓操作")
        self.selectGoodsCode = QAction('筛选品种操作', self)
        self.selectGoodsCode.setFont(self.font)
        self.selectGoodsCode.triggered.connect(self.selectGoodsCodeUI)
        stopOpen.addAction(self.selectGoodsCode)
        # endregion

        #region 总布局
        widget = QWidget()
        widget.setLayout(vbox0)
        self.setCentralWidget(widget)
        #endregion

    def switchTab2(self):
        tab2 = self.sender()
        index = tab2.currentIndex()
        text = tab2.tabText(index)
        if text == '账户持仓':  # 说明当时在查询持仓了：
            try:
                dfPosition.drop(list(dfPosition.index), inplace=True)
                self.td.getPosition()
                for each in range(100):
                    if self.td.checkPosition:
                        self.position()
                        break
                    else:
                        ttt.sleep(0.01)
            except:
                putLogEvent("查询持仓量失败，资金账号未登陆")
        elif text == '账户资金':
            try:
                # 先将旧持仓清空
                self.tableAccount.clearContents()
                self.tableAccount.setRowCount(0)
                self.td.getAccount()
            except:
                putLogEvent("查询账号资金失败，资金账号未登陆")
    # endregion

    # region 菜单事件处理
    def setShow(self):  # 手动设置操作
        self.theSetShow = SetUi()
        self.theSetShow.setWindowFlags(Qt.Dialog)
        self.theSetShow.setWindowModality(Qt.ApplicationModal)
        self.theSetShow.show()

    def setShow2(self):
        now = datetime.now()
        if (now.time() < time(8, 59)) or (time(11, 30) < now.time() < time(13, 30)) or (
                time(10, 15) < now.time() < time(10, 35)) or (
                time(15, 15) < now.time() < time(20, 55)) or now.date() not in tradeDate.tolist():
            threading.Thread(target=self.execWeekTradeTab, daemon=True).start()

    def recordTrade(self):
        self.setItem3.setEnabled(False)
        os.makedirs("交易记录", exist_ok=True)
        if '{}.csv'.format(week) in os.listdir("交易记录"):
            dfTradeRecord = pd.read_csv("交易记录\\{}.csv".format(week), encoding='gbk')
        else:
            dfTradeRecord = pd.DataFrame(columns=listFreqTrade)
        dfResultRate = pd.DataFrame(columns=["平仓时间", '代码', '名称', "开仓价", "平仓价", '数量', '频段', "收益情况", "收益率", '手续费'])
        # 读取手续费操作
        dfGoodsName = pd.read_excel("RD files\\公共参数.xlsx", sheetname="品种信息")
        dictGoodsTransactionDay = {}
        dictGoodsTransaction = {}
        for i in range(dfGoodsName.shape[0]):
            goodsCode = dfGoodsName['品种代码'][i]
            dictGoodsTransactionDay[goodsCode] = dfGoodsName['日内交易'][i]
            dictGoodsTransaction[goodsCode] = dfGoodsName['非日内交易'][i]
        for freq in dictFreqTrade.keys():
            dfTemp = dictFreqTrade[freq]
            dfTradeRecord = dfTradeRecord.append(dfTemp)
            # 就是 dictFreqTrade[freq]
            for theI in range(dfTemp.shape[0]):
                if dfTemp['本地下单码'][theI][-1] in ["0", "2", '9']:
                    closeTime = dfTemp['时间'][theI]
                    if closeTime > datetime.now():
                        closeTime -= timedelta(days=1)
                    if (datetime.now() - closeTime) > timedelta(days=1):
                        continue
                    instrument = dfTemp['代码'][theI]
                    goodsCode = getGoodsCode(instrument)
                    goodsName = dfTemp['名称'][theI]
                    num = dfTemp['数量'][theI] * (-1)
                    closePrice = dfTemp['价格'][theI]
                    theDfWeek = pd.read_csv('weekTradeTab\\{}\\{}.csv'.format(freq, goodsCode), parse_dates=['交易时间'], encoding='gbk')
                    theDfWeek["开仓时间"] = theDfWeek["开仓时间"].astype('str')
                    theDfWeek["平仓时间"] = theDfWeek["平仓时间"].astype('str')
                    theDfWeek = theDfWeek[theDfWeek["交易时间"] <= closeTime]
                    theDfWeek = theDfWeek[(theDfWeek["开仓时间"] == 'nan')|(theDfWeek["平仓时间"] != 'nan')]
                    if num > 0:
                        openPrice = round(theDfWeek['开仓线多'].iat[-1] * (1 / dictGoodsUnit[goodsCode])) * dictGoodsUnit[goodsCode]
                    else:
                        openPrice = round(theDfWeek['开仓线空'].iat[-1] * (1 / dictGoodsUnit[goodsCode])) * dictGoodsUnit[goodsCode]
                    # yikui 是需要减去手续费的
                    if dictGoodsTransaction[goodsCode][-1] == "万":
                        shou = abs(num * float(dictGoodsTransaction[goodsCode][:-1]) * openPrice * 1.05 * 0.0001 + num * float(dictGoodsTransaction[goodsCode][:-1]) * closePrice * 1.05 * 0.0001)
                        yikui = (closePrice - openPrice) * num * dictGoodsCheng[goodsCode] - shou
                    elif dictGoodsTransaction[goodsCode][-1] == "手":
                        shou = abs(2 * num * float(dictGoodsTransaction[goodsCode][:-1]) * 1.05)
                        yikui = (closePrice - openPrice) * num * dictGoodsCheng[goodsCode] - shou
                    dfResultRate.loc[dfResultRate.shape[0]] = [closeTime, instrument, goodsName, openPrice, closePrice, num, freq, yikui, yikui / dfCapital[dfCapital['账户名'] == dictFreqSet[freq][5]]['资金'].iat[0], shou]
        dfResultRate.to_csv("交易记录\\{}.csv".format("今日交易记录"), encoding='gbk', index = False)
        # dfTradeRecord = dfTradeRecord.sort_values("时间")
        dfTradeRecord = dfTradeRecord.drop_duplicates()
        dfTradeRecord.to_csv("交易记录\\{}.csv".format(week), encoding='gbk', index = False)
        putLogEvent("本交易日的交易记录添加完毕")
        self.setItem3.setEnabled(True)

    def execWeekTradeTab(self):
        putLogEvent("正在刷新周交易明细表")
        dfTemp = pd.DataFrame(columns=listWeekTradeTab).set_index('交易时间')
        for freq in listFreq:
            for goodsCode in dictGoodsName.keys():
                if goodsCode in dictFreqUnGoodsCode[freq]:
                    continue
                goodsName = dictGoodsName[goodsCode]
                dictData[freq][goodsName + '_周交易明细表'] = dictData[freq][goodsName + '_周交易明细表'][0:0].copy()
                dfTemp.to_csv('weekTradeTab\\{}\\{}.csv'.format(freq, goodsCode), index=True, encoding='gbk')
                getWeekTradeTab(goodsCode, freq)
        self.execOrderDB()
        putLogEvent("刷新周交易明细表完成")

    def execOrderDB(self):
        for index in dfOrderDB.index:
            event = Event(type_=EVENT_SHOWORDERDB)
            d = {}
            d['isChg'] = True
            d['index'] = index
            d['goods_code'] = dfOrderDB['合约号'][index] + '.'
            event.dict_ = d
            ee.put(event)
        # 刷新指令
        for freq in listFreq:
            for goodsCode in dictGoodsName.keys():
                if goodsCode not in dictFreqUnGoodsCode[freq]:
                    goodsName = dictGoodsName[goodsCode]
                    tradeTime = dictData[freq][goodsName + '_调整表'].index[-1]
                    indexGoods = listGoods.index(goodsCode)
                    indexBar = dictGoodsClose[freq][goodsCode].index(tradeTime.time())
                    orderRef = theTradeDay.strftime('%Y%m%d') + '.' + str(freq) + '.' + str(indexGoods) + '.' + str(indexBar) + '.'
                    preOrderRef = ""
                    if ((datetime.now() - tradeTime) < timedelta(days=1, hours=1)) or (dictFreqGoodsMin[freq][goodsCode][-1] == dictFreqGoodsMin[1][goodsCode][-1]):
                        getOrder(freq, goodsCode, orderRef, preOrderRef, False)

    def orderShow(self):  # 下单
        self.theOrderShow = OrderUi()
        self.theOrderShow.setWindowFlags(Qt.Dialog)
        self.theOrderShow.setWindowModality(Qt.ApplicationModal)
        self.theOrderShow.show()

    def orderCancelShow(self):  # 撤单
        self.theOrderCancelShow = OrderCancelUi()
        self.theOrderCancelShow.setWindowFlags(Qt.Dialog)
        self.theOrderCancelShow.setWindowModality(Qt.ApplicationModal)
        self.theOrderCancelShow.show()

    def orderParkShow(self):  # 预下单
        self.theOrderParkShow = OrderParkUi()
        self.theOrderParkShow.setWindowFlags(Qt.Dialog)
        self.theOrderParkShow.setWindowModality(Qt.ApplicationModal)
        self.theOrderParkShow.show()

    def orderCancelParkShow(self):  # 预撤单
        self.theorderCancelParkShow = OrderCancelParkUi()
        self.theorderCancelParkShow.setWindowFlags(Qt.Dialog)
        self.theorderCancelParkShow.setWindowModality(Qt.ApplicationModal)
        self.theorderCancelParkShow.show()

    def orderMarket(self):
        freq = self.sender().text()
        freq = int(freq[3:])
        self.ui = orderMarketUI(freq)
        self.ui.setWindowFlags(Qt.Dialog)
        self.ui.setWindowModality(Qt.ApplicationModal)
        self.ui.show()

    def selectGoodsCodeUI(self):
        self.ui = ShowGoodsCode()
        self.ui.setWindowFlags(Qt.Dialog)
        self.ui.setWindowModality(Qt.ApplicationModal)
        self.ui.show()

    def orderPosition(self):
        freq = self.sender().text()
        freq = int(freq[3:])
        self.ui = ChgPositionHandleUI(freq)
        self.ui.setWindowFlags(Qt.Dialog)
        self.ui.setWindowModality(Qt.ApplicationModal)
        self.ui.show()
    # endregion

    # region 持仓全平操作（不需要用到）
    def chgRealPositionHandle(self):  # 在持仓上进行所有平仓
        self.sender().setEnabled(False)
        index, instrument, num = self.sender().objectName().split(' ')
        index = int(index)
        num = int(num)
        if num > 0:
            fangXiang = '买'
            direction = 'Sell'
        else:
            fangXiang = '卖'
            direction = 'Buy'
        goodsCode = getGoodsCode(instrument)
        # 判断是否在交易时间内

        if not judgeInTradeTime(goodsCode):
            reply = QMessageBox.warning(self, '警告', '该品种不在交易时间内')
            return
        # 将冻结仓位解冻
        dfOrderSourceTemp = dfOrderSource.copy()
        for eachIndex in dfOrderSourceTemp['InstrumentID'][dfOrderSourceTemp['InstrumentID'] == instrument].index:
            if dfOrderSourceTemp['StatusMsg'][eachIndex] not in ["全部成交", "全部成交报单已提交"] \
                    and dfOrderSourceTemp['StatusMsg'][eachIndex][:3] != "已撤单":
                if dfOrderSourceTemp['Direction'][eachIndex] == direction:
                    # 先对该品种进行撤单操作
                    cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                    orderRef = dfOrderSourceTemp['OrderRef'][eachIndex]
                    cancelEvent.dict_['orderref'] = orderRef
                    self.orderCancel(cancelEvent)
        # 将持仓删除
        for freq in listFreq:
            dfFreqPosition = dictFreqPosition[freq].copy()
            if instrument in dfFreqPosition['代码'].tolist() and \
                    dfFreqPosition['方向'][dfFreqPosition['代码'].tolist().index(instrument)][0] == fangXiang:
                event = Event(type_=EVENT_SHOWPOSITION)
                event.dict_['instrument'] = instrument
                event.dict_['freq'] = freq
                self.showPositionEvent(event)
        # 正式下单
        orderEvent = Event(type_=EVENT_ORDERCOMMAND)
        orderEvent.dict_['InstrumentID'] = instrument
        dfPositionTemp = dfPosition.copy()
        s = dfPositionTemp.loc[index]
        todayNum = s['今日持仓']
        yesterdayNum = abs(s['数量']) - todayNum
        # 平昨仓
        ttt.sleep(0.2)
        if yesterdayNum > 0:
            orderEvent.dict_['InstrumentID'] = instrument
            if s['方向'][:1] == '买':
                directionType = TThostFtdcDirectionType.THOST_FTDC_D_Sell
            else:
                directionType = TThostFtdcDirectionType.THOST_FTDC_D_Buy
            orderEvent.dict_['Direction'] = directionType
            orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
            orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
            orderEvent.dict_['LimitPrice'] = 0
            orderEvent.dict_['orderref'] = datetime.now().strftime('%m%d%H%M%S%f')[:11]
            orderEvent.dict_['VolumeTotalOriginal'] = int(yesterdayNum)
            ee.put(orderEvent)
        # 平今仓
        if todayNum > 0:
            orderEvent.dict_['InstrumentID'] = instrument
            if s['方向'][:1] == '买':
                directionType = TThostFtdcDirectionType.THOST_FTDC_D_Sell
            else:
                directionType = TThostFtdcDirectionType.THOST_FTDC_D_Buy
            orderEvent.dict_['Direction'] = directionType
            orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
            orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
            orderEvent.dict_['LimitPrice'] = 0
            orderEvent.dict_['orderref'] = datetime.now().strftime('%m%d%H%M%S%f')[:11]
            orderEvent.dict_['VolumeTotalOriginal'] = int(todayNum)
            orderEvent.dict_['isToday'] = True
            ee.put(orderEvent)
    # endregion

    # region 事件对应处理方法
    def registerEngine(self):
        ee.register(EVENT_LOG, self.pLog)
        ee.register(EVENT_ACCOUNT, self.account)  # 账号
        ee.register(EVENT_ORDER, self.order)
        ee.register(EVENT_TRADE, self.trade)
        ee.register(EVENT_ORDER_ERROR, self.showError)
        ee.register(EVENT_ORDERCOMMAND, self.orderCommand)  # 下单
        ee.register(EVENT_ORDERCANCEL, self.orderCancel)  # 撤单
        ee.register(EVENT_ORDERCANCELPARK, self.orderCancelPark)  # 预撤单
        ee.register(EVENT_MARKETDATA_CONTRACT, self.dealTickData)  # 处理tick数据
        ee.register(EVENT_SHOWORDERDB, self.showOrderDB)
        ee.register(EVENT_SHOWPOSITION, self.showPositionEvent)  # 重新显示持仓信息
        ee.register(EVENT_LOGIN, self.login)  # 重新显示持仓信息
        self.listInstrumentInformation = []  # 保存合约资料
        self.checkInstrumentChg = True
        ee.register(EVENT_INSTRUMENT, self.insertMarketData)
        ee.register(EVENT_MARKETORDER, self.marketOrder)  # 进行某频段某品种的市价平仓操作
        ee.start()

    def pLog(self, event):
        self.txtLog.append(event.dict_['log'])
        self.txtLog.moveCursor(QTextCursor.End)
        logRuida.info(event.dict_['log'])

    def account(self, event):
        var = event.dict_
        tmp = {}
        tmp["经纪公司代码"] = var["BrokerID"]
        tmp["投资者帐号"] = var["AccountID"]
        tmp["上次存款额"] = round(var["PreDeposit"], 2)
        tmp["上次结算准备金"] = round(var["PreBalance"], 2)
        tmp["上次占用的保证金"] = var["PreMargin"]
        tmp["当前保证金总额"] = round(var["CurrMargin"], 2)
        tmp["可用资金"] = round(var["Available"], 2)
        tmp["可取资金"] = round(var["WithdrawQuota"], 2)
        self.tableAccount.setRowCount(self.tableAccount.rowCount() + 1)
        for num in range(len(listAccount)):
            self.tableAccount.setItem(self.tableAccount.rowCount() - 1, num,
                                      QTableWidgetItem(str(tmp[listAccount[num]])))
        self.tableAccount.resizeColumnsToContents()

    def position(self):  # 总持仓查询
        self.tablePosition.clearContents()
        self.tablePosition.setRowCount(0)
        dfTemp = dfPosition.copy()
        for eachRow in range(dfTemp.shape[0]):
            self.tablePosition.setRowCount(self.tablePosition.rowCount() + 1)
            for eachColumn in range(len(listPosition)):
                self.tablePosition.setItem(self.tablePosition.rowCount() - 1, eachColumn,
                                           QTableWidgetItem(str(dfTemp.iat[eachRow, eachColumn])))
            widget = QWidget()
            hbox = QHBoxLayout()
            btn = QPushButton('全平操作')
            # btn.clicked.connect(self.chgRealPositionHandle)
            btn.setObjectName('{} {} {}'.format(eachRow, dfTemp.at[eachRow, "代码"], dfTemp.at[eachRow, "数量"]))
            hbox.addWidget(btn)
            hbox.setContentsMargins(5, 2, 5, 2)
            widget.setLayout(hbox)
            self.tablePosition.setCellWidget(self.tablePosition.rowCount() - 1,
                                             len(listPosition), widget)
        self.tablePosition.resizeColumnsToContents()

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
        logTick.info('order: ' + str(tmp))
        if var["OrderRef"] not in dfOrderSource['OrderRef'].tolist():
            with lockDfOrder:
                dfOrder.loc[dfOrder.shape[0]] = [tmp[x] for x in listFreqOrder]
                dfOrderSource.loc[dfOrderSource.shape[0]] = [var[x] for x in listOrderSourceColumns]
            self.tableOrder.setRowCount(self.tableOrder.rowCount() + 1)
            for num in range(len(listFreqOrder)):
                self.tableOrder.setItem(self.tableOrder.rowCount() - 1, num, QTableWidgetItem(str(tmp[listFreqOrder[num]])))
            self.tableOrder.resizeColumnsToContents()
        else:
            with lockDfOrder:
                index = dfOrderSource['OrderRef'].tolist().index(var["OrderRef"])
                dfOrder.loc[index] = [tmp[x] for x in listFreqOrder]
                dfOrderSource.loc[index] = [var[x] for x in listOrderSourceColumns]
            for num in range(len(listFreqOrder)):
                self.tableOrder.setItem(index, num, QTableWidgetItem(str(tmp[listFreqOrder[num]])))
            self.tableOrder.resizeColumnsToContents()
        # 记录委托单数据：
        dfFreqOrder = dictFreqOrder[freq]
        dfFreqOrderSource = dictFreqOrderSource[freq]
        tableFreqOrder = self.dictFreqOrderTable[freq]
        if var["OrderRef"] not in dfFreqOrderSource['OrderRef'].tolist():
            with lockDictFreqOrder:
                dfFreqOrder.loc[dfFreqOrder.shape[0]] = [tmp[x] for x in listFreqOrder]
                dfFreqOrderSource.loc[dfFreqOrderSource.shape[0]] = [var[x] for x in listOrderSourceColumns]
            tableFreqOrder.setRowCount(tableFreqOrder.rowCount() + 1)
            for num in range(len(listFreqOrder)):
                tableFreqOrder.setItem(tableFreqOrder.rowCount() - 1, num, QTableWidgetItem(str(tmp[listFreqOrder[num]])))
            tableFreqOrder.resizeColumnsToContents()
        else:
            with lockDictFreqOrder:
                index = dfFreqOrderSource['OrderRef'].tolist().index(var["OrderRef"])
                dfFreqOrder.loc[index] = [tmp[x] for x in listFreqOrder]
                dfFreqOrderSource.loc[index] = [var[x] for x in listOrderSourceColumns]
            for num in range(len(listFreqOrder)):
                tableFreqOrder.setItem(index, num, QTableWidgetItem(str(tmp[listFreqOrder[num]])))
            tableFreqOrder.resizeColumnsToContents()
        if tmp["状态"] == '已撤单' \
                and tmp["本地下单码"] in self.dictPreOrderRefOrder.keys():
            # 如果是最后一个的话，那么进行下单操作
            if var["OrderRef"] == dictRefOrder[tmp["本地下单码"]][-1]:
                event = self.dictPreOrderRefOrder.pop(tmp["本地下单码"])
                event.dict_['preOrderRef'] = ''
                self.orderCommand(event)

    # 查询成交单
    def trade(self, event):
        var = event.dict_
        tmp = {}
        if var["OrderRef"] not in dictOrderRef.keys():
            return
        else:
            var["OrderRef"] = dictOrderRef[var["OrderRef"]]
            tmp["本地下单码"] = var["OrderRef"]
        tmp["时间"] = pd.Timestamp(theTradeDay.strftime('%Y%m%d') + ' ' + var["TradeTime"])
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
        logTick.info('trade: ' + str(tmp))
        if tmp["本地下单码"] not in dfTrade['本地下单码'].values:
            dfTrade.loc[dfTrade.shape[0]] = [tmp[x] for x in listFreqTrade]
        else:
            index = dfTrade[dfTrade['本地下单码'] == tmp["本地下单码"]].index[0]
            dfTrade.at[index, '数量'] += tmp["数量"]
            dfTrade.at[index, '时间'] = tradeTime
        # 本地下单码 日期 5 位  频段 2 位  品种 2 位  第几个 bar 数据 2 位  开仓 还是 平仓 1 位
        freq = int(tmp["本地下单码"].split('.')[1])
        tmp['freq'] = freq
        if freq not in listFreq:
            return False
        codeGoodsCode = listGoods[int(tmp["本地下单码"].split('.')[2])]
        if codeGoodsCode != goodsCode:
            return
        # 关于持仓的增加问题
        tableFreqTrade = self.TableFreqTrade[freq]
        dfFreqTrade = dictFreqTrade[freq]
        if tmp["本地下单码"] not in dfFreqTrade['本地下单码'].tolist():
            dfFreqTrade.loc[dfFreqTrade.shape[0]] = [tmp[x] for x in listFreqTrade]
            tableFreqTrade.setRowCount(tableFreqTrade.rowCount() + 1)
            for eachNum in range(len(listFreqTrade)):
                tableFreqTrade.setItem(tableFreqTrade.rowCount() - 1, eachNum,
                                       QTableWidgetItem(str(tmp[listFreqTrade[eachNum]])))
            tableFreqTrade.resizeColumnsToContents()
        else:
            index = dfFreqTrade[dfFreqTrade['本地下单码'] == tmp["本地下单码"]].index[0]
            dfFreqTrade.at[index, '数量'] += tmp["数量"]
            dfFreqTrade.at[index, '时间'] = tradeTime
            tableFreqTrade.setItem(index, listFreqTrade.index('数量'),
                                   QTableWidgetItem(str(dfFreqTrade.at[index, '数量'])))
            tableFreqTrade.setItem(index, listFreqTrade.index('时间'),
                                   QTableWidgetItem(str(dfFreqTrade.at[index, '时间'])))
            tableFreqTrade.resizeColumnsToContents()
        # 修改成交时间
        # now = datetime.now()
        # if tradeTime > now:
        #     tradeTime = datetime(now.year, now.month, now.day, tradeTime.hour, tradeTime.minute, tradeTime.second)
        #     if tradeTime > now:
        #         return
        # 查看该成交记录是否已经处理
        tradeID = tradeTime.strftime('%Y%m%d%H%M%S') + var['TradeID']
        if tradeID not in listTradeID:
            listTradeID.append(tradeID)
            pd.to_pickle(listTradeID, 'pickle\\listTradeID.pkl')
        else:
            return
        # 对频段持仓进行处理
        dfOrderDBTemp = dfOrderDB.copy()
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
                    if not dfOrderDBTemp.loc[dfOrderDBTemp['本地下单码'] == tmp["本地下单码"]].empty:
                        index = dfOrderDBTemp.index[dfOrderDBTemp['本地下单码'] == tmp["本地下单码"]][-1]
                        s = dfOrderDBTemp.loc[index]
                        profitLine = s['多止盈线']
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = profitLine
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(var["Volume"])
                        self.orderCommand(orderEvent)
                        # region 写入 dfOrderDB 上
                        if tmp["本地下单码"][:-1] + '0' not in dfOrderDB['本地下单码'].tolist():
                            # 新写入止损单
                            s["本地下单码"] = tmp["本地下单码"][:-1] + '0'
                            s['应开多手数'] = 0
                            s['应开空手数'] = 0
                            s['合约号'] = s['合约号'].split('.')[0]
                            s['持有多手数'] = int(var["Volume"])
                            s['发单时间'] = pd.Timestamp(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            event = Event(type_=EVENT_SHOWORDERDB)
                            event.dict_ = s
                            self.showOrderDB(event)
                        else:
                            # 能够直接执行是因为 dfOrderDB 是单线程操作，不会有两条线路操作
                            index = dfOrderDB['本地下单码'].tolist().index(tmp["本地下单码"][:-1] + '0')
                            with lockDfOrderDB:
                                dfOrderDB.at[index, '持有多手数'] += int(var["Volume"])
                                dfOrderDB.to_pickle('pickle\\dfOrderDB.pkl')
                            self.tableOrderDB.setItem(index, listOrderDB.index('持有多手数'),
                                                      QTableWidgetItem(str(dfOrderDB.at[index, '持有多手数'])))
                        # endregion
                else:
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                    if not dfOrderDBTemp.loc[dfOrderDBTemp['本地下单码'] == tmp["本地下单码"]].empty:
                        index = dfOrderDBTemp.index[dfOrderDBTemp['本地下单码'] == tmp["本地下单码"]][-1]
                        s = dfOrderDBTemp.loc[index]
                        profitLine = s['空止盈线']
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = profitLine
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(var["Volume"])
                        self.orderCommand(orderEvent)
                        # region 写入 dfOrderDB 上
                        if tmp["本地下单码"][:-1] + '0' not in dfOrderDB['本地下单码'].tolist():
                            # 新写入止损单
                            s["本地下单码"] = tmp["本地下单码"][:-1] + '0'
                            s['应开多手数'] = 0
                            s['应开空手数'] = 0
                            s['合约号'] = s['合约号'].split('.')[0]
                            s['持有空手数'] = int(var["Volume"])
                            s['发单时间'] = pd.Timestamp(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            event = Event(type_=EVENT_SHOWORDERDB)
                            event.dict_ = s
                            self.showOrderDB(event)
                        else:
                            index = dfOrderDB['本地下单码'].tolist().index(tmp["本地下单码"][:-1] + '0')
                            with lockDfOrderDB:
                                dfOrderDB.at[index, '持有空手数'] += int(var["Volume"])
                                dfOrderDB.to_pickle('pickle\\dfOrderDB.pkl')
                            self.tableOrderDB.setItem(index, listOrderDB.index('持有空手数'),
                                                      QTableWidgetItem(str(dfOrderDB.at[index, '持有空手数'])))
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
                    if not dfOrderDBTemp.loc[dfOrderDBTemp['本地下单码'] == tmp["本地下单码"]].empty:
                        index = dfOrderDBTemp.index[dfOrderDBTemp['本地下单码'] == tmp["本地下单码"]][-1]
                        s = dfOrderDBTemp.loc[index]
                        profitLine = s['多止盈线']
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = profitLine
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(var["Volume"])
                        self.orderCommand(orderEvent)
                        # region 写入 dfOrderDB 上
                        if tmp["本地下单码"][:-1] + '0' not in dfOrderDB['本地下单码'].tolist():
                            # 新写入止损单
                            s["本地下单码"] = tmp["本地下单码"][:-1] + '0'
                            s['应开多手数'] = 0
                            s['应开空手数'] = 0
                            s['合约号'] = s['合约号'].split('.')[0]
                            s['持有多手数'] = int(var["Volume"])
                            s['发单时间'] = pd.Timestamp(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            event = Event(type_=EVENT_SHOWORDERDB)
                            event.dict_ = s
                            self.showOrderDB(event)
                        else:
                            # 能够直接执行是因为 dfOrderDB 是单线程操作，不会有两条线路操作
                            index = dfOrderDB['本地下单码'].tolist().index(tmp["本地下单码"][:-1] + '0')
                            with lockDfOrderDB:
                                dfOrderDB.at[index, '持有多手数'] += int(var["Volume"])
                                dfOrderDB.to_pickle('pickle\\dfOrderDB.pkl')
                            self.tableOrderDB.setItem(index, listOrderDB.index('持有多手数'),
                                                      QTableWidgetItem(str(dfOrderDB.at[index, '持有多手数'])))
                        # endregion
                else:
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                    if not dfOrderDBTemp.loc[dfOrderDBTemp['本地下单码'] == tmp["本地下单码"]].empty:
                        index = dfOrderDBTemp.index[dfOrderDBTemp['本地下单码'] == tmp["本地下单码"]][-1]
                        s = dfOrderDBTemp.loc[index]
                        profitLine = s['空止盈线']
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = profitLine
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(var["Volume"])
                        self.orderCommand(orderEvent)
                        # region 写入 dfOrderDB上
                        if tmp["本地下单码"][:-1] + '0' not in dfOrderDB['本地下单码'].tolist():
                            # 新写入止损单
                            s["本地下单码"] = tmp["本地下单码"][:-1] + '0'
                            s['应开多手数'] = 0
                            s['应开空手数'] = 0
                            s['合约号'] = s['合约号'].split('.')[0]
                            s['持有空手数'] = int(var["Volume"])
                            s['发单时间'] = pd.Timestamp(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            event = Event(type_=EVENT_SHOWORDERDB)
                            event.dict_ = s
                            self.showOrderDB(event)
                        else:
                            index = dfOrderDB['本地下单码'].tolist().index(tmp["本地下单码"][:-1] + '0')
                            with lockDfOrderDB:
                                dfOrderDB.at[index, '持有空手数'] += int(var["Volume"])
                                dfOrderDB.to_pickle('pickle\\dfOrderDB.pkl')
                            self.tableOrderDB.setItem(index, listOrderDB.index('持有空手数'),
                                                      QTableWidgetItem(str(dfOrderDB.at[index, '持有空手数'])))
                        # endregion
        else:
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
                    ee.put(event)

    # 查询错误委托
    def showError(self, event):
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
        dictTemp['错误原因'] =  var.get('ErrorMsg', "未知原因")
        dictTemp['时间'] = datetime.now()
        logTick.info('error: ' + str(var))
        self.tableError.setRowCount(self.tableError.rowCount() + 1)
        for num in range(len(listError)):
            self.tableError.setItem(self.tableError.rowCount() - 1, num,
                                       QTableWidgetItem(str(dictTemp[listError[num]])))
        self.tableError.resizeColumnsToContents()
        # CTP:平仓量超过持仓量
        # CTP:平今仓位不足
        if dictTemp['本地下单码'][-1] == '0' \
                and dictTemp['错误原因'] in ['CTP:平仓量超过持仓量', 'CTP:平今仓位不足'] \
                and dictTemp['本地下单码'] not in listErrorOrder:
            listErrorOrder.append(dictTemp['本地下单码'])
            if judgeCodeValue(dictTemp['本地下单码'], datetime.now()):
                if judgeInTradeTime(goodsCode):
                    # 撤单
                    cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                    cancelEvent.dict_['orderref'] = dictTemp['本地下单码'][:-1] + '2'
                    self.orderCancel(cancelEvent)
                    # 下单
                    orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                    orderEvent.dict_['InstrumentID'] = var['InstrumentID']
                    orderEvent.dict_['Direction'] = theDirection
                    orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                    orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                    orderEvent.dict_['LimitPrice'] = var["LimitPrice"]
                    orderEvent.dict_['orderref'] = dictTemp['本地下单码']
                    orderEvent.dict_['VolumeTotalOriginal'] = var["VolumeTotalOriginal"]
                    orderEvent.dict_['preOrderRef'] = dictTemp['本地下单码'][:-1] + '2'
                    self.orderCommand(orderEvent)
        elif dictTemp['错误原因'] == 'CTP:报单错误：不允许重复报单':
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

    # 限价委托
    def orderCommand(self, event):
        instrument = event.dict_['InstrumentID']
        orderPrice = event.dict_['LimitPrice']
        upPrice = dictInstrumentUpDownPrice[instrument][0]
        lowPrice = dictInstrumentUpDownPrice[instrument][1]
        if upPrice != 0 and lowPrice != 0:
            if orderPrice != 0:
                if orderPrice > upPrice:
                    listStopProfit.append(event.dict_['orderref'])
                    return
                elif orderPrice < lowPrice:
                    listStopProfit.append(event.dict_['orderref'])
                    return
        # 开仓操作
        if event.dict_['CombOffsetFlag'] == chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value):
            now = datetime.now() + timedelta(hours=6)
            mapOrderref = now.strftime('%H%M%S%f')[:9] + (uniformCode + event.dict_['orderref'].split('.')[1]).zfill(3)
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
                        self.dictPreOrderRefOrder[preOrderRef] = event
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
            # if 'isToday' in event.dict_.keys():
            #     isToday = event.dict_['isToday']
            now = datetime.now() + timedelta(hours=6)
            mapOrderref = now.strftime('%H%M%S%f')[:9] + (uniformCode + event.dict_['orderref'].split('.')[1]).zfill(3)
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

    def dealTickData(self, event):
        instrument = event.dict_["InstrumentID"]
        goodsCode = getGoodsCode(instrument)
        if goodsCode not in setTheGoodsCode:
            return
        goodsInstrument = instrument + '.'
        close = event.dict_["LastPrice"]
        now = datetime.now()
        theTradeTime = pd.Timestamp(event.dict_["TradingDay"] + ' '
                                        + event.dict_["UpdateTime"] + '.'
                                            + str(event.dict_["UpdateMillisec"]))
        if theTradeTime > now:  # 防止夜盘时，tick数据会出错
            theTradeTime = datetime(now.year, now.month, now.day,
                                    theTradeTime.hour, theTradeTime.minute, theTradeTime.second)
        now += timedelta(minutes=1)
        nowTime = time(now.hour, now.minute)
        if nowTime not in dictGoodsClose[1][goodsCode]:  # 如果不在交易时间内，tick无效
            return
        if dictInstrumentPrice[instrument] == 0:
            dictInstrumentPrice[instrument] = round(close, 4)
        dfOrderDBTemp = dfOrderDB.copy()
        dfOrderDBTemp = dfOrderDBTemp[(dfOrderDBTemp['合约号'] == instrument) & (~dfOrderDBTemp.index.isin(self.listDeadOrderDBIndex))].copy()  # 这个tick数据处理，是完全的错误的吧：是的：
        for eachNum in range(dfOrderDBTemp.shape[0]):
            s = dfOrderDBTemp.iloc[eachNum]
            orderRef = s['本地下单码']
            freq = int(orderRef.split('.')[1])
            index = dfOrderDBTemp.index[eachNum]
            if not judgeCodeValue(orderRef, theTradeTime):
                logTick.info('tick数据时间{}已经超出代码{}的有效时间'.format(theTradeTime, orderRef))
                event = Event(type_=EVENT_SHOWORDERDB)
                s['isChg'] = True
                s['index'] = index
                s['goods_code'] = goodsInstrument
                event.dict_ = s
                self.showOrderDB(event)
                continue
            if orderRef[-1] == '1':
                if close <= s['多开仓线'] <= dictInstrumentPrice[instrument]  or close >= s['多开仓线'] >= dictInstrumentPrice[instrument]:
                    # 如果 dictFreqPosition[freq] 已经存在持仓的话，不进行开仓
                    if instrument in dictFreqPosition[freq]['代码'].tolist():
                        logTick.info("满足品种 {} 策略 {} 的多开仓线，进行下多仓单操作，但是该频段却持有仓位，则不进行开仓操作".format(instrument, orderRef))
                        continue
                    logTick.info("满足品种 {} 策略 {} 的多开仓线，进行下多仓单操作".format(instrument, orderRef))
                    # 修改 dfOrderDB 操作
                    event = Event(type_=EVENT_SHOWORDERDB)
                    s['isChg'] = True
                    s['index'] = index
                    s['goods_code'] = goodsInstrument
                    event.dict_ = s
                    self.showOrderDB(event)
                    if not isOpenPosition[goodsCode]:
                        continue
                    # 进行下单操作
                    orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                    orderEvent.dict_['InstrumentID'] = instrument
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                    orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value)
                    orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                    orderEvent.dict_['LimitPrice'] = s['多开仓线']
                    orderEvent.dict_['orderref'] = orderRef
                    orderEvent.dict_['VolumeTotalOriginal'] = int(s['应开多手数'])
                    self.orderCommand(orderEvent)
                elif close <= s['空开仓线'] <= dictInstrumentPrice[instrument] or close >= s['空开仓线'] >= dictInstrumentPrice[instrument]:
                    if instrument in dictFreqPosition[freq]['代码'].tolist():
                        logTick.info("满足品种 {} 策略 {} 的多开仓线，进行下多仓单操作，但是该频段却持有仓位，则不进行开仓操作".format(instrument, orderRef))
                        continue
                    logTick.info("满足品种 {} 策略 {} 的空开仓线，进行下多仓单操作".format(instrument, orderRef))
                    # 修改 dfOrderDB 操作
                    event = Event(type_=EVENT_SHOWORDERDB)
                    s['isChg'] = True
                    s['index'] = index
                    s['goods_code'] = goodsInstrument
                    event.dict_ = s
                    self.showOrderDB(event)
                    if not isOpenPosition[goodsCode]:
                        continue
                    # 进行下单操作
                    orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                    orderEvent.dict_['InstrumentID'] = instrument
                    orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                    orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Open.value)
                    orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                    orderEvent.dict_['LimitPrice'] = s['空开仓线']
                    orderEvent.dict_['orderref'] = orderRef
                    orderEvent.dict_['VolumeTotalOriginal'] = int(s['应开空手数'])
                    self.orderCommand(orderEvent)
            elif orderRef[-1] == '0':
                if s['持有多手数'] > 0:
                    if close <= s['多止损线']:
                        # 别这么急着下止损单，先看看止盈单下了没有
                        with lockDictFreqOrder:
                            dfFreqOrderTemp = dictFreqOrder[freq].copy()
                        # 如果没有挂止盈单，又没有在涨跌停版上的话，那么不需要止损吧
                        if (orderRef[:-1] + "2" not in dfFreqOrderTemp['本地下单码'].tolist()) and (orderRef[:-1] + "2" not in listStopProfit):
                            continue
                        logTick.info("满足品种 {} 策略 {} 的多止损线，进行多止损操作".format(instrument, orderRef))
                        cancelOrderRef = orderRef[:-1] + '2'
                        logTick.info("品种：{} 之前的编号 {} 进行撤单操作".format(instrument,cancelOrderRef))
                        cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                        cancelEvent.dict_['orderref'] = cancelOrderRef
                        self.orderCancel(cancelEvent)
                        # 修改 dfOrderDB 操作
                        event = Event(type_=EVENT_SHOWORDERDB)
                        s['isChg'] = True
                        s['index'] = index
                        s['goods_code'] = goodsInstrument
                        event.dict_ = s
                        self.showOrderDB(event)
                        # 进行下单操作
                        orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                        orderEvent.dict_['InstrumentID'] = instrument
                        orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = s['多止损线']
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(s['持有多手数'])
                        orderEvent.dict_['preOrderRef'] = cancelOrderRef
                        self.orderCommand(orderEvent)
                elif s['持有空手数'] > 0:
                    if close >= s['空止损线']:
                        # 别这么急着下止损单，先看看止盈单下了没有
                        with lockDictFreqOrder:
                            dfFreqOrderTemp = dictFreqOrder[freq].copy()
                        if (orderRef[:-1] + "2" not in dfFreqOrderTemp['本地下单码'].tolist()) and (orderRef[:-1] + "2" not in listStopProfit):
                            continue
                        logTick.info("满足品种 {} 策略 {} 的空止损线，进行空止损操作".format(instrument, orderRef))
                        cancelOrderRef = orderRef[:-1] + '2'
                        logTick.info("品种：{} 之前的编号 {} 进行撤单操作".format(instrument, cancelOrderRef))
                        cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                        cancelEvent.dict_['orderref'] = cancelOrderRef
                        self.orderCancel(cancelEvent)
                        # 修改 dfOrderDB 操作
                        event = Event(type_=EVENT_SHOWORDERDB)
                        s['isChg'] = True
                        s['index'] = index
                        s['goods_code'] = goodsInstrument
                        event.dict_ = s
                        self.showOrderDB(event)
                        # 进行下单操作
                        orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                        orderEvent.dict_['InstrumentID'] = instrument
                        orderEvent.dict_['Direction'] = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_LimitPrice
                        orderEvent.dict_['LimitPrice'] = s['空止损线']
                        orderEvent.dict_['orderref'] = orderRef
                        orderEvent.dict_['VolumeTotalOriginal'] = int(s['持有空手数'])
                        orderEvent.dict_['preOrderRef'] = cancelOrderRef
                        self.orderCommand(orderEvent)
        dictInstrumentPrice[instrument] = round(close, 4)

    def showOrderDB(self, event):
        data = event.dict_
        with lockDfOrderDB:
            if data.get('isChg', False):
                # 更改dfOrderDB数据
                self.listDeadOrderDBIndex.append(data['index'])
                dfOrderDB.at[data['index'], '合约号'] = data['goods_code']
                self.tableOrderDB.setItem(data['index'], listOrderDB.index('合约号'),
                                          QTableWidgetItem(str(data['goods_code'])))
            else:
                # 将之前相同本地下单码的指令单转成无效
                dfOrderDBTemp = dfOrderDB[(dfOrderDB['本地下单码'] == data['本地下单码'])
                                          & (~dfOrderDB.index.isin(self.listDeadOrderDBIndex))].copy()
                for i in dfOrderDBTemp.index:
                    self.listDeadOrderDBIndex.append(i)
                    dfOrderDB.at[i, '合约号'] += '.'
                    self.tableOrderDB.setItem(i, listOrderDB.index('合约号'),
                                              QTableWidgetItem(str(dfOrderDB.at[i, '合约号'])))
                dfOrderDB.loc[dfOrderDB.shape[0]] = [data[x] for x in listOrderDB]
                self.tableOrderDB.setRowCount(self.tableOrderDB.rowCount() + 1)
                for eachColumn in range(len(listOrderDB)):
                    self.tableOrderDB.setItem(self.tableOrderDB.rowCount() - 1, eachColumn,
                                                         QTableWidgetItem(str(data[listOrderDB[eachColumn]])))
        self.tableOrderDB.resizeColumnsToContents()
        dfOrderDB.to_pickle('pickle\\dfOrderDB.pkl')

    # 将频段显示的持仓删除
    def showPositionEvent(self, event):
        with lockDictFreqPosition:
            if event.dict_.get('append', False):
                freq = event.dict_['freq']
                dictTemp = event.dict_.copy()
                if dictTemp['代码'] in dictFreqPosition[freq]['代码'].tolist():
                    index = dictFreqPosition[freq]['代码'].tolist().index(dictTemp['代码'])
                    dictFreqPosition[freq].loc[index] = [dictTemp[x] for x in listFreqPosition]
                    for column in listFreqPosition:
                        self.TableFreqPosition[freq].setItem(index, listFreqPosition.index(column), QTableWidgetItem(str(dictTemp[column])))
                else:
                    dictFreqPosition[freq].loc[dictFreqPosition[freq].shape[0]] = [dictTemp[x] for x in listFreqPosition]
                    self.TableFreqPosition[freq].setRowCount(dictFreqPosition[freq].shape[0])
                    for column in listFreqPosition:
                        self.TableFreqPosition[freq].setItem(dictFreqPosition[freq].shape[0] - 1, listFreqPosition.index(column), QTableWidgetItem(str(dictTemp[column])))
            else:
                freq = event.dict_['freq']
                instrument = event.dict_['代码']
                if instrument in dictFreqPosition[freq]['代码'].tolist():
                    index = dictFreqPosition[freq]['代码'].tolist().index(instrument)
                    dictFreqPosition[freq] = dictFreqPosition[freq].drop([index]).reset_index(drop = True)
                    self.TableFreqPosition[freq].removeRow(index)
        self.TableFreqPosition[freq].resizeColumnsToContents()
        pd.to_pickle(dictFreqPosition, 'pickle\\dictFreqPosition.pkl')
    # endregion

    # region 写入内存操作
    def getData(self):
        self.listDeadOrderDBIndex = []
        self.dictPreOrderRefOrder = {}
        self.queueRecv = queue.PriorityQueue()
        self.strRecv = ""
        putLogEvent("将数据库数据写入内存上")
        for freq in listFreqPlus:
            dictData[freq] = {}
            putLogEvent("将频段 {} 数据库数据写入内存".format(freq))
            for eachGoodsCode in dictGoodsName.keys():
                if eachGoodsCode in dictFreqUnGoodsCode[freq]:
                    continue
                eachGoodsName = dictGoodsName[eachGoodsCode]
                if freq < 60:
                    # 使用len(dictGoodsClose[freq][eachGoodsCode]) * 5
                    num = len(dictGoodsClose[freq][eachGoodsCode]) * 5 + mvlenvector[-1] + 10
                else:
                    num = len(dictGoodsClose[freq][eachGoodsCode]) * 30 + int(mvlenvector[-1] * 1.3) + 10
                dictData[freq][eachGoodsName + '_调整表'] = pd.read_sql(
                    "select * from cta{}_trade.{}_调整表 order by trade_time desc limit {}".format(freq, eachGoodsName, num), con).set_index(
                    'trade_time').sort_index()
                dictData[freq][eachGoodsName + '_调整表'] = dictData[freq][eachGoodsName + '_调整表'].drop(['id'],axis=1)
                if freq != 1:
                    dictData[freq][eachGoodsName + '_均值表'] = pd.read_sql(
                        "select * from cta{}_trade.{}_均值表 order by trade_time desc limit {}".format(freq, eachGoodsName, num), con).set_index(
                        'trade_time').sort_index()
                    dictData[freq][eachGoodsName + '_均值表'] = dictData[freq][eachGoodsName + '_均值表'].drop(['id'],
                                                                                                                 axis=1)
                    dictData[freq][eachGoodsName + '_重叠度表'] = pd.read_sql(
                        "select * from cta{}_trade.{}_重叠度表 order by trade_time desc limit {}".format(freq, eachGoodsName, num), con).set_index(
                        'trade_time').sort_index()
                    dictData[freq][eachGoodsName + '_重叠度表'] = dictData[freq][eachGoodsName + '_重叠度表'].drop(listDrop,
                                                                                                           axis=1)
                    dictData[freq][eachGoodsName + '_周交易明细表'] = pd.read_csv('weekTradeTab\\{}\\{}.csv'.format(freq, eachGoodsCode), parse_dates=['交易时间', '开仓时间', '平仓时间'], encoding='gbk').set_index('交易时间')
                    getWeekTradeTab(eachGoodsCode, freq)
        self.getZhuli()
        for eachMenuButton in self.listMenuButton:
            eachMenuButton.setEnabled(True)

    def getZhuli(self):
        putLogEvent("从 CTA{} 上读取主力合约".format(1))
        for eachGoods in dictGoodsName.keys():
            sql = "select * from cta1_trade.{}_调整时刻表".format(dictGoodsName[eachGoods])
            dictGoodsAdj[eachGoods] = pd.read_sql(sql, con).set_index('goods_code')
            dictGoodsAdj[eachGoods]['adjdate'] = pd.to_datetime(dictGoodsAdj[eachGoods]['adjdate']) + timedelta(
                hours=16)
            dictGoodsInstrument[eachGoods] = dictGoodsAdj[eachGoods].index[-1]
            instrument = dictGoodsAdj[eachGoods].index[-1].split('.')[0]
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
        putLogEvent("点击开始执行交易")
    #endregion

    # region 显示所有持仓信息（showALLData）
    def getPositionUI(self):
        self.thePositionShow = allPositionUI()
        self.thePositionShow.show()
    # endregion

    # region 点击开始交易执行事件
    def getTrade(self):
        # 登陆成功时，才为True
        self.islogin = False
        self.timer0.start(5000)  # 查看市价平仓是否正常操作
        self.timer1.start(15000)  # 实时更新那个做多与做空的操作
        self.timer2.start(7000)
        self.ui = loginInformationUI()
        self.ui.setWindowFlags(Qt.Dialog)
        self.ui.setWindowModality(Qt.ApplicationModal)
        self.ui.show()

    def login(self, event):
        userid = event.dict_['userid']
        password = event.dict_['password']
        brokerid = event.dict_['brokerid']
        RegisterFront = event.dict_['RegisterFront']
        product_info = event.dict_['product_info']
        app_id = event.dict_['app_id']
        auth_code = event.dict_['auth_code']
        if 'tcp' not in RegisterFront:
            RegisterFront = 'tcp://' + RegisterFront
        self.td = TdApi(userid, password, brokerid, RegisterFront, product_info, app_id, auth_code)
        t = 0
        while not self.td.islogin and t < 1000:
            t += 1
            ttt.sleep(0.01)
        if t >= 1000:
            self.td.t.ReqUserLogout(brokerid, userid)
            putLogEvent('账号登陆失败，请重新登陆')
            self.setItem.setEnabled(True)
            self.setItem2.setEnabled(True)
        else:
            self.md = MdApi()
            thd = threading.Thread(target=self.getAPI, daemon=True)
            thd.start()
            thdExec = threading.Thread(target=self.execOnBar, daemon=True)
            thdExec.start()
            self.startItem.setEnabled(False)
            self.setItem.setEnabled(True)
            self.setItem2.setEnabled(True)
            self.islogin = True

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
            putLogEvent(str(err))

    def insertMarketData(self, event):
        var = event.dict_
        self.listInstrumentInformation.append(var)
        if var['last']:
            putLogEvent('查询主力合约是否变化完成')
            ret = pd.DataFrame(self.listInstrumentInformation)
            ret = ret.set_index('InstrumentID')
            for goodsIcon in dictGoodsChg.keys():
                dfTemp = ret.loc[ret['ProductID'] == goodsIcon]
                dfTemp = dfTemp.sort_values(by='OpenInterest',ascending= False)
                if dfTemp.shape[0] == 0:
                    continue
                if dfTemp.index[0] not in listInstrument:
                    putLogEvent('切换主力合约为 {} '.format(dfTemp.index[0]))
                    nowTime = datetime.now().time()
                    instrument = dictGoodsInstrument[goodsIcon + '.' + dictGoodsChg[goodsIcon]]
                    if nowTime > time(14, 50) and nowTime < time(15):
                        print("因为切换了主力合约，进行平仓操作")
                        # 查看是否有该品种的持仓
                        with lockDictFreqPosition:
                            dictFreqPositionTemp = dictFreqPosition.copy()
                        for freq in listFreq:
                            print(freq)
                            print(instrument)
                            if instrument.split('.')[0] in dictFreqPositionTemp[freq]['代码'].tolist():
                                print("有持仓，进行平仓操作")
                                orderEvent = Event(type_=EVENT_MARKETORDER)
                                orderEvent.dict_['instrument'] = instrument
                                orderEvent.dict_['freq'] = freq
                                orderEvent.dict_['num'] = dictFreqPositionTemp[freq]['数量'][dictFreqPositionTemp[freq]['代码'] == instrument].iat[0]
                                ee.put(orderEvent)
            self.listInstrumentInformation = []

    def marketOrder(self, event):  # 调用这个方法能够进行高价平仓操作
        freq = event.dict_['freq']
        instrument = event.dict_['instrument']
        goodsCode = getGoodsCode(instrument)
        indexGoods = listGoods.index(goodsCode)
        num = event.dict_['num']
        now = datetime.now()
        nowTime = now.time()
        # 获取 indexBar 即为 Bar 的索引
        for i in range(len(dictGoodsClose[freq][goodsCode])):
            if i == len(dictGoodsClose[freq][goodsCode]) - 1:
                iN = 0
            else:
                iN = i + 1
            if dictGoodsClose[freq][goodsCode][i] < dictGoodsClose[freq][goodsCode][iN]:
                if dictGoodsClose[freq][goodsCode][i] <= nowTime < dictGoodsClose[freq][goodsCode][iN]:
                    indexBar = i
                    break
            else:
                if dictGoodsClose[freq][goodsCode][i] <= nowTime or nowTime < dictGoodsClose[freq][goodsCode][iN]:
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
        if dfFreqOrderTemp.shape[0] > 0:
            for index in dfFreqOrderTemp['本地下单码'][
                        pd.DataFrame(dfFreqOrderTemp['本地下单码'].str.split('.').tolist())[2] == str(indexGoods)].index:
                if dfFreqOrderTemp['状态'][index] not in ["全部成交", "全部成交报单已提交"] and dfFreqOrderTemp['状态'][index][
                                                                                 :3] != "已撤单":
                    cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                    cancelEvent.dict_['orderref'] = dfFreqOrderTemp['本地下单码'][index]
                    preOrderRef = cancelEvent.dict_['orderref']
                    self.orderCancel(cancelEvent)
                    if preOrderRef[:-1] + '0' in dfOrderDB['本地下单码'].tolist():
                        index = dfOrderDB['本地下单码'].tolist().index(preOrderRef[:-1] + '0')
                        orderDBEvent = Event(type_=EVENT_SHOWORDERDB)
                        orderDBEvent.dict_['isChg'] = True
                        orderDBEvent.dict_['index'] = index
                        orderDBEvent.dict_['index'] = instrument + '.'
                        self.showOrderDB(orderDBEvent)
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

    # region 执行接收的Bar数据
    def execOnBar(self):
        # 执行每个频段每个品种的最后 onBar 操作
        while True:
            try:
                strReplyTemp = self.queueRecv.get(timeout=2)
                strReplyTemp = strReplyTemp[1]
                logTick.info(strReplyTemp)
                onBar(strReplyTemp)
                self.queueRecv.task_done()
            except Empty:
                pass
    # endregion

    #region 定期检查市价单是否已经成交
    def flushMarketOrder(self):
        # 对所有预下单数据进行操作
        if self.islogin:
            for freq in dictFreqGoodsNextOrder.keys():
                for goodsCode in dictFreqGoodsNextOrder[freq].keys():
                    for eachTradeTime in list(dictFreqGoodsNextOrder[freq][goodsCode].keys()):
                        startTime = eachTradeTime
                        event = dictFreqGoodsNextOrder[freq][goodsCode][startTime]
                        if event.dict_['InstrumentID'] not in listInstrument:
                            dictFreqGoodsNextOrder[freq][goodsCode].pop(startTime)
                            continue
                        orderRef = event.dict_['orderref']
                        endTime = startTime + timedelta(minutes=int(orderRef.split('.')[1]))
                        now = datetime.now() - timedelta(seconds=1.5)
                        if startTime < now < endTime:
                            self.orderCommand(event)
                            dictFreqGoodsNextOrder[freq][goodsCode].pop(startTime)
                        elif now <= startTime:
                            pass
                        elif now >= endTime:
                            dictFreqGoodsNextOrder[freq][goodsCode].pop(startTime)
                        ttt.sleep(0.001)
            pd.to_pickle(dictFreqGoodsNextOrder, 'pickle\\dictFreqGoodsNextOrder.pkl')
            # 检查市价平仓操作
            if not judgeExecTimer():
                return False
            with lockDfOrder:
                dfOrderTemp = dfOrder.copy()
            for row in range(dfOrderTemp.shape[0]):
                orderRef = dfOrderTemp['本地下单码'][row]
                if orderRef[-1] == '9' \
                        and dfOrderTemp['状态'][row] not in ["全部成交", "全部成交报单已提交"] \
                        and dfOrderTemp['状态'][row][:3] != '已撤单':
                    # 判断当时时间是否为时间
                    instrument = dfOrderTemp['代码'][row]
                    goodsCode = getGoodsCode(instrument)
                    # 判断是否在交易时间内
                    if judgeInTradeTime(goodsCode):
                        logTick.info("对编号 {} 进行再次市价下单操作".format(orderRef))
                        if (datetime.now() - dfOrderTemp['时间'][row]) <= timedelta(seconds=10):
                            continue
                        # 检查是否在前一分钟上
                        code = orderRef
                        # 撤单
                        logTick.info("对编号 {} 进行撤单操作".format(code.strip()))
                        cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                        cancelEvent.dict_['orderref'] = code
                        self.orderCancel(cancelEvent)
                        # 下单操作
                        logTick.info("对编号 {} 进行下单操作".format(code.strip()))
                        orderEvent = Event(type_=EVENT_ORDERCOMMAND)
                        orderEvent.dict_['InstrumentID'] = instrument
                        if dfOrderTemp['方向'][row][:1] == '买':
                            directionType = TThostFtdcDirectionType.THOST_FTDC_D_Buy
                        else:
                            directionType = TThostFtdcDirectionType.THOST_FTDC_D_Sell
                        orderEvent.dict_['Direction'] = directionType
                        orderEvent.dict_['CombOffsetFlag'] = chr(TThostFtdcOffsetFlagType.THOST_FTDC_OF_Close.value)
                        orderEvent.dict_['OrderPriceType'] = TThostFtdcOrderPriceTypeType.THOST_FTDC_OPT_AnyPrice
                        orderEvent.dict_['LimitPrice'] = 0
                        orderEvent.dict_['orderref'] = code
                        orderEvent.dict_['VolumeTotalOriginal'] = abs(dfOrderTemp['数量'][row]) - abs(dfOrderTemp['已成交'][row])
                        orderEvent.dict_['preOrderRef'] = code
                        ee.put(orderEvent)
            # 到点执行
            now = datetime.now()
            if self.checkInstrumentChg and time(14, 59, 10) <= now.time() <= time(14, 59, 50):
                self.checkInstrumentChg = False
                self.td.t.ReqQryDepthMarketData()
    #endregion

    # region 定期刷新做多与做空的操作
    def flushDuoKong(self):
        if self.islogin:
            if not judgeExecTimer():
                return False
            with lockDfOrderDB:
                dfOrderDBTemp = dfOrderDB.copy()
            with lockDictFreqPosition:
                dictFreqPositionTemp = dictFreqPosition.copy()
            for freq in listFreq:
                self.TableFreqDuo[freq].clearContents()
                self.TableFreqKong[freq].clearContents()
                self.TableFreqDuo[freq].setRowCount(0)
                self.TableFreqKong[freq].setRowCount(0)
                if dfOrderDBTemp.shape[0] > 0:
                    dfOrderDBTempFreq = dfOrderDBTemp[(pd.DataFrame(dfOrderDBTemp['本地下单码'].str.split('.').tolist())[1] == str(freq))
                                                      & (~dfOrderDBTemp.index.isin(self.listDeadOrderDBIndex))]
                else:
                    dfOrderDBTempFreq = dfOrderDBTemp.copy()
                # 做多持仓
                duoID = 1
                for instrument in dictFreqPositionTemp[freq]['代码'].tolist():
                    index = dictFreqPositionTemp[freq]['代码'].tolist().index(instrument)
                    if dictFreqPositionTemp[freq]['数量'][index] > 0:
                        goodsCode = getGoodsCode(instrument)
                        goodsName = dictGoodsName[goodsCode]
                        price = dictInstrumentPrice[instrument]
                        if price == 0:
                            price = dictData[1][goodsName + '_调整表']['close'][-1]
                        yinKui = (price - dictFreqPositionTemp[freq]['价格'][index]) * dictFreqPositionTemp[freq]['数量'][index] * dictGoodsCheng[goodsCode]
                        self.TableFreqPosition[freq].setItem(index, listFreqPosition.index('持仓盈亏'), QTableWidgetItem(str(yinKui)))
                        dictTemp = {}.fromkeys(listDuoKong)
                        dictTemp['序号'] = duoID
                        dictTemp['名称'] = goodsName
                        dictTemp['合约号'] = instrument
                        dictTemp['数量'] = dictFreqPositionTemp[freq]['数量'][index]
                        dictTemp['状态'] = '持仓'
                        dictTemp['实价'] = price
                        dictTemp['开线'] = dictFreqPositionTemp[freq]['价格'][index]
                        dictTemp['开比例'] = 0
                        dfTemp = dfOrderDBTempFreq[(dfOrderDBTempFreq['合约号'] == instrument)][-1:]
                        if dfTemp.shape[0] > 0:
                            # 做多
                            dictTemp['刷时'] = dfTemp['发单时间'].iat[0].strftime('%H:%M')
                            dictTemp['盈线'] = dfTemp['多止盈线'].iat[0]
                            dictTemp['盈比例'] = (dictTemp['盈线'] - price) / price
                            dictTemp['损线'] = dfTemp['多止损线'].iat[0]
                            dictTemp['损比例'] = (dictTemp['损线'] - price) / price
                            dictTemp['开时'] = dfTemp['发单时间'].iat[0].strftime('%H:%M:%S')
                            dictTemp['初时'] = 9
                            self.TableFreqDuo[freq].setRowCount(duoID)
                            for column in listDuoKong:
                                if '比例' in column:
                                    item = QTableWidgetItem("{0:.2f}%".format(dictTemp[column] * 100))
                                    if '盈' in column or '损' in column:
                                        item.setBackground(QColor(70, 160, 240))
                                    self.TableFreqDuo[freq].setItem(duoID - 1, listDuoKong.index(column),
                                                                    item)
                                else:
                                    item = QTableWidgetItem(str(dictTemp[column]))
                                    if '盈' in column or '损' in column:
                                        item.setBackground(QColor(70, 160, 240))
                                    self.TableFreqDuo[freq].setItem(duoID - 1, listDuoKong.index(column), item)
                            duoID += 1
                # 做空持仓
                kongID = 1
                for instrument in dictFreqPositionTemp[freq]['代码'].tolist():
                    index = dictFreqPositionTemp[freq]['代码'].tolist().index(instrument)
                    if dictFreqPositionTemp[freq]['数量'][index] < 0:
                        goodsCode = getGoodsCode(instrument)
                        goodsName = dictGoodsName[goodsCode]
                        price = dictInstrumentPrice[instrument]
                        if price == 0:
                            price = dictData[1][goodsName + '_调整表']['close'][-1]
                        yinKui = (price - dictFreqPositionTemp[freq]['价格'][index]) * dictFreqPositionTemp[freq]['数量'][index] * dictGoodsCheng[goodsCode]
                        self.TableFreqPosition[freq].setItem(index, listFreqPosition.index('持仓盈亏'),
                                                             QTableWidgetItem(str(yinKui)))
                        dictTemp = {}.fromkeys(listDuoKong)
                        dictTemp['序号'] = kongID
                        dictTemp['名称'] = goodsName
                        dictTemp['合约号'] = instrument
                        dictTemp['数量'] = dictFreqPositionTemp[freq]['数量'][index]
                        dictTemp['状态'] = '持仓'
                        dictTemp['实价'] = price
                        dictTemp['开线'] = dictFreqPositionTemp[freq]['价格'][index]
                        dictTemp['开比例'] = 0
                        dfTemp = dfOrderDBTempFreq[(dfOrderDBTempFreq['合约号'] == instrument)][-1:]
                        if dfTemp.shape[0] > 0:
                            # 做空
                            dictTemp['刷时'] = dfTemp['发单时间'].iat[0].strftime('%H:%M')
                            dictTemp['盈线'] = dfTemp['空止盈线'].iat[0]
                            dictTemp['盈比例'] = (dictTemp['盈线'] - price) / price
                            dictTemp['损线'] = dfTemp['空止损线'].iat[0]
                            dictTemp['损比例'] = (dictTemp['损线'] - price) / price
                            dictTemp['开时'] = dfTemp['发单时间'].iat[0].strftime('%H:%M:%S')
                            dictTemp['初时'] = 9
                            self.TableFreqKong[freq].setRowCount(kongID)
                            for column in listDuoKong:
                                if '比例' in column:
                                    item = QTableWidgetItem("{0:.2f}%".format(dictTemp[column] * 100))
                                    if '盈' in column or '损' in column:
                                        item.setBackground(QColor(70, 160, 240))
                                    self.TableFreqKong[freq].setItem(kongID - 1, listDuoKong.index(column),item)
                                else:
                                    item = QTableWidgetItem(str(dictTemp[column]))
                                    if '盈' in column or '损' in column:
                                        item.setBackground(QColor(70, 160, 240))
                                    self.TableFreqKong[freq].setItem(kongID - 1, listDuoKong.index(column), item)
                            kongID += 1
                # 做多开仓
                for index in dfOrderDBTempFreq.index:
                    if dfOrderDBTempFreq['应开多手数'][index] > 0 and dfOrderDBTempFreq['本地下单码'][index][-1] == '1' and dfOrderDBTempFreq['多开仓线'][index] > 1:
                        instrument = dfOrderDBTempFreq['合约号'][index]
                        goodsCode = getGoodsCode(instrument)
                        goodsName = dictGoodsName[goodsCode]
                        price = dictInstrumentPrice[instrument]
                        if price == 0:
                            price = dictData[1][goodsName + '_调整表']['close'][-1]
                        dictTemp = {}.fromkeys(listDuoKong)
                        dictTemp['序号'] = duoID
                        dictTemp['名称'] = goodsName
                        dictTemp['合约号'] = instrument
                        dictTemp['数量'] = dfOrderDBTempFreq['应开多手数'][index]
                        dictTemp['状态'] = '未开仓'
                        dictTemp['刷时'] = dfOrderDBTempFreq['发单时间'][index].strftime('%H:%M')
                        dictTemp['实价'] = price
                        dictTemp['开线'] = dfOrderDBTempFreq['多开仓线'][index]
                        dictTemp['开比例'] = (dictTemp['开线'] - price) / price
                        dictTemp['盈线'] = dfOrderDBTempFreq['多止盈线'][index]
                        dictTemp['盈比例'] = 0
                        dictTemp['损线'] = dfOrderDBTempFreq['多止损线'][index]
                        dictTemp['损比例'] = 0
                        dictTemp['开时'] = dfOrderDBTempFreq['发单时间'][index].strftime('%H:%M:%S')
                        dictTemp['初时'] = 9
                        self.TableFreqDuo[freq].setRowCount(duoID)
                        for column in listDuoKong:
                            if '比例' in column:
                                item = QTableWidgetItem("{0:.2f}%".format(dictTemp[column] * 100))
                                if '开' in column and column != '开时':
                                    item.setBackground(QColor(70, 160, 240))
                                self.TableFreqDuo[freq].setItem(duoID - 1, listDuoKong.index(column),
                                                                item)
                            else:
                                item = QTableWidgetItem(str(dictTemp[column]))
                                if '开' in column and column != '开时':
                                    item.setBackground(QColor(70, 160, 240))
                                self.TableFreqDuo[freq].setItem(duoID - 1, listDuoKong.index(column),
                                                                item)
                        duoID += 1
                # 做空开仓
                for index in dfOrderDBTempFreq.index:
                    if dfOrderDBTempFreq['应开空手数'][index] > 0 and dfOrderDBTempFreq['本地下单码'][index][-1] == '1' and dfOrderDBTempFreq['空开仓线'][index] > 1:
                        instrument = dfOrderDBTempFreq['合约号'][index]
                        goodsCode = getGoodsCode(instrument)
                        goodsName = dictGoodsName[goodsCode]
                        price = dictInstrumentPrice[instrument]
                        if price == 0:
                            price = dictData[1][goodsName + '_调整表']['close'][-1]
                        dictTemp = {}.fromkeys(listDuoKong)
                        dictTemp['序号'] = kongID
                        dictTemp['名称'] = goodsName
                        dictTemp['合约号'] = instrument
                        dictTemp['数量'] = dfOrderDBTempFreq['应开空手数'][index]
                        dictTemp['状态'] = '未开仓'
                        dictTemp['刷时'] = dfOrderDBTempFreq['发单时间'][index].strftime('%H:%M')
                        dictTemp['实价'] = price
                        dictTemp['开线'] = dfOrderDBTempFreq['空开仓线'][index]
                        dictTemp['开比例'] = (dictTemp['开线'] - price) / price
                        dictTemp['盈线'] = dfOrderDBTempFreq['空止盈线'][index]
                        dictTemp['盈比例'] = 0
                        dictTemp['损线'] = dfOrderDBTempFreq['空止损线'][index]
                        dictTemp['损比例'] = 0
                        dictTemp['开时'] = dfOrderDBTempFreq['发单时间'][index].strftime('%H:%M:%S')
                        dictTemp['初时'] = 9
                        self.TableFreqKong[freq].setRowCount(kongID)
                        for column in listDuoKong:
                            if '比例' in column:
                                item = QTableWidgetItem("{0:.2f}%".format(dictTemp[column] * 100))
                                if '开' in column and column != '开时':
                                    item.setBackground(QColor(70, 160, 240))
                                self.TableFreqKong[freq].setItem(kongID - 1, listDuoKong.index(column),
                                                                item)
                            else:
                                item = QTableWidgetItem(str(dictTemp[column]))
                                if '开' in column and column != '开时':
                                    item.setBackground(QColor(70, 160, 240))
                                self.TableFreqKong[freq].setItem(kongID - 1, listDuoKong.index(column), item)
                        kongID += 1
                self.TableFreqDuo[freq].resizeColumnsToContents()
                self.TableFreqKong[freq].resizeColumnsToContents()

    # endregion

    # region 监测是否应该自动平仓
    def flushPosition(self):
        if self.islogin:
            if not judgeExecTimer1():
                return
            with lockDictFreqOrder:
                dictFreqOrderTemp = dictFreqOrder.copy()
            with lockDictFreqPosition:
                dictFreqPositionTemp = dictFreqPosition.copy()
            now = datetime.now()
            nowTime = now.time()
            for freq in listFreq:
                for instrument in dictFreqPositionTemp[freq]['代码'].tolist():
                    goodsCode = getGoodsCode(instrument)

                    if goodsCode in dictFreqUnGoodsCode[freq]:
                        putLogEvent("在CTA{} 中的 {} 不进行频段品种交易，需要手动平仓".format(freq, instrument))
                        continue

                    if judgeInTradeTime(goodsCode):
                        indexGoods = listGoods.index(goodsCode)
                        for i in range(len(dictGoodsClose[freq][goodsCode])):
                            if i == len(dictGoodsClose[freq][goodsCode]) - 1:
                                iN = 0
                            else:
                                iN = i + 1
                            if dictGoodsClose[freq][goodsCode][i] < dictGoodsClose[freq][goodsCode][iN]:
                                if dictGoodsClose[freq][goodsCode][i] <= nowTime < dictGoodsClose[freq][goodsCode][iN]:
                                    indexBar = i
                                    break
                            else:
                                if dictGoodsClose[freq][goodsCode][i] <= nowTime or nowTime < dictGoodsClose[freq][goodsCode][iN]:
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
                                            putLogBarDealEvent("品种：{} 之前的编号 {} 进行撤单操作".format(instrument, dfFreqOrder['本地下单码'][i]), freq)
                                            cancelEvent = Event(type_=EVENT_ORDERCANCEL)
                                            cancelEvent.dict_['orderref'] = dfFreqOrder['本地下单码'][i]
                                            preOrderRef = dfFreqOrder['本地下单码'][i]
                                            ee.put(cancelEvent)
                            getOrder(freq, goodsCode, orderRef, preOrderRef, True)
    # endregion

    #region 退出程序
    def closeEvent(self, event):
        reply = QMessageBox.question(self, '退出', '是否确定需要退出程序', QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            if hasattr(self, 'thePositionShow'):
                self.thePositionShow.close()
            event.accept()
        else:
            event.ignore()
    #endregion

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ui = RdMdUi()
    ui.show()
    sys.exit(app.exec_())
