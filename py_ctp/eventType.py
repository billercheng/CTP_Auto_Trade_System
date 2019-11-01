# encoding: UTF-8

'''
本文件仅用于存放对于事件类型常量的定义。

由于python中不存在真正的常量概念，因此选择使用全大写的变量名来代替常量。
这里设计的命名规则以EVENT_前缀开头。

常量的内容通常选择一个能够代表真实意义的字符串（便于理解）。

建议将所有的常量定义放在该文件中，便于检查是否存在重复的现象。
'''


EVENT_TIMER = 'eTimer'  # 计时器事件，每隔1秒发送一次
EVENT_LOG = 'eLog'  # 记录程序日志
EVENT_LOGTICK = 'eTickLog'  # 记录tick数据
EVENT_LOGBARDEAL = 'eBarDeal'  # 该bar数据输出操作记录
EVENT_INSTRUMENT = 'eInstrument'
EVENT_MARKETDATA = 'eMarketdata'
EVENT_ACCOUNT = 'eAccount'
EVENT_POSITION = 'ePosition'
EVENT_MARKETDATA_CONTRACT = 'eContract'
EVENT_LOGBAR = 'eBarLog'
EVENT_ORDER = 'eOrder'
EVENT_POSITION = 'ePosition'
EVENT_ORDERCOMMAND = 'eOrderCommand'
EVENT_TRADE = 'e_Trade'
EVENT_ORDER_ERROR = 'eOrderError'  # 记录错误委托信息
EVENT_RSPQRYORDER = 'eRspQryOrder'
EVENT_CLEAR_POSITION = 'eClearPosition'
EVENT_WIND = 'eWind'
EVENT_ORDERCOMMANDPARK = 'EVENT_ORDERCOMMANDPARK'
EVENT_ORDERCANCEL = 'EVENT_ORDERCANCEL'  # 撤单事件操作
EVENT_ORDERCANCELPARK = 'EVENT_ORDERCANCELPARK'  # 预撤单事件操作
EVENT_SHOWORDERDB = 'event_showorderdb'
EVENT_ORDERPARK = 'event_orderpark'
EVENT_SHOWPOSITION = 'event_showposition'
EVENT_LOGIN = 'event_Login'
EVENT_ACCOUNTPOSITION = 'event_allposition'
EVENT_MARKETORDER = 'event_marketorder'
#----------------------------------------------------------------------
def test():
    """检查是否存在内容重复的常量定义"""
    check_dict = {}
    
    global_dict = globals()    
    
    for key, value in global_dict.items():
        if '__' not in key:                       # 不检查python内置对象
            if value in check_dict:
                check_dict[value].append(key)
            else:
                check_dict[value] = [key]
            
    for key, value in check_dict.items():
        if len(value)>1:
            print(u'存在重复的常量定义:{}'.format(str(key)))
            for name in value:
                print(name)
            print('')
        
    print(u'测试完毕')

# 直接运行脚本可以进行测试
if __name__ == '__main__':
    test()