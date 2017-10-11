import arrow
import json
from wash import Washer
import datetime

import pandas as pd
import tradingtime as tt

from slaveobject import Contract


class Contracter(Washer):
    """
    处理合约信息
    """

    def __init__(self, startDate=None, *args, **kwargs):
        super(Contracter, self).__init__(*args, **kwargs)
        # 开始从哪个交易日开始回溯数据
        self.startTradingDay = startDate or self.tradingDay

        self.contractByUnderlyingSymbol = {}  # {underlyingSymbol: set(Contract())}
        self.contracts = {}

    def run(self):
        self.log.info('isTradingDay: {}'.format(self.isTradingDay))
        self.log.info('startTradingDay {}'.format(self.startTradingDay))

        # 加载合约数据
        self.loadContractData()

        # 加载日线数据
        self.loadOriginDailyData(self.startTradingDay)

        drData = self.drDataLocal

        # 合约详情
        self.contracts = drData.contractData

        # 日线数据
        self.dailyBarDF = drData.originDailyData[['symbol', 'tradingDay', 'volume']].copy()
        self.dailyBarDF['underlyingSymbol'] = self.dailyBarDF['symbol'].apply(tt.contract2name)

        # 根据品种名缓存合约
        self.log.info('根据品种名缓存合约')
        self.refreshContractByUnderlyingSymbol()

        # 更新合约详情中的数据始末日期
        self.log.info('更新合约详情中的数据始末日期')
        self.updateStartEndDate()

        # 根据品种名缓存合约
        self.log.info('再次 根据品种名缓存合约')
        self.refreshContractByUnderlyingSymbol()

        # 按照品种分组
        group = self.dailyBarDF.groupby('underlyingSymbol').size()
        underlyingSymbolList = group.index.values

        # 按照日期排序，逐日更新主力合约
        for underlyingSymbol in underlyingSymbolList:
            self.updateActiveByUnderlyingSymbol(underlyingSymbol)

        # 将合约存库
        self.saveContracts()

    def updateActiveByUnderlyingSymbol(self, underlyingSymbol):
        # 逐日，按品种，跟新合约
        df = self.dailyBarDF
        # 选取品种
        df = df[df.underlyingSymbol == underlyingSymbol]
        # 根据日期排序
        df = df.sort_values('tradingDay', inplace=False)

        # 获得当前的主力合约
        try:
            contractList = [c for c in self.contractByUnderlyingSymbol[underlyingSymbol] if c.startDate is not None]
        except KeyError:
            self.log.warning(u'{} 没有任何合约'.format(underlyingSymbol))
            return

        contractList.sort(key=lambda c: (c.startDate, c.symbol))

        # currentActiveContract = contractList[0]
        # for con in contractList:
        #     if con.activeStartDate is None:
        #         continue
        #     if currentActiveContract.activeStartDate is None:
        #         currentActiveContract = con
        #     if con.activeStartDate >= currentActiveContract.activeStartDate:
        #         currentActiveContract = con

        # 获得每天的主力合约
        activContract = df.groupby('tradingDay').apply(lambda t: t[t.volume == t.volume.max()])
        activContract = activContract.sort_values('tradingDay', inplace=False)
        activeContractList = activContract.to_dict('record')

        dic = activeContractList[0]
        symbol, tradingDay = dic['symbol'], dic['tradingDay']
        currentActive = self.contracts[symbol]
        currentActive.updateActiveStartDate(tradingDay)
        currentActive.updateActiveEndDate(tradingDay)

        for dic in activeContractList:
            # 逐日计算
            symbol, tradingDay = dic['symbol'], dic['tradingDay']
            con = self.contracts[symbol]
            if currentActive is con:
                # 仍然是主力合约
                # 直接更新
                currentActive.updateActiveStartDate(tradingDay)
                # 主力最后一天
                currentActive.updateActiveEndDate(tradingDay)
            else:
                # 跟主力合约 不一样 ，比较是否数据起始日期
                if con.startDate and con.startDate > currentActive.startDate:
                    # 主力合约被替换
                    currentActive = con
                    # 设定期货主力合约起始日
                    currentActive.updateActiveStartDate(tradingDay)
                    # 主力最后一天
                    currentActive.updateActiveEndDate(tradingDay)
                else:
                    # 跟主力合约不一样，且没有主力合约没有被替换
                    currentActive.updateActiveEndDate(tradingDay)

    def updateStartEndDate(self):
        dailyBarDF = self.dailyBarDF
        contracts = self.contracts
        tradingDays = dailyBarDF.groupby('symbol')['tradingDay']
        # 起始日期
        startDates = tradingDays.last()
        endDates = tradingDays.first()

        dates = pd.DataFrame({'endDate': endDates, 'startDate': startDates})
        dates = dates.reset_index(inplace=False)

        for dic in dates.to_dict('record'):
            # dic = {symbol:'hc1801', 'startDate':datetime(), 'endDate': datetime()}
            symbol, startDate, endDate = dic['symbol'], dic['startDate'], dic['endDate']
            try:
                c = contracts[symbol]
                c.updateDate(startDate, endDate)
                assert isinstance(c, Contract)
            except KeyError:
                # 尚未有这个合约
                us = tt.contract2name(symbol)
                for c in contracts.values():
                    # 寻找相同品种的合约顶替
                    if c.underlyingSymbol == us:
                        assert isinstance(c, Contract)
                        c = Contract(c.originData)
                        # 替换掉合约名即可
                        c.symbol = symbol
                        c.vtSymbol = symbol
                        contracts[symbol] = c
                        c.updateDate(startDate, endDate)
                        break
                else:
                    self.log.warning(u'找不到相同品种的合约 {}'.format(symbol))
                    continue

    def refreshContractByUnderlyingSymbol(self):
        contracts = self.contracts
        cbus = self.contractByUnderlyingSymbol
        for symbol, c in contracts.items():
            try:
                cs = cbus[c.underlyingSymbol]
                cs.add(c)
            except KeyError:
                cbus[c.underlyingSymbol] = {c}

    def saveContracts(self):

        self.drDataLocal.updateContracts(self.contracts)

        self.drDataRemote.updateContracts(self.contracts)


if __name__ == '__main__':
    settingFile = 'conf/kwarg.json'
    loggingConfigFile = 'conf/logconfig.json'
    serverChanFile = 'conf/serverChan.json'

    if __debug__:
        settingFile = 'tmp/kwarg.json'
        loggingConfigFile = 'tmp/logconfig.json'

    with open(serverChanFile, 'r') as f:
        serverChanUrls = json.load(f)['serverChanSlaveUrls']

    with open(settingFile, 'r') as f:
        kwargs = json.load(f)

    with open(loggingConfigFile, 'r') as f:
        loggingConfig = json.load(f)

    startDate = None
    startDate = arrow.get('2011-01-01 00:00:00+08:00').datetime
    c = Contracter(startDate, loggingConfig=loggingConfig, **kwargs)
    # c.startTradingDay -= datetime.timedelta(days=1)
    c.start()
