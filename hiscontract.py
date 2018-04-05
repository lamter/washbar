import arrow
import json
from wash import Washer
import datetime

import pandas as pd
import tradingtime as tt

from slaveobject import Contract


class HisContracter(Washer):
    """
    补齐历史合约
    """

    def __init__(self, startDate=None, *args, **kwargs):
        super(HisContracter, self).__init__(*args, **kwargs)
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
        if drData.originDailyData.shape[0] == 0:
            return
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

        # 将合约存库
        self.saveContracts()

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
                try:
                    contractsByUnderlying = list(self.contractByUnderlyingSymbol[us])
                except KeyError:
                    self.log.warning(u'找不到相同品种的合约 {}'.format(symbol))
                    continue

                # 采用最新的合约,以防热门合约的保证金变化导数值失真
                contractsByUnderlying.sort(key=lambda c: c.vtSymbol, reverse=True)
                for c in contractsByUnderlying:
                    # 寻找相同品种的合约顶替
                    if c.underlyingSymbol == us:
                        assert isinstance(c, Contract)
                        c = Contract(c.originData)
                        # 替换掉合约名即可
                        c.symbol = symbol
                        c.vtSymbol = symbol
                        contracts[symbol] = c
                        c.updateDate(startDate, endDate)
                        c.activeStartDate = None
                        c.activeEndDate = None
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

    import logging
    logging.loaded= False

    startDate = None
    startDate = arrow.get('2011-01-01 00:00:00+08:00').datetime
    # endDate = arrow.get('2017-10-27 00:00:00+08:00').datetime
    c = HisContracter(startDate, loggingConfig=loggingConfig, **kwargs)
    c.start()
