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
        self._inited = False
        self.tradingDay = startDate or self.tradingDay
        self.contractListByUnderlyingSymbol = {}  # {'underlyingSymbol': [Contract(), ...]}
        # 今天品种的主力合约
        self.activeContractDic = {}  # {'underlyingSymbol': Contract()}
        self.drData = None

    def init(self):
        """

        :return:
        """
        self.loadOriginDailyDataByDate(self.tradingDay)
        self.loadContractData()
        self.drData = self.drDataLocal
        self._inited = True

    def start(self):
        """

        :return:
        """
        self.init()
        if not self._inited:
            raise ValueError(u'尚未初始化')

        self.run()

    def run(self):

        if self.drData.originDailyDataByDate.shape[0] == 0:
            self.log.info(u'{} 交易日没有任何数据'.format(self.tradingDay))
            return

        # 将合约按照品种分类
        self.sortByUnderlyingSymbol()

        # 判断当前主力合约
        for us in self.contractListByUnderlyingSymbol.keys():
            self.findOldActiveContract(us)

        # 从日线数据找出主力合约
        for us in self.contractListByUnderlyingSymbol.keys():
            self.findNewActiveContract(us)

        # 保存合约数据
        self.saveActiveContraact()

    def sortByUnderlyingSymbol(self):
        """
        将合约按照品种分类
        :return:
        """
        for c in self.drData.contractData.values():
            us = c.underlyingSymbol
            try:
                contractList = self.contractListByUnderlyingSymbol[us]
            except KeyError:
                self.contractListByUnderlyingSymbol[us] = contractList = []

            # 放入同品种的队列
            contractList.append(c)

    def findOldActiveContract(self, unserlyingSymbol):
        us = unserlyingSymbol
        contractList = self.contractListByUnderlyingSymbol.get(us)

        if not contractList:
            return

        activeContract = None

        for c in contractList:
            if c.activeEndDate:
                # 曾经是活跃合约
                if activeContract is None:
                    # 直接替换
                    activeContract = c
                else:
                    # 判断哪个是更新的主力合约
                    if c.activeEndDate > activeContract.activeEndDate:
                        activeContract = c

        if activeContract:
            self.activeContractDic[us] = activeContract

    def loadOriginDailyDataByDate(self, tradingDay):
        """

        :param date:
        :return:
        """
        self.drDataLocal.loadOriginDailyDataByDate(tradingDay)

    def findNewActiveContract(self, underlyingSymbol):
        """
        根据当天日线数据好处主力合约
        :param underlyingSymbol:
        :return:
        """
        us = underlyingSymbol

        df = self.drData.originDailyDataByDate
        df = df[df.underlyingSymbol == us]

        if df.shape[0] == 0:
            return

        activeContract = self.activeContractDic.get(us)
        contractDic = self.drData.contractData

        maxVolumeDf = df[df.volume == df.volume.max()]
        for symbol in maxVolumeDf.symbol:
            c = contractDic[symbol]
            if activeContract is None:
                # 尚未有任何主力合约
                activeContract = c
            else:
                if c.startDate > activeContract.startDate or c.endDate > activeContract.endDate:
                    activeContract = c

        if activeContract:
            # 更新主力合约的始末日期
            activeContract.updateActiveEndDate(self.tradingDay)
            activeContract.updateActiveStartDate(self.tradingDay)
            self.activeContractDic[us] = activeContract

    def saveActiveContraact(self):
        """
        更新主力合约
        :return:
        """
        contracts = {c.symbol: c for c in self.activeContractDic.values()}
        self.drData.updateContracts(contracts)
        if not __debug__:
            self.drDataLocal.updateContracts(contracts)


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
    logging.loaded = False

    startDate = None
    startDate = arrow.get('2014-03-24 00:00:00+08:00').datetime
    endDate = arrow.get('2017-10-27 00:00:00+08:00').datetime

    tradingDay = startDate
    while tradingDay <= endDate:
        c = Contracter(tradingDay, loggingConfig=loggingConfig, **kwargs)
        c.start()
        tradingDay += datetime.timedelta(days=1)
