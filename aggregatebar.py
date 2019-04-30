import logging
import json
from itertools import chain
import pandas as pd

from wash import Washer


class AggregateBar(Washer):
    """
    1. 聚合各种周期的数据，并存库
    2. 基于清洗完成后的 1min bar 来聚合
    3. 一般情况下这个 bar 聚合是跟 washer 一起运行的。就不需要再向 slavem 汇报了

    """

    def run(self):
        """

        :return:
        """
        self.log.info('isTradingDay: {}'.format(self.isTradingDay))
        self.log.info('聚合 {} 的数据'.format(self.tradingDay.date()))

        # 汇报
        # self.slavemReport.lanuchReport()

        # 启动循环
        # self.drDataLocal.start()
        # self.drDataRemote.start()

        # 从本地加载数据即可，此时已经完成了两地数据互补，两地数据已经一致了
        self.drDataLocal.loadOriginData()

        # 聚合5分钟 bar
        # 聚合15分钟 bar
        # 聚合30分钟 bar
        # 聚合1小时 bar

        # 聚合日线数据
        self.aggregatedAllDayBar()

        # 重新聚合所有合约的 1min bar 数据，并存库
        self.stop()

        self.log.info('聚合结束')

    def aggregatedAllDayBar(self):
        """
        聚合日线数据
        :return:
        """
        vtSymbolsChain = list(chain(self.drDataLocal.originData.keys(), self.drDataRemote.originData.keys()))
        self.log.info('共 {} 个合约'.format(len(vtSymbolsChain),))
        for vtSymbol in vtSymbolsChain:
            self.aggregatedDayBar(vtSymbol)

    def aggregatedDayBar(self, vtSymbol):
        """
        聚合指定合约的日线数据
        :param vtSymbol:
        :return:
        """
        originData = self.drDataLocal.originData.get(vtSymbol)

        if originData is None:
            # self.log.warning('symbol {} local 没有数据可以聚合'.format(symbol))
            return

        assert isinstance(originData, pd.DataFrame)

        df = originData.set_index('tradingDay').sort_index()

        # 聚合日线
        ndf = self.resample1DayBar(df)

        # 更新数据
        self.drDataLocal.updateDayData(ndf)
        self.drDataRemote.updateDayData(ndf)


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

    import datetime

    a = AggregateBar(loggingConfig=loggingConfig, **kwargs)
    import arrow

    # a.tradingDay = arrow.get('2017-08-24 00:00:00+08:00').datetime
    # print(a.tradingDay)
    a.start()
