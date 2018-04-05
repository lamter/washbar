import sys
import datetime
import logging
import logging.config
import pytz
from threading import Thread
from itertools import chain
import json
from time import sleep
import traceback

import arrow
import pymongo
from pymongo.errors import ServerSelectionTimeoutError
import tradingtime as tt
import numpy as np
import pandas as pd

from drdata import DRData


class Washer(object):
    """

    """

    LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')

    def __init__(self, mongoConf, tradingDayTmp, startDate=None):
        self.log = logging.getLogger()

        self.log4mongoActive = True
        self.keeplog4mongo = Thread(target=self._keeplog4mongo, name='keeplog4mongo')

        self.mongoConf = mongoConf  # [{conf}, {conf}]
        self.mongoCollections = []  # 是 collections, 不是 client, db

        # self.initLog(loggingConfigFile)

        self.drDataLocal = DRData(self, DRData.TYPE_LOCAL, mongoConf['mongoLocal'])
        self.drDataRemote = DRData(self, DRData.TYPE_REMOTE, mongoConf['mongoRemote'])

        if startDate is None:
            startDate = datetime.datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)

            # 设定当前要处理的交易日
            self.isTradingDay, self.tradingDay = tt.get_tradingday(startDate)
        else:
            self.tradingDay = startDate
            self.isTradingDay = True

        if not self.tradingDay.tzinfo:
            self.tradingDay = self.LOCAL_TIMEZONE.localize(self.tradingDay)

        # 核对tradingDay 是否已经完成过了
        self.tradingDayTmp = tradingDayTmp

    def start(self):
        try:
            self.run()
        except:
            self.stop()
            raise

    def run(self):
        # 检查已经清洗的交易日，如果校验不通过，则会在此抛出异常
        self.checkTradingCache()

        self.log.info('isTradingDay: {}'.format(self.isTradingDay))
        self.log.info('清洗 {} 的数据'.format(self.tradingDay.date()))

        # 启动循环
        self.drDataLocal.start()
        self.drDataRemote.start()

        # 清除多余的 bar
        self.loadOriginData()
        self.clearBar()

        # 重新聚合所有合约的 1min bar 数据，并存库
        self.aggregatedAll()

        self.stop()

        self.log.info('清洗结束')

    def stop(self):
        # 结束存库循环
        self.drDataLocal.stop()
        self.drDataRemote.stop()
        self.log4mongoActive = False

    def loadOriginData(self):
        """
        从数据库加载数据
        :return:
        """

        t = Thread(target=self.drDataRemote.loadOriginData)
        t.start()
        self.drDataLocal.loadOriginData()
        if t.isAlive():
            t.join()

    def loadOriginDailyData(self, startDate):
        """
        从数据库加载数据
        :return:
        """
        # t = Thread(target=self.drDataRemote.loadOriginDailyData, kwargs={'startDate': startDate})
        # t.start()
        self.drDataLocal.loadOriginDailyData(startDate)
        # if t.isAlive():
        #     t.join()

    def loadContractData(self):
        """
        加载合约详情数据
        :param startData:
        :return:
        """
        t = Thread(target=self.drDataRemote.loadContractData)
        t.start()
        self.drDataLocal.loadContractData()
        if t.isAlive():
            t.join()

    def clearBar(self):
        """
        一个 bar 只能有一个，并且清除多余的 bar
        :return:
        """
        t = Thread(target=self.drDataRemote.clearBar())
        t.start()
        self.drDataLocal.clearBar()
        if t.isAlive():
            t.join()

    def aggregatedAll(self):
        """
        再次聚合数据
        :return:
        """
        # 聚合数据
        symbolsChain = chain(self.drDataLocal.originData.keys(), self.drDataRemote.originData.keys())
        for symbol in symbolsChain:
            self.aggregate(symbol)

    def aggregate(self, symbol):
        """
        聚合指定合约的数据
        :param symbol:
        :return:
        """
        localData = self.drDataLocal.originData.get(symbol)
        remoteData = self.drDataRemote.originData.get(symbol)

        # # 整个合约数据丢失
        isNeedAggregate = True
        if localData is not None and remoteData is not None:
            # 两者都有数据
            df = localData.append(remoteData)
            isNeedAggregate = True
        elif localData is None:
            # 本地完全没有 这个合约的数据
            # self.drDataLocal.makeupBar(symbol, remoteData)
            df = remoteData.copy()
            isNeedAggregate = False
        elif remoteData is None:
            # self.drDataRemote.makeupBar(symbol, localData)
            # return
            df = localData.copy()
            isNeedAggregate = False
        else:
            # 两个都是错误的
            self.log.warning('local 和 remote 都没有 symbol {} 的数据'.format(symbol))
            return

        # 衔接两地数据

        assert isinstance(df, pd.DataFrame)
        df = df.set_index('datetime', drop=False).sort_index()

        if isNeedAggregate:
            # 聚合
            ndf = self.resample1MinBar(df)
        else:
            ndf = df.copy()

        # 原始数据中, volume 是总量，这里用 vol 替换
        # vol 此时为总量，同时重新定义 volume 为增量
        # 计算 增量 volume
        ndf['vol'] = ndf['volume']
        volumeSeries = ndf.volume.diff()
        volumeSeries.apply(lambda vol: np.nan if vol < 0 else vol)
        volumeSeries.iat[0] = ndf.volume[0]
        volumeSeries = volumeSeries.fillna(method='bfill')
        volumeSeries.astype('int')
        ndf['volume'] = volumeSeries
        # print(ndf[['vol', 'volume']].tail())

        # 将新数据保存
        self.drDataLocal.updateData(ndf)
        self.drDataRemote.updateData(ndf)

    @staticmethod
    def resample1MinBar(df):
        r = df.resample('1T', closed='left', label='left')
        close = r.close.last()
        date = r.date.last()
        high = r.high.max()
        low = r.low.min()
        # lowerLimit = df['lowerLimit'][0]  # r.lowerLimit.first()
        _open = r.open.first()
        openInterest = r.openInterest.max()
        symbol = df['symbol'][0]  # r.symbol.
        time = r.time.last()
        tradingDay = df['tradingDay'][0]
        # upperLimit = df['upperLimit'][0]
        volume = r.volume.max()

        # 构建新的完整的数据
        ndf = pd.DataFrame({
            'close': close,
            'date': date,
            'high': high,
            'low': low,
            # 'lowerLimit': lowerLimit,
            'open': _open,
            'openInterest': openInterest,
            'symbol': symbol,
            'time': time,
            'tradingDay': tradingDay,
            # 'upperLimit': upperLimit,
            'volume': volume,
        }).dropna(how='any')
        ndf.openInterest = ndf.openInterest.astype('int')
        ndf.volume = ndf.volume.astype('int')
        ndf['datetime'] = ndf.index
        return ndf

    @staticmethod
    def resample1DayBar(df):
        r = df.resample('1T', closed='left', label='left')
        close = r.close.last()
        #     date = r.date.last()
        date = df.date[-1]
        high = r.high.max()
        low = r.low.min()
        try:
            lowerLimit = df['lowerLimit'][0]  # r.lowerLimit.first()
            upperLimit = df['upperLimit'][0]
        except KeyError:
            upperLimit = lowerLimit = np.nan
        _open = r.open.first()
        openInterest = r.openInterest.last()
        symbol = df['symbol'][0]  # r.symbol.
        time = r.time.last()
        volume = r.volume.sum()

        # 构建新的完整的数据
        ndf = pd.DataFrame({
            'close': close,
            'date': date,
            'high': high,
            'low': low,
            'lowerLimit': lowerLimit,
            'open': _open,
            'openInterest': openInterest,
            'symbol': symbol,
            'time': time,
            'upperLimit': upperLimit,
            'volume': volume,
        })

        # 抛弃纵轴全为null的数据
        ndf.dropna(axis=1, how='all')
        ndf.dropna(how='any')
        ndf.openInterest = ndf.openInterest.astype('int')
        ndf.volume = ndf.volume.astype('int')
        ndf['datetime'] = ndf.index
        ndf['tradingDay'] = ndf.index
        return ndf

    def _keeplog4mongo(self):
        """

        :return:
        """
        for h in self.log.handlers:
            if hasattr(h, 'connect'):
                c = h.connect
                break
        else:
            self.log.warning(u'log4mongo 未连接')
            return
        num = 0
        while self.log4mongoActive:
            sleep(1)
            num += 1
            if num % 60 == 0:
                c.server_info()

    def checkTradingCache(self):
        """
        检查已经清洗的交易日
        :return:
        """
        # return
        try:
            with open(self.tradingDayTmp, 'r') as f:
                tradingDayCache = arrow.get(f.read()).datetime
                self.log.debug('已经清洗的交易日 {}'.format(tradingDayCache))
        except FileNotFoundError:
            self.log.debug('{} 没有已清洗的交易日缓存'.format(self.tradingDayTmp))
            pass
        else:
            if self.tradingDay <= tradingDayCache:
                err = '行情数据已经清洗到了 {}, 数据清洗日期 {} 停止'.format(tradingDayCache, self.tradingDay)
                if __debug__:
                    self.log.debug('调试时请自行删除 {} 文件'.format(self.tradingDayTmp))
                self.log.critical(err)
                raise ValueError(err)

        # 保存已经清洗过的 tradingDay
        with open(self.tradingDayTmp, 'w') as f:
            f.write(str(self.tradingDay))


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

    logging.loaded = False

    w = Washer(loggingConfig=loggingConfig, **kwargs)
    import arrow

    w.tradingDay = arrow.get('2017-10-24 00:00:00+08:00').datetime
    w.start()
