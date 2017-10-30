import sys
import datetime
import logging
import logging.config
import pytz
from threading import Thread
from itertools import chain
import json

import pymongo
from pymongo.errors import ServerSelectionTimeoutError
import tradingtime as tt
import numpy as np
import pandas as pd
from slavem import Reporter

from drdata import DRData


class Washer(object):
    """

    """

    LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')

    def __init__(self, mongoConf, slavemConf, loggingConfig=None):
        self.mongoConf = mongoConf  # [{conf}, {conf}]
        self.mongoCollections = []  # 是 collections, 不是 client, db

        # slavem 汇报
        self.slavemReport = Reporter(**slavemConf)

        self.initLog(loggingConfig)

        self.drDataLocal = DRData(self, DRData.TYPE_LOCAL, mongoConf['mongoLocal'])
        self.drDataRemote = DRData(self, DRData.TYPE_REMOTE, mongoConf['mongoRemote'])

        # 设定当前要处理的交易日
        self.isTradingDay, self.tradingDay = tt.get_tradingday(
            datetime.datetime.now().replace(hour=8, minute=0, second=0, microsecond=0))
        self.tradingDay = self.LOCAL_TIMEZONE.localize(self.tradingDay)

    def initLog(self, loggingconf):
        """
        初始化日志
        :param loggingconf:
        :return:
        """
        if loggingconf:
            if not logging.loaded:
                logging.config.loaded = True
                # log4mongo 的bug导致使用非admin用户时，建立会报错。
                # 这里使用注入的方式跳过会报错的代码
                import log4mongo.handlers
                log4mongo.handlers._connection = pymongo.MongoClient(
                    host=loggingconf['handlers']['mongo']['host'],
                    port=loggingconf['handlers']['mongo']['port'],
                )

                try:
                    logging.config.dictConfig(loggingconf)
                except ServerSelectionTimeoutError:
                    print(u'Mongohandler 初始化失败，检查 MongoDB 否正常')
                    raise
            self.log = logging.getLogger('root')
        else:
            self.log = logging.getLogger()
            self.log.setLevel('DEBUG')
            fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
            # datefmt = "%a-%d-%b %Y %H:%M:%S"
            datefmt = None
            formatter = logging.Formatter(fmt, datefmt)
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(formatter)
            sh.setLevel('DEBUG')
            self.log.addHandler(sh)

            sh = logging.StreamHandler(sys.stderr)
            sh.setFormatter(formatter)
            sh.setLevel('WARN')
            self.log.addHandler(sh)
            self.log.warning(u'未配置 loggingconfig')

    def start(self):
        try:
            self.run()
        except:
            self.stop()
            raise

    def run(self):
        """

        :return:
        """
        self.log.info('isTradingDay: {}'.format(self.isTradingDay))
        self.log.info('清洗 {} 的数据'.format(self.tradingDay.date()))

        # 汇报
        self.slavemReport.lanuchReport()

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

        # 计算 volume 增量 vol
        volumeSeries = ndf.volume.diff()
        volumeSeries.apply(lambda vol: np.nan if vol < 0 else vol)
        volumeSeries.iat[0] = ndf.volume[0]
        volumeSeries = volumeSeries.fillna(method='bfill')
        volumeSeries.astype('int')
        ndf['vol'] = volumeSeries
        # print(ndf[['vol', 'volume']].tail())

        # 将新数据保存
        self.drDataLocal.updateData(ndf)
        if not __debug__:
            self.drDataRemote.updateData(ndf)
            # 保存日线数据
            # ndf = ndf.set_index('tradingDay')
            # dayndf = self.resample1DayBar(ndf)
            # self.drDataLocal.updateDayData(dayndf)
            # if not __debug__:
            #     self.drDataRemote.updateDayData(dayndf)

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
        volume = r.volume.last()

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
