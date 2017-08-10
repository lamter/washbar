import sys
import datetime
import logging
import logging.config
import pytz
from threading import Thread
from itertools import chain

import pymongo
from pymongo.errors import ServerSelectionTimeoutError
import tradingtime as tt
import pandas as pd

from drdata import DRData


class Washer(object):
    """

    """

    LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')

    def __init__(self, mongoConf, loggingConfig=None):
        self.mongoConf = mongoConf  # [{conf}, {conf}]
        self.mongoCollections = []  # 是 collections, 不是 client, db

        self.initLog(loggingConfig)
        self.log = logging.getLogger()

        self.drDataLocal = DRData(self, DRData.TYPE_LOCAL, mongoConf['mongoLocal'])
        self.drDataRemote = DRData(self, DRData.TYPE_REMOTE, mongoConf['mongoRemote'])

        # 设定当前要处理的交易日
        self.isTradingDay, self.tradingDay = tt.get_tradingday(
            datetime.datetime.now().replace(hour=8, minute=0, second=0, microsecond=0))
        self.tradingDay = self.LOCAL_TIMEZONE.localize(self.tradingDay)

        self.log.info('isTradingDay {}; tradingDay  '.format(self.isTradingDay, self.tradingDay))

    def initLog(self, loggingconf):
        """
        初始化日志
        :param loggingconf:
        :return:
        """
        if loggingconf:
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
        """

        :return:
        """

        self.log.info('清洗 {} 的数据'.format(self.tradingDay.date()))

        # 清除多余的 bar
        self.loadOriginData()
        self.clearBar()

        # 多个 bar 之间的数据
        self.aggregatedAll()

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

        # assert isinstance(localData, pd.DataFrame)
        # assert isinstance(remoteData, pd.DataFrame)

        # 整个合约数据丢失
        if localData is not None and remoteData is not None:
            # 两者都有数据
            self.log.debug('{} 两地都有数据')
            pass
        elif localData is None:
            # 本地完全没有 这个合约的数据
            self.drDataLocal.makeupBar(symbol, remoteData)
            return
        elif remoteData is None:
            self.drDataRemote.makeupBar(symbol, localData)
            return
        else:
            # 两个都是错误的
            self.log.critical('local 和 remote 都没有 symbol {} 的数据'.format(symbol))
            return

        # 衔接两地数据
        df = localData.append(remoteData)
        assert isinstance(df, pd.DataFrame)
        df = df.set_index('datetime').sort_index()

        # 聚合
        r = df.resample('1T', closed='left', label='left')
        close = r.close.last()
        date = r.date.last()
        high = r.high.max()
        low = r.low.min()
        lowerLimit = df['lowerLimit'][0]  # r.lowerLimit.first()
        _open = r.open.first()
        openInterest = r.openInterest.max()
        symbol = df['symbol'][0]  # r.symbol.
        time = r.time.last()
        tradingDay = df['tradingDay'][0]
        upperLimit = df['upperLimit'][0]
        volume = r.volume.max()

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
            'tradingDay': tradingDay,
            'upperLimit': upperLimit,
            'volume': volume,
        }).dropna(how='any')
        ndf.openInterest = ndf.openInterest.astype('int')
        ndf.volume = ndf.volume.astype('int')
        ndf['datetime'] = ndf.index

        # 对比并更新数据
        self.drDataLocal.aggreate(ndf, localData)
        # self.drDataRemote.aggreate(ndf, remoteData)
