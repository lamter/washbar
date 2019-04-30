import datetime

import tradingtime as tt
import pymongo
from pymongo.errors import OperationFailure
from pymongo import IndexModel, ASCENDING, DESCENDING
import logging
from bson.codec_options import CodecOptions
from threading import Thread
from queue import Queue, Empty
import arrow

import pandas as pd

from slaveobject import Contract


class DRData(object):
    """
    每个 dr 数据实例
    """
    TYPE_REMOTE = 'remote'
    TYPE_LOCAL = 'local'

    def __init__(self, mainEngine, type, mongoConf):
        """

        :param mongoConf: {}
        """
        self.type = type
        self.mainEngine = mainEngine
        self.mongoConf = mongoConf
        logName = 'drDataLocal' if self.type == self.TYPE_LOCAL else 'drDataRemote'
        self.log = logging.getLogger(logName)

        # 初始化 MongoDB 链接
        l = '建立 mongo 链接 {host}:{port} {dbn}.{collection}'.format(**mongoConf)
        self.log.info(l)
        db = pymongo.MongoClient(mongoConf['host'], mongoConf['port'])[mongoConf['dbn']]
        db.authenticate(mongoConf['username'], mongoConf['password'])
        db.client.server_info()
        self.db = db

        # 1min bar 的 collection
        self.bar_1minCollection = db[mongoConf['collection']].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=self.mainEngine.LOCAL_TIMEZONE))

        # 日线的 collection
        self.bar_1dayCollection = db[mongoConf['dayCollection']].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=self.mainEngine.LOCAL_TIMEZONE))

        # 合约 的 collection
        self.contractCollection = db[mongoConf['contractCollection']].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=self.mainEngine.LOCAL_TIMEZONE))

        # 初始化日线collection
        self.initBarCollection()

        # 初始化合约collection
        self.initContractCollection()

        # 原始数据
        self.originData = {}  # {vtSymbol: DataFrame()}
        self.originDailyData = None  # DataFrame()
        self.originDailyDataByDate = None  # DataFrame()
        self.contractData = {}  # {vtSymbol: Contract()}
        self._active = False
        self.queue = Queue()
        self._run = Thread(target=self.__run, name="{} 存库".format(self.type))

    @property
    def isLocal(self):
        return self.type == self.TYPE_LOCAL

    def loadOriginData(self):
        """
        从数据库加载数据
        :return:
        """
        self.originData.clear()
        sql = {'tradingDay': self.mainEngine.tradingDay}

        cursor = self.bar_1minCollection.find(sql)
        df = pd.DataFrame([i for i in cursor])

        self.log.info('加载了 {} 条数据'.format(df.shape[0]))

        if df.shape[0] > 0:
            group = df.groupby('vtSymbol').size()
            for vtSymbol in group.index:
                d = df[df.vtSymbol == vtSymbol].sort_values('datetime')
                # {vtSymbol: dataframe()}
                self.originData[vtSymbol] = d

            self.log.info('加载了 {} 个合约'.format(group.shape[0]))

    def loadOriginDailyData(self, startDate):
        """
        从日线数据库加载数据，加载 startDate 之后的数据
        :return:
        """
        self.originData.clear()
        sql = {'tradingDay': {
            '$gte': startDate,
        }}

        cursor = self.bar_1dayCollection.find(sql, {'_id': 0}).hint('tradingDay')

        df = pd.DataFrame([i for i in cursor])

        self.log.info('加载了 {} 日线条数据'.format(df.shape[0]))
        self.originDailyData = df

        if df.shape[0] > 0:
            group = df.groupby('vtSymbol').size()
            self.log.info('加载了 {} 个合约'.format(group.shape[0]))

    def loadOriginDailyDataByDate(self, tradingDay):
        """
        从数据库加载数据，记载指定日期的日线数据
        :return:
        """
        self.originData.clear()
        sql = {'tradingDay': tradingDay}

        cursor = self.bar_1dayCollection.find(sql, {'_id': 0}).hint('tradingDay')

        self._loadOriginDailyDataByDate(cursor)

    def _loadOriginDailyDataByDate(self, cursor):
        df = pd.DataFrame([i for i in cursor])
        self.log.info('加载了 {} 日线条数据'.format(df.shape[0]))

        if df.shape[0] > 0:
            df['underlyingSymbol'] = df.vtSymbol.apply(tt.contract2name)

        self.originDailyDataByDate = df

        if df.shape[0] > 0:
            group = df.groupby('vtSymbol').size()
            self.log.info('加载了 {} 个合约'.format(group.shape[0]))

    def loadContractData(self):
        """
        加载合约详情数据
        :param startDate:
        :return:
        """
        sql = {

        }
        cursor = self.contractCollection.find(sql, {'_id': 0})
        self._loadContractData(cursor)

    def _loadContractData(self, cursor):
        for od in cursor:
            c = Contract(od)
            self.contractData[c.vtSymbol] = c

    def clearBar(self):
        """
        清除多余的 bar ，如果多个 bar 的 volume 没有递增，说明这些 bar 没有新的成交。只保留第一个 bar
        :return:
        """
        for vtSymbol, odf in self.originData.items():
            df = odf[['datetime', 'vtSymbol', 'volume']].copy()

            # 去重后的数据
            dunplicatedSeries = df.duplicated(['datetime', 'vtSymbol', 'volume'])

            # 更新数据
            ndf = odf[dunplicatedSeries == False]

            self.originData[vtSymbol] = ndf

    def aggreate(self, ndf, localData):

        ndf = ndf.copy()
        # 将聚合后的 ndf 和原来的数据对比
        localDF = localData.set_index('datetime')
        # 将 ndf 的 Index 和 localData 的合并，以对其数据进行比较
        newLocalDF = pd.DataFrame(localDF.to_dict(), index=ndf.index.copy())

        # 出现变动需要更新的
        o = newLocalDF['open'] == ndf['open']
        h = newLocalDF['high'] == ndf['high']
        l = newLocalDF['low'] == ndf['low']
        c = newLocalDF['close'] == ndf['close']
        oi = newLocalDF['openInterest'] == ndf['openInterest']
        v = newLocalDF['volume'] == ndf['volume']
        newLocalDF['upsert'] = (o & h & l & c & oi & v).apply(lambda x: not x)

        # 获得需要更新的 bar
        ndf['_id'] = newLocalDF['_id']
        upsertDF = ndf[newLocalDF.upsert]

        insertManyDF = upsertDF[upsertDF._id.isnull()]

        vtSymbol = ndf['vtSymbol'][0]

        n = insertManyDF.shape[0]
        if n > 0:
            self.log.info('{} 补充了 {} 个 bar'.format(vtSymbol, n))

        # 批量保存的
        if insertManyDF.shape[0] > 0:
            del insertManyDF['_id']
            r = self.bar_1minCollection.insert_many(insertManyDF.to_dict('records'))

        # 逐个更新的
        updateOneDF = upsertDF[upsertDF._id.isnull().apply(lambda x: not x)]

        n = upsertDF.shape[0]
        if n > 0:
            self.log.info('{} 更新到了 {} 个 bar'.format(vtSymbol, 0))
        if updateOneDF.shape[0] > 0:
            documents = updateOneDF.to_dict('records')

            # 存库
            for d in documents:
                sql = {
                    '_id': d.pop('_id'),
                    'tradingDay': d.pop('tradingDay')
                }
                self.bar_1minCollection.find_one_and_update(sql, {'$set': d}, upsert=True)

    def updateData(self, df):
        """
        更新数据源中的数据
        :return:
        """
        tradingDay = df.tradingDay[0]
        # symbol = df.symbol[0]
        vtSymbol = df.vtSymbol[0]

        # 先删除这个 tradingDay 中的数据
        data = (
            self.bar_1minCollection.delete_many,
            {
                'filter': {'tradingDay': tradingDay, 'vtSymbol': vtSymbol}
            }
        )
        self.queue.put(data, timeout=10)

        # 重新将数据填充
        data = (
            self.bar_1minCollection.insert_many,
            {
                'documents': df.to_dict('records')
            }
        )
        self.queue.put(data, timeout=10)

    def __run(self):
        """
        :return:
        """
        while self._active:
            while True:
                try:
                    excute, kwargs = self.queue.get(timeout=1)
                except Empty:
                    break
                excute(**kwargs)

    def start(self):
        """

        :return:
        """
        self._active = True
        self._run.start()

    def stop(self):
        """

        :return:
        """
        self._active = False

        if self._run.isAlive():
            self._run.join()

    def initBarCollection(self):
        """

        :return:
        """
        # 需要建立的索引
        indexSymbol = IndexModel([('vtSymbol', ASCENDING)], name='vtSymbol', background=True)
        indexTradingDay = IndexModel([('tradingDay', DESCENDING)], name='tradingDay', background=True)
        indexes = [indexSymbol, indexTradingDay]

        # 初始化日线的 collection
        self._initCollectionIndex(self.bar_1dayCollection, indexes)

    def initContractCollection(self):
        # 需要建立的索引
        indexSymbol = IndexModel([('vtSymbol', ASCENDING)], name='vtSymbol', background=True)
        indexTradingDay = IndexModel([('tradingDay', DESCENDING)], name='tradingDay', background=True)
        indexUnderlyingSymbol = IndexModel([('underlyingSymbol', DESCENDING)], name='underlyingSymbol', background=True)
        indexes = [indexSymbol, indexTradingDay, indexUnderlyingSymbol]

        # 初始化日线的 collection
        self._initCollectionIndex(self.bar_1dayCollection, indexes)

    def _initCollectionIndex(self, col, indexes):
        """
        初始化分钟线的 collection
        :return:
        """

        # 检查索引
        try:
            indexInformation = col.index_information()
            for indexModel in indexes:
                if indexModel.document['name'] not in indexInformation:
                    col.create_indexes(
                        [
                            indexModel,
                        ],
                    )
        except OperationFailure:
            # 有索引
            col.create_indexes(indexes)

    def updateDayData(self, df):
        """
        更新数据源中的日线数据
        :return:
        """
        tradingDay = df.tradingDay[0]
        # symbol = df.symbol[0]
        vtSymbol = df.vtSymbol[0]

        # 先删除这个 tradingDay 中的数据
        self.bar_1dayCollection.delete_many({'tradingDay': tradingDay, 'vtSymbol': vtSymbol})

        # 重新将数据填充
        self.bar_1dayCollection.insert_many(df.to_dict('records'))

    def updateContracts(self, contracts):
        """
        :param contracts: {'vtSymbol': Contract()}
        :return:
        """
        for c in contracts.values():
            try:
                assert isinstance(c, Contract)
            except AssertionError:
                print(c)
            dic = c.toSave()
            filter = {
                'vtSymbol': c.vtSymbol,
            }
            self.contractCollection.find_one_and_update(filter, {'$set': dic}, upsert=True)
