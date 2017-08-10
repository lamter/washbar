import pytz
import json
import pymongo
import logging
from bson.codec_options import CodecOptions
import pytz

from pymongo import DESCENDING
import pandas as pd


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
        self.log.info('建立 mongo 链接 {host}.{dbn}.{collection}'.format(**mongoConf))
        db = pymongo.MongoClient(mongoConf['host'], mongoConf['port'])[mongoConf['dbn']]
        db.authenticate(mongoConf['username'], mongoConf['password'])
        c = db[mongoConf['collection']].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=self.mainEngine.LOCAL_TIMEZONE))
        self.collection = c

        # 原始数据
        self.originData = {}  # {symbol: DataFrame()}

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

        cursor = self.collection.find(sql).hint('tradingDay')
        df = pd.DataFrame([i for i in cursor])

        self.log.info('加载了 {} 条数据'.format(df.shape[0]))

        group = df.groupby('symbol').size()
        for symbol in group.index:
            # {symbol: DataFrame()}
            d = df[df.symbol == symbol].sort_values('datetime')
            self.originData[symbol] = d

        self.log.info('加载了 {} 个合约'.format(group.shape[0]))

    def clearBar(self):
        """
        清除多余的 bar ，如果多个 bar 的 volume 没有递增，说明这些 bar 没有新的成交。只保留第一个 bar
        :return:
        """

        count = 0
        for symbol, odf in self.originData.items():
            df = odf[['_id', 'symbol', 'volume']].copy()
            # df = pd.DataFrame(od, columns=['_id', 'datetime', 'symbol', 'volume']).sort('datetime')
            # 去重后的数据
            dunplicatedSeries = df.duplicated(['symbol', 'volume'])

            _ids = df['_id'][dunplicatedSeries]

            if self.isLocal:
                # 对于本地的数据，删除当天所有的数据，重新写入
                for _id in _ids:
                    sql = {
                        '_id': _id,
                        'tradingDay': self.mainEngine.tradingDay
                    }
                    self.collection.delete_one(sql)
                    count += 1

            # 更新数据
            self.originData[symbol] = odf[dunplicatedSeries == False]

        self.log.info('共删除 {} 条数据'.format(count))

    def makeupBar(self, symbol, makupDF):
        """

        :param makupDF:
        :return:
        """
        self.log.warning('本地没有 {} 的数据, 直接补充'.format(symbol))
        df = makupDF.copy()
        del df['_id']
        self.collection.insert_many(df.to_dict('records'))

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

        symbol = ndf['symbol'][0]

        n = insertManyDF.shape[0]
        if n > 0:
            self.log.info('{} 补充了 {} 个 bar'.format(symbol, n))

        # 批量保存的
        if insertManyDF.shape[0] > 0:
            del insertManyDF['_id']
            r = self.collection.insert_many(insertManyDF.to_dict('records'))

        # 逐个更新的
        updateOneDF = upsertDF[upsertDF._id.isnull().apply(lambda x: not x)]

        n = upsertDF.shape[0]
        if n > 0:
            self.log.info('{} 更新到了 {} 个 bar'.format(symbol, 0))
        if updateOneDF.shape[0] > 0:
            documents = updateOneDF.to_dict('records')

            # 存库
            for d in documents:
                sql = {
                    '_id': d.pop('_id'),
                    'tradingDay': d.pop('tradingDay')
                }
                self.collection.find_one_and_update(sql, {'$set': d}, upsert=True)
