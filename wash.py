import sys
import datetime
import logging
import logging.config
import json
import pytz
from bson.codec_options import CodecOptions

import pymongo
from pymongo.errors import ServerSelectionTimeoutError
import tradingtime as tt
import pandas as pd


class Washer(object):
    """

    """

    LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')

    def __init__(self, mongoConf, loggingConfig=None):
        self.mongoConf = mongoConf  # [{conf}, {conf}]
        self.mongoCollections = []  # 是 collections, 不是 client, db

        self.originDatas = []  # [originDatas]

        self.log = logging.getLogger('main')

        # 设定当前要处理的交易日
        self.isTradingDay, self.tradingDay = tt.get_tradingday(
            datetime.datetime.now().replace(hour=8, minute=0, second=0, microsecond=0))

        self.initLog(loggingConfig)

        self.initMongo()

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
            self.log = logging.getLogger('main')

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

    def initMongo(self):
        """

        :return:
        """



        self.mongoCollections.clear()
        for conf in self.mongoConf:
            self.log.info('建立 mongo 链接 {}'.format(json.dumps(conf, indent=1)))
            db = pymongo.MongoClient(conf['host'], conf['port'])[conf['dbn']]
            db.authenticate(conf['username'], conf['password'])
            c = db[conf['collection']].with_options(codec_options=CodecOptions(tz_aware=True, tzinfo=self.LOCAL_TIMEZONE))
            self.mongoCollections.append(c)

    def start(self):
        """

        :return:
        """

        # TODO 清除多余的 bar
        self.loadOriginData()
        self.clearBar()

        # TODO 多个 bar 之间的数据

    def loadOriginData(self):
        """
        加载原始数据
        :return:
        """

        self.originDatas.clear()
        for collection in self.mongoCollections:
            assert isinstance(collection, pymongo.collection.Collection)
            sql = {'tradingDay': self.tradingDay}
            cursor = collection.find(sql)
            self.originDatas.append((i for i in cursor))

    def clearBar(self):
        """
        一个 bar 只能有一个，并且清除多余的 bar
        :return:
        """
        for od in self.originDatas:
            df = pd.DataFrame(od)

