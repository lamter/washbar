import pymongo
import json
import re
import pandas as pd
import configparser


# logging.config.fileConfig(loggingConfigFile)
# logger = logging.getLogger()


class UpdateBarVtSymbol(object):
    """
    更新 bar 中的 vtSymbol 字段
    """

    def __init__(self, configfile):
        self.config = configparser.ConfigParser()
        with open(configfile, 'r') as f:
            self.config.read_file(f)

        CTP_mongo = self.config['CTP_mongo']

        # 初始化 MongoDB 链接
        client = pymongo.MongoClient(
            host=CTP_mongo['host'],
            port=CTP_mongo.getint('port'),
        )

        self.db = client[CTP_mongo['dbn']]
        self.db.authenticate(CTP_mongo['username'], CTP_mongo['password'])
        self.col_contract = self.db[CTP_mongo['contract']]

        CTA_mongo = self.config['CTA_mongo']
        # 初始化 MongoDB 链接
        client = pymongo.MongoClient(
            host=CTA_mongo['host'],
            port=CTA_mongo.getint('port'),
        )
        self.db_cta = client[CTA_mongo['dbn']]
        self.db_cta.authenticate(CTP_mongo['username'], CTP_mongo['password'])

        self.col_cta = self.db_cta[CTA_mongo['cta']]
        self.col_orderback = self.db_cta[CTA_mongo['orderback']]
        self.col_pos = self.db_cta[CTA_mongo['pos']]
        self.col_trade = self.db_cta[CTA_mongo['trade']]

    def loadSymbol2vtSymbol(self):
        """

        :return:
        """

        return [d for d in self.col_contract.find({}, {'symbol': 1, 'vtSymbol': 1, '_id': 0})]

    def run(self):
        """

        :return:
        """

        # 读取所有 [{'symbol':'ZC909', 'vtSymbol':'ZC1909.CZCE'}]
        symbol2vtSymbol = self.loadSymbol2vtSymbol()

        for dic in symbol2vtSymbol:
            symbol, vtSymbol = dic['symbol'], dic['vtSymbol']
            _count = self.col_cta.find({'symbol': symbol}).count()
            if _count != 0:
                print(f'{symbol} -> {vtSymbol}')
            self.col_cta.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            self.col_orderback.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            self.col_pos.update_many({'vtSymbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            self.col_trade.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})


if __name__ == '__main__':
    ucvs = UpdateBarVtSymbol('./tmp/newVtSymbol.ini')
    ucvs.run()
