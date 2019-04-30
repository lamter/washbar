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

        self.section = 'UpdateCtaVtSymbol'

        # 初始化 MongoDB 链接
        self.client = self.client = pymongo.MongoClient(
            host=self.host,
            port=self.port
        )
        self.db = self.client[self.dbn]
        self.db.authenticate(self.username, self.password)
        self.col_contract = self.db[self.contractColName]

        self.cta_dbn = self.client[self.col_cta_dbn]
        self.col_cta = self.db[self.col_cta_name]
        self.col_orderback = self.db[self.col_orderback_name]
        self.col_pos = self.db[self.col_pos_name]
        self.col_trade = self.db[self.col_trade_name]
    @property
    def col_cta_dbn(self):
        return self.config[self.section]['cta_dbn']

    @property
    def col_cta_name(self):
        return self.config[self.section]['cta']

    @property
    def col_orderback_name(self):
        return self.config[self.section]['orderback']

    @property
    def col_pos_name(self):
        return self.config[self.section]['pos']

    @property
    def col_trade_name(self):
        return self.config[self.section]['trade']

    @property
    def password(self):
        return self.config[self.section]['password']

    @property
    def host(self):
        return self.config[self.section]['host']

    @property
    def port(self):
        return self.config[self.section].getint('port')

    @property
    def dbn(self):
        return self.config[self.section]['dbn']

    @property
    def contractColName(self):
        return self.config[self.section]['collection']

    @property
    def username(self):
        return self.config[self.section]['username']

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
            print(f'{symbol} -> {vtSymbol}')
            print(self.col_cta.find({'symbol': symbol}))
            self.col_cta.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            self.col_orderback.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            self.col_pos.update_many({'vtSymbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            self.col_trade.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})


if __name__ == '__main__':
    ucvs = UpdateBarVtSymbol('./tmp/newVtSymbol.ini')
    ucvs.run()
