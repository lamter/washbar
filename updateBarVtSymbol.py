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

        self.section  ='UpdateBarVtSymbol'
        # 初始化 MongoDB 链接
        self.client = self.client = pymongo.MongoClient(
            host=self.host,
            port=self.port
        )

        self.db = self.client[self.dbn]
        self.db.authenticate(self.username, self.password)
        self.col_contract = self.db[self.contractColName]
        self.col_1min = self.db[self.col_1min_name]
        self.col_1day = self.db[self.col_1day_name]

    @property
    def col_1min_name(self):
        return self.config[self.section]['bar_1min']

    @property
    def col_1day_name(self):
        return self.config[self.section]['bar_1day']

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

        # bar_1min
        amount = len(symbol2vtSymbol)
        count = 0
        for dic in symbol2vtSymbol:
            symbol, vtSymbol = dic['symbol'], dic['vtSymbol']
            self.col_1min.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            count += 1
            print(f'1min {round(count/amount * 100, 2) } %')

        # bar_1day
        count = 0
        for dic in symbol2vtSymbol:
            symbol, vtSymbol = dic['symbol'], dic['vtSymbol']
            # print(f'{symbol} -> {vtSymbol}')
            self.col_1day.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            count += 1
            print(f'1day {round(count/amount * 100, 2) } %')

if __name__ == '__main__':
    ucvs = UpdateBarVtSymbol('./tmp/newVtSymbol.ini')
    ucvs.run()
