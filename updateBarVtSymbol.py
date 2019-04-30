import pymongo
import json
import re
import pandas as pd
import configparser
import arrow


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
        self.col_1min = self.db[CTP_mongo['bar_1min']]
        self.col_1day = self.db[CTP_mongo['bar_1day']]

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
        b = arrow.now().datetime
        for dic in symbol2vtSymbol:
            symbol, vtSymbol = dic['symbol'], dic['vtSymbol']
            self.col_1min.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            count += 1
            e = arrow.now().datetime
            costSecs = (e - b).total_seconds()
            finished = count / amount
            needSecs = costSecs / finished * (amount - count) / count
            print(f'1min {round(finished * 100, 2)} % need {round(needSecs / 60, 1)} min')

        # bar_1day
        count = 0
        b = arrow.now().datetime
        for dic in symbol2vtSymbol:
            symbol, vtSymbol = dic['symbol'], dic['vtSymbol']
            # print(f'{symbol} -> {vtSymbol}')
            self.col_1day.update_many({'symbol': symbol}, {'$set': {'vtSymbol': vtSymbol}})
            count += 1
            e = arrow.now().datetime
            costSecs = (e - b).total_seconds()
            finished = count / amount
            needSecs = costSecs / finished * (amount - count) / count
            print(f'1day {round(finished * 100, 2)} % need {round(needSecs / 60, 1)} min')

if __name__ == '__main__':
    ucvs = UpdateBarVtSymbol('./tmp/newVtSymbol.ini')
    ucvs.run()
