import pymongo
import json
import re
import pandas as pd
import configparser


# logging.config.fileConfig(loggingConfigFile)
# logger = logging.getLogger()


class UpdateVtSymbol(object):
    """
    更新合约中的 vtSymbol 字段
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

    def loadContractDF(self):
        """

        :return:
        """
        documents = [d for d in self.col_contract.find({}, {'symbol', 'exchange'})]
        df = pd.DataFrame(documents)
        return df

    def run(self):
        """

        :return:
        """

        # {'_id': ObjectId('5cb6999bc7149402cc3549ae'), 'underlyingSymbol': 'v', 'activeEndDate': None,
        #  'openRatioByMoney': 8.000000000000002e-07, 'optionType': '', 'closeTodayRatioByVolume': 0.0,
        #  'strikePrice': 0.0, 'symbol': 'v1808', 'marginRate': 0.08, 'exchange': 'DCE',
        #  'closeRatioByVolume': 2.0100000000000002, 'activeStartDate': None, 'vtSymbol': 'v1808', 'name': '聚氯乙烯1808',
        #  'closeRatioByMoney': 8.000000000000002e-07, 'gatewayName': 'CTP', 'priceTick': 5.0,
        #  'openRatioByVolume': 2.0100000000000002, 'size': 5, 'closeTodayRatioByMoney': 8.000000000000002e-07,
        #  'endDate': datetime.datetime(2018, 8, 13, 16, 0), 'startDate': datetime.datetime(2017, 8, 21, 0, 0),
        #  'productClass': '期货'}

        # 读取所有合约
        contractDF = self.loadContractDF()

        # 重新生成合约的 vtSymbol
        pattern = re.compile(r'^(\D*)(\d*)$', re.I)

        def rewriteVtSymbol(symbol):
            r = pattern.match(symbol)
            us, year_month = r.groups()
            if len(year_month) == 3:
                # 郑商所，需要修正
                if year_month[0] == '0':
                    # 0 开头是2020年
                    vtSymbol = '{}{}'.format(us, '2' + year_month)
                else:
                    vtSymbol = '{}{}'.format(us, '1' + year_month)
            else:
                vtSymbol = symbol
            return vtSymbol + '.'

        contractDF['vtSymbol'] = contractDF['symbol'].apply(rewriteVtSymbol) + contractDF['exchange']

        # 保存回数据库
        dic = contractDF.set_index('symbol')['vtSymbol'].to_dict()

        for symbol, vtSymbol in dic.items():
            _filter = {'symbol': symbol}
            _count = self.col_contract.find(_filter).count()
            if _count != 0:
                print(f'{symbol} -> {vtSymbol}')
            self.col_contract.update_one(_filter, {'$set': {'vtSymbol': vtSymbol}})


if __name__ == '__main__':
    ucvs = UpdateVtSymbol('./tmp/newVtSymbol.ini')
    ucvs.run()
