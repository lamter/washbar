import pymongo
import json
import re
import pandas as pd
import configparser
import arrow
import datetime
from collections import OrderedDict
import tradingtime as tt


class RefreshActiveContract(object):
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
        # self.col_1min = self.db[CTP_mongo['bar_1min']]
        self.col_1day = self.db[CTP_mongo['bar_1day']]

        self.activeContractByUs = {}  # {'us': ActiveContract()}

    # def loadContract(self):
    #     """
    #
    #     :return:
    #     """
    #
    #     return [d for d in self.col_contract.find({}, {'vtSymbol': 1, 'underlyingSymbol': 1, '_id': 0})]

    def findActiveContract(self):
        today = arrow.get('2010-01-01 00:00:00+08').datetime
        # endDate = arrow.get('2013-02-01 00:00:00+08').datetime
        endDate = arrow.now().datetime

        while today < endDate:
            # 查询一整天的数据
            today += datetime.timedelta(days=1)
            print(today)
            cursor = self.col_1day.find({'tradingDay': today}, {'volume': 1, 'vtSymbol': 1, 'symbol': 1, '_id': 0})
            # 按照品种分类
            df = pd.DataFrame([d for d in cursor])
            if df.shape[0] == 0:
                continue

            df = df.dropna(how='any')

            df['us'] = df['vtSymbol'].apply(tt.contract2name)
            _maxVolume = df.groupby('us').apply(lambda t: t[t.volume == t.volume.max()])
            for vtSymbol in _maxVolume.vtSymbol:
                us = tt.contract2name(vtSymbol)
                try:
                    activeContract = self.activeContractByUs[us]
                except KeyError:
                    activeContract = self.activeContractByUs[us] = ActiveContract(us)

                # 尚未存在主力合约
                if not activeContract.active:
                    # 第一个主力合约
                    activeContract.active.append({'vtSymbol': vtSymbol,
                                                  'activeStartDate': today,
                                                  'activeEndDate': today})
                    continue

                # 跟已有的主力合约对比
                old = activeContract.active[-1]
                if vtSymbol > old['vtSymbol']:
                    # 新的主力合约
                    activeContract.active.append({'vtSymbol': vtSymbol,
                                                  'activeStartDate': today,
                                                  'activeEndDate': today})
                else:
                    # 没有更换主力，更新 activeEndDate 即可
                    old['activeEndDate'] = today

    def run(self):
        # 删除所有主力合约的字段
        self.col_contract.update_many({}, {'$unset': {'activeStartDate': 0, 'activeEndDate': 0}})

        # 获得历史所有的主力合约
        self.findActiveContract()

        # 更新合约主力时间段
        for activeContract in self.activeContractByUs.values():
            for dic in activeContract.active:
                _filter = {'vtSymbol': dic['vtSymbol']}
                self.col_contract.update_one(_filter, {
                    '$set': {'activeEndDate': dic['activeEndDate'], 'activeStartDate': dic['activeStartDate']}})


class ActiveContract(object):
    """
    指定品种对象
    """

    def __init__(self, us):
        self.us = us
        # self.contracts = OrderedDict() # 合约按照时间排序
        self.active = []  # 合约按照时间排序 {'vtSymbol': ['activeStartDate', 'activeEndDate]}

    def __str__(self):
        f = lambda d: d if isinstance(d, str) else str(d.date())
        return f'<us:{self.us} {" ".join([f(d) for d in self.active[-1].values()]) if self.active else ""}' + super(
            ActiveContract, self).__str__()[
                                                                                                              1:]


if __name__ == '__main__':
    rac = RefreshActiveContract('./tmp/newVtSymbol.ini')
    rac.run()
