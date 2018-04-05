import logging

import tradingtime as tt


class BaseObject(object):
    def __init__(self):
        pass


class Contract(BaseObject):
    """
    合约详情对象
    """

    def __init__(self, originData):
        super(Contract, self).__init__()
        self.originData = originData
        assert isinstance(originData, dict)

        # 需要保存到数据库 ================
        self.symbol = ''  # 合约名
        self.startDate = None  # 日线数据起始交易日
        self.endDate = None  # 日线数据中止交易日
        self.activeStartDate = None
        self.activeEndDate = None
        for k, v in originData.items():
            setattr(self, k, v)

        # 品种名
        self.underlyingSymbol = tt.contract2name(self.symbol)
        # 需要保存到数据库 ================

    def updateDate(self, starDate, endDate):
        # 起始日，要更小的日期
        self.startDate = min(starDate, self.startDate) if self.startDate else starDate

        # 起始日，要更大的日期
        self.endDate = max(endDate, self.endDate) if self.endDate else endDate

    def updateActiveStartDate(self, tradingDay):
        if self.activeStartDate is None:
            self.activeStartDate = tradingDay
        else:
            self.activeStartDate = min(self.activeStartDate, tradingDay)

    def updateActiveEndDate(self, tradingDay):
        if self.activeEndDate is None:
            self.activeEndDate = tradingDay
        else:
            self.activeEndDate = max(self.activeEndDate, tradingDay)

    def __str__(self):
        s = super(Contract, self).__str__()
        s = s[:-1]
        s += ' {} '.format(self.symbol)
        if self.startDate:
            s += '{} '.format(self.startDate.strftime('%Y-%m-%d'))
        else:
            s += '{} '.format(self.startDate)

        if self.endDate:
            s += '{} '.format(self.endDate.strftime('%Y-%m-%d'))
        else:
            s += '{} '.format(self.endDate)

        if self.activeStartDate:
            s += '{} '.format(self.activeStartDate.strftime('%Y-%m-%d'))
        else:
            s += '{} '.format(self.activeStartDate)

        if self.activeEndDate:
            s += '{} '.format(self.activeEndDate.strftime('%Y-%m-%d'))
        else:
            s += '{} '.format(self.activeEndDate)

        return s + '>'

    def toSave(self):
        dic = self.__dict__.copy()
        dic.pop('originData')
        try:
            dic.pop('_id')
        except KeyError:
            pass
        return dic
