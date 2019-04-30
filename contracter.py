import arrow
import json
from wash import Washer
import datetime
import traceback


class Contracter(Washer):
    """
    处理合约信息
    """

    def __init__(self, startDate=None, *args, **kwargs):
        super(Contracter, self).__init__(*args, **kwargs)
        # 开始从哪个交易日开始回溯数据
        self._inited = False
        self.tradingDay = startDate or self.tradingDay
        self.contractListByUnderlyingSymbol = {}  # {'underlyingSymbol': [Contract(), ...]}
        # 今天品种的主力合约
        self.activeContractDic = {}  # {'underlyingSymbol': Contract()}
        self.drData = None
        self.activeContractChangeDic = {}  # {oldActiveContract: newActiveContract}

    def init(self):
        """

        :return:
        """
        self.log.info('处理合约信息 {}'.format(self.tradingDay))
        self.loadOriginDailyDataByDate(self.tradingDay)
        self.loadContractData()
        self.drData = self.drDataLocal
        self._inited = True

    def start(self):
        """

        :return:
        """
        self.init()
        if not self._inited:
            raise ValueError(u'尚未初始化')

        self.run()

    def run(self):

        if self.drData.originDailyDataByDate.shape[0] == 0:
            self.log.info(u'{} 交易日没有任何数据'.format(self.tradingDay))
            return

        # 将合约按照品种分类
        self.sortByUnderlyingSymbol()

        # 判断当前主力合约
        for us in self.contractListByUnderlyingSymbol.keys():
            self.findOldActiveContract(us)

        # 从日线数据找出主力合约
        for us in self.contractListByUnderlyingSymbol.keys():
            self.findNewActiveContract(us)

        # 保存合约数据
        self.saveActiveContraact()

        # 汇报新旧主力合约变化
        self.reportNewActiveContract()

    def sortByUnderlyingSymbol(self):
        """
        将合约按照品种分类
        :return:
        """
        for c in self.drData.contractData.values():
            us = c.underlyingSymbol
            try:
                contractList = self.contractListByUnderlyingSymbol[us]
            except KeyError:
                self.contractListByUnderlyingSymbol[us] = contractList = []

            # 放入同品种的队列
            contractList.append(c)

    def findOldActiveContract(self, unserlyingSymbol):
        us = unserlyingSymbol
        contractList = self.contractListByUnderlyingSymbol.get(us)

        if not contractList:
            return

        activeContract = None

        for c in contractList:
            if c.activeEndDate:
                # 曾经是活跃合约
                if activeContract is None:
                    # 直接替换
                    activeContract = c
                else:
                    # 判断哪个是更新的主力合约
                    if c.activeEndDate > activeContract.activeEndDate:
                        activeContract = c

        if activeContract:
            self.activeContractDic[us] = activeContract

    def loadOriginDailyDataByDate(self, tradingDay):
        """

        :param date:
        :return:
        """
        self.drDataLocal.loadOriginDailyDataByDate(tradingDay)

    def findNewActiveContract(self, underlyingSymbol):
        """
        根据当天日线数据找出主力合约
        :param underlyingSymbol:
        :return:
        """
        us = underlyingSymbol

        df = self.drData.originDailyDataByDate
        df = df[df.underlyingSymbol == us]

        if df.shape[0] == 0:
            return

        oldActiveContract = activeContract = self.activeContractDic.get(us)
        if oldActiveContract is None:
            self.log.warning(u'{} 没有主力合约'.format(us))

        contractDic = self.drData.contractData

        maxVolumeDf = df[df.volume == df.volume.max()]
        for vtSymbol in maxVolumeDf.vtSymbol:
            c = contractDic[vtSymbol]
            if activeContract is None:
                # 尚未有任何主力合约
                activeContract = c
            else:
                if c.vtSymbol > activeContract.vtSymbol:
                    # 新的 vtSymbol 字段，可以通过直接对比来排序
                    log = '{} {}'.format(activeContract.vtSymbol, activeContract.startDate)
                    self.log.info(log)
                    activeContract = c
                # if c.startDate > activeContract.startDate:
                #     # 汇报新旧主力合约替换
                #     log = u'startDate {} {} '.format(c.vtSymbol, c.startDate)
                #     log += u'{} {}'.format(activeContract.vtSymbol, activeContract.startDate)
                #     self.log.info(log)
                #     activeContract = c
                # elif c.startDate == activeContract.startDate:
                #     # 已经结束了的合约
                #     if c.endDate > activeContract.endDate:
                #         log = u'endDate {} {} '.format(c.vtSymbol, c.endDate)
                #         log += u'{} {}'.format(activeContract.vtSymbol, activeContract.endDate)
                #         self.log.info(log)
                #         activeContract = c
                # else:  # c.startDate > activeContract.startDate
                #     pass

        if oldActiveContract != activeContract:
            # 主力合约出现变化，汇报
            if oldActiveContract is None:
                self.log.info(u'{} 没有主力合约 {} '.format(us, str(oldActiveContract), activeContract))
            else:
                self.activeContractChangeDic[oldActiveContract] = activeContract

        if activeContract:
            # 更新主力合约的始末日期
            activeContract.updateActiveEndDate(self.tradingDay)
            activeContract.updateActiveStartDate(self.tradingDay)
            self.activeContractDic[us] = activeContract

    def saveActiveContraact(self):
        """
        更新主力合约
        :return:
        """
        contracts = {c.vtSymbol: c for c in self.activeContractDic.values()}
        self.drData.updateContracts(contracts)

        self.drDataRemote.updateContracts(contracts)

    def reportNewActiveContract(self):
        """
        当主力合约出现更换时，进行邮件汇报
        :return:
        """
        if not self.activeContractChangeDic:
            # 没有变化，不需要汇报
            self.log.info('没有主力合约变更')
            return
        self._reportNewActiveContract()

    def _reportNewActiveContract(self):

        # 组织汇报内容
        text = '主力合约变动\n'

        if self.drDataLocal.originDailyDataByDate is None:
            self.drDataLocal.loadOriginDailyDataByDate(self.tradingDay)

        # DataFrame
        odd = self.drDataLocal.originDailyDataByDate.copy()
        odd.set_index('vtSymbol', inplace=True)

        for o, n in self.activeContractChangeDic.items():
            # 新旧合约变化
            try:
                try:
                    oldVolume = odd.loc[o.vtSymbol, 'volume']
                except KeyError:
                    oldVolume = 0
                dic = {
                    'old': o.vtSymbol if o else None,
                    'new': n.vtSymbol,
                    'oldVolume': oldVolume,
                    'newVolume': odd.loc[n.vtSymbol, 'volume'],
                }

                text += '{old} vol:{oldVolume} -> {new} vol:{newVolume} \n'.format(**dic)
            except Exception:
                err = traceback.format_exc()
                self.log.error(err)

        self.log.warning(text)


if __name__ == '__main__':
    settingFile = 'conf/kwarg.json'
    loggingConfigFile = 'conf/logconfig.json'
    serverChanFile = 'conf/serverChan.json'

    if __debug__:
        settingFile = 'tmp/kwarg.json'
        loggingConfigFile = 'tmp/logconfig.json'

    with open(serverChanFile, 'r') as f:
        serverChanUrls = json.load(f)['serverChanSlaveUrls']

    with open(settingFile, 'r') as f:
        kwargs = json.load(f)

    with open(loggingConfigFile, 'r') as f:
        loggingConfig = json.load(f)

    import logging

    logging.loaded = False

    startDate = None
    startDate = arrow.get('2014-03-24 00:00:00+08:00').datetime
    endDate = arrow.get('2017-10-27 00:00:00+08:00').datetime

    tradingDay = startDate
    while tradingDay <= endDate:
        c = Contracter(tradingDay, loggingConfig=loggingConfig, **kwargs)
        c.start()
        tradingDay += datetime.timedelta(days=1)
