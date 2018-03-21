"""
主力合约接口
"""
import arrow
import datetime


def test_repportActiveContractChange(contracter_, contract20180321, daily20180321):
    """
    汇报主力合约变动
    :return:
    """
    from drdata import DRData
    assert isinstance(contracter_.drDataLocal, DRData)

    # 加载合约数据
    contracter_.drDataLocal._loadContractData(contract20180321)

    oldContract = contracter_.drDataLocal.contractData['rb1805']
    newContract = contracter_.drDataLocal.contractData['rb1810']

    # # 加载当日数据
    contracter_.drDataLocal._loadOriginDailyDataByDate(daily20180321)

    # 设置新旧主力合约更换
    contracter_.activeContractChangeDic[oldContract] = newContract

    # 调试接口
    contracter_._reportNewActiveContract()


def test_activeCntractChange():
    """

    :return:
    """