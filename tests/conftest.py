import sys
import pytest
import pickle
import json
import logging
import pandas as pd
import arrow
from wash import Washer
from contracter import Contracter



@pytest.fixture(scope='session', autouse=True)
def kwargs():
    settingFile = '../tmp/kwarg.json'
    with open(settingFile, 'r') as f:
        dic = json.load(f)
    return dic


# @pytest.fixture(scope='session', autouse=True)
# def loggingConfig():
#     loggingConfigFile = '../tmp/logging.conf'
#     with open(loggingConfigFile, 'r') as f:
#         dic = json.load(f)
#     return dic

@pytest.fixture(scope='session', autouse=True)
def tradingDay():
    return arrow.get('2018-03-21 00:00:00+08').datetime


@pytest.fixture(scope='session', autouse=True)
def contract20180321():
    with open('data/20180321合约.pickle', 'rb') as f:
        documents = pickle.load(f)
    return documents

@pytest.fixture(scope='session', autouse=True)
def contract20180321_df(contract20180321):
    return pd.DataFrame(contract20180321)

@pytest.fixture(scope='session', autouse=True)
def daily20180321():
    with open('data/20180321日线.pickle', 'rb') as f:
        documents = pickle.load(f)
    return documents

@pytest.fixture(scope='session', autouse=True)
def daily20180321_df(daily20180321):
    return pd.DataFrame(daily20180321)

@pytest.fixture(scope='session', autouse=True)
def logger():
    logger = logging.getLogger('root')
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.DEBUG)
    logger.addHandler(sh)
    return logger

@pytest.fixture(scope='session', autouse=True)
def washer(kwargs):
    return Washer(**kwargs)


@pytest.fixture
def contracter_(kwargs):
    c = Contracter(**kwargs)
    c.tradingDay = tradingDay
    return c
