import json
import time
import traceback
import logging.config
from threading import Thread, Event
import arrow
import datetime

from slavem import Reporter

from wash import Washer
from aggregatebar import AggregateBar
from contracter import Contracter
from hiscontract import HisContracter

settingFile = 'conf/kwarg.json'
# loggingConfigFile = 'conf/logconfig.json'
loggingConfigFile = 'conf/logging.conf'
serverChanFile = 'conf/serverChan.json'

if __debug__:
    settingFile = 'tmp/kwarg.json'
    loggingConfigFile = 'tmp/logging.conf'

with open(serverChanFile, 'r') as f:
    serverChanUrls = json.load(f)['serverChanSlaveUrls']

with open(settingFile, 'r') as f:
    kwargs = json.load(f)

# 加载日志模块
logging.config.fileConfig(loggingConfigFile)
logger = logging.getLogger()

# 汇报
stopped = Event()
slavemReport = Reporter(**kwargs.pop('slavemConf'))


# 启动心跳进程
def heartBeat():
    while not stopped.wait(30):
        slavemReport.heartBeat()

    slavemReport.endHeartBeat()

beat = Thread(target=heartBeat, daemon=True)

try:
    # 启动汇报
    slavemReport.lanuchReport()
    slavemReport.isActive = True

    beat.start()

    # 清洗数据
    w = Washer(**kwargs)
    w.start()

    # 聚合日线数据
    a = AggregateBar(**kwargs)
    a.start()

    # 更新合约的始末日期
    h = HisContracter(**kwargs)
    h.start()

    # 生成主力合约数据
    c = Contracter(**kwargs)
    c.start()

except:
    e = traceback.format_exc()
    logger.critical(e)
    time.sleep(3)

finally:
    # 关闭心跳
    stopped.set()
    # 心跳超过2小时
    beat.join(60 * 60 * 2)
    if beat.isAlive():
        # 运行超时了
        logger.warning('心跳持续时间超时')
    time.sleep(3)
