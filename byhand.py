import datetime
import json
import traceback
import os
import logging.config
from threading import Thread, Event
from slavem import Reporter
import time

import arrow
import logging.config
from wash import Washer
from aggregatebar import AggregateBar
from contracter import Contracter
from hiscontract import HisContracter

settingFile = 'conf/kwarg.json'
loggingConfigFile = 'conf/logging.conf'

# serverChanFile = 'conf/serverChan.json'

if __debug__:
    settingFile = 'tmp/kwarg.json'
    loggingConfigFile = 'tmp/logging.conf'

# with open(serverChanFile, 'r') as f:
#     serverChanUrls = json.load(f)['serverChanSlaveUrls']

with open(settingFile, 'r') as f:
    kwargs = json.load(f)

# with open(loggingConfigFile, 'r') as f:
#     loggingConfig = json.load(f)

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


beat = Thread(target=heartBeat, daemon=True)

startDate = arrow.get('2018-04-02 00:00:00+08:00').datetime
endDate = arrow.get('2018-04-04 00:00:00+08:00').datetime

tradingDay = startDate

try:
    while tradingDay <= endDate:
        # 清洗数据
        w = Washer(startDate=tradingDay, **kwargs)
        w.start()

        # 聚合日线数据
        a = AggregateBar(startDate=tradingDay, **kwargs)
        a.start()

        # 更新合约的始末日期
        h = HisContracter(startDate=tradingDay, **kwargs)
        h.start()

        # 生成主力合约数据
        c = Contracter(startDate=tradingDay, **kwargs)
        c.start()

        tradingDay += datetime.timedelta(days=1)
    os.system('say "清洗数据完成"')
except:
    e = traceback.format_exc()
    logger.critical(e)
    time.sleep(3)
    os.system('say "失败，失败，失败"')

finally:
    pass
