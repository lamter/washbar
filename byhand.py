import datetime
import json
import traceback

import arrow
import logging.config
from wash import Washer
from aggregatebar import AggregateBar
from contracter import Contracter
from hiscontract import HisContracter

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

logging.loaded = False

try:
    startDate = arrow.get('2011-01-01 00:00:00+08:00').datetime
    endDate = arrow.get('2017-10-31 00:00:00+08:00').datetime
    tradingDay = startDate
    while tradingDay <= endDate:
        # 清洗数据
        # w = Washer(loggingConfig=loggingConfig, **kwargs)
        # w.tradingDay = tradingDay
        # w.start()

        # 聚合日线数据
        a = AggregateBar(loggingConfig=loggingConfig, **kwargs)
        a.tradingDay = tradingDay
        a.start()

        # 更新合约的始末日期
        h = HisContracter(loggingConfig=loggingConfig, startDate=tradingDay, **kwargs)
        h.start()

        # 生成主力合约数据
        c = Contracter(loggingConfig=loggingConfig, startDate=tradingDay, **kwargs)
        c.start()

        tradingDay += datetime.timedelta(days=1)

except:
    e = traceback.format_exc()
    print(e)

    if __debug__:
        exit()

    e.replace('\n', '\n\n')
    import requests
    import time

    for url in serverChanUrls.values():
        serverChanUrl = requests.get(url).text
        text = 'washbar - {} - 数据清洗异常'.format(kwargs['mongoConf']['mongoLocal']['host'])
        url = serverChanUrl.format(serverChanUrl, text=text, desp=e)
        while True:
            r = requests.get(url)
            if r.status_code == 200:
                break
            else:
                time.sleep(60)
    raise
