import datetime
import json
import traceback

import logging.config
from wash import Washer
from aggregatebar import AggregateBar
from contracter import Contracter
from hiscontract import HisContracter

logging.loaded = False

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

# with open(loggingConfigFile, 'r') as f:
#     loggingConfig = json.load(f)


try:
    # 清洗数据
    w = Washer(loggingConfigFile=loggingConfigFile, **kwargs)
    w.start()

    # 聚合日线数据
    a = AggregateBar(loggingConfigFile=loggingConfigFile, **kwargs)
    a.start()

    # 更新合约的始末日期
    h = HisContracter(loggingConfigFile=loggingConfigFile, **kwargs)
    h.start()

    # 生成主力合约数据
    c = Contracter(loggingConfigFile=loggingConfigFile, **kwargs)
    c.start()

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