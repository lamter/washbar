import datetime
import json
import traceback

from wash import Washer
from aggregatebar import AggregateBar
from contracter import Contracter

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

try:
    # 清洗数据
    w = Washer(loggingConfig=loggingConfig, **kwargs)
    w.start()

    # 聚合日线数据
    a = AggregateBar(loggingConfig=loggingConfig, **kwargs)
    a.start()

    # 补充合约数据
    c = Contracter(loggingConfig=loggingConfig, **kwargs)
    c.startTradingDay -= datetime.timedelta(days=1)
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