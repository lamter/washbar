import datetime
import json
import traceback
import arrow

from aggregatebar import AggregateBar

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
    # 聚合日线数据
    # 回溯聚合日线,从最新的一天开始
    startDate = arrow.now().datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    endDate = arrow.get('2017-08-01 00:00:00+08:00').datetime
    currentTradingDay = startDate
    while currentTradingDay >= endDate:
        a = AggregateBar(loggingConfig=loggingConfig, **kwargs)
        a.tradingDay = currentTradingDay
        a.start()
        currentTradingDay -= datetime.timedelta(days=1)

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
