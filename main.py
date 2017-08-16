import json
from wash import Washer
import traceback

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
    w = Washer(loggingConfig=loggingConfig, **kwargs)
    w.start()
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
