import json
from wash import Washer

settingFile = 'conf/kwarg.json'
loggingConfigFile = 'conf/logconfig.json'
if __debug__:
    settingFile = 'tmp/kwarg.json'
    loggingConfigFile = 'tmp/logconfig.json'


with open(settingFile, 'r') as f:
    kwargs = json.load(f)

with open(loggingConfigFile, 'r') as f:
    loggingConfig = json.load(f)

w = Washer(loggingConfig=loggingConfig, **kwargs)
w.start()
