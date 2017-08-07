import sys
import json
import logging
import logging.config

import pymongo
from pymongo.errors import ServerSelectionTimeoutError


class SansanLoad(object):
    def __init__(self):
        self.saveFile = 'tmp/sansan/sansan.json'

        self.log = logging.getLogger('main')
        with open('tmp/sansan/logconfig.json', 'r') as f:
            self.initLog(json.load(f))

        self.init()

    def init(self):
        """

        :return:
        """
        self.log.info('123')

    def initLog(self, loggingconf):
        """
        初始化日志
        :param loggingconf:
        :return:
        """
        if loggingconf:
            # log4mongo 的bug导致使用非admin用户时，建立会报错。
            # 这里使用注入的方式跳过会报错的代码
            import log4mongo.handlers
            log4mongo.handlers._connection = pymongo.MongoClient(
                host=loggingconf['handlers']['mongo']['host'],
                port=loggingconf['handlers']['mongo']['port'],
            )

            try:
                logging.config.dictConfig(loggingconf)
            except ServerSelectionTimeoutError:
                print(u'Mongohandler 初始化失败，检查 MongoDB 否正常')
                raise
            self.log = logging.getLogger('main')

        else:
            self.log = logging.getLogger()
            self.log.setLevel('DEBUG')
            fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
            # datefmt = "%a-%d-%b %Y %H:%M:%S"
            datefmt = None
            formatter = logging.Formatter(fmt, datefmt)
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(formatter)
            sh.setLevel('DEBUG')
            self.log.addHandler(sh)

            sh = logging.StreamHandler(sys.stderr)
            sh.setFormatter(formatter)
            sh.setLevel('WARN')
            self.log.addHandler(sh)
            self.log.warning(u'未配置 loggingconfig')

    def start(self):
        """

        :return:
        """


if __name__ == '__main__':
    ss = SansanLoad()
    # ss.start()