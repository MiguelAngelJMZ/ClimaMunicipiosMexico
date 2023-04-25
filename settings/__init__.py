import logging
import logging.config
import sys

import pkg_util as util

default_parameters = {
    '-log': './settings/loggerdefault.ini',
    '-time_to_wait': 1,
    '-debug_log': False
}

log_file = util.get_arg('-log', default=default_parameters.get('-log'))
logging.basicConfig(format="%(asctime)s [pid:%(process)s] %(name)-18s %(levelname)-6s - %(message)s",
                    level=logging.DEBUG,
                    stream=sys.stdout)
__get_config_data: dict = None

def getLogger(loggername: str) -> logging.Logger:
    return logging.getLogger(name=loggername)