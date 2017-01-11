"""
Created on January 11, 2017

@author: zimmer
"""

import logging
from logging import Formatter, StreamHandler
from logging.handlers import RotatingFileHandler

#'utils': {
#    'class': 'logging.StreamHandler',
#    'level': "DEBUG",

def initLogger(logfile,level="INFO",childlevel="INFO"):
    log_levels = dict(CRITICAL=0, ERROR=1, WARNING=2, INFO=3, DEBUG=4)
    #log_levels_rev = {v: k for k, v in log_levels.iteritems()}
    #childlevel = log_levels_rev[log_levels[level]+1] if log_levels[level]<=3 else level

    handler_file = RotatingFileHandler(maxBytes=2000000, filename=logfile, backupCount=5)
    handler_console = StreamHandler()

    form = Formatter("[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s", '%Y-%m-%d %H:%M:%S')
    handler_file.setFormatter(form)
    handler_console.setFormatter(form)

    for logger in ['batch','RecoRunner','utils']:
        log = logging.getLogger(logger)
        if logger == 'RecoRunner':
            log.setLevel(level)
        else:
            log.setLevel(childlevel)
        log.addHandler(handler_file)

    log = logging.getLogger("core")
    log.setLevel(level)
    log.addHandler(handler_console)



def initLogger_v2(logfile,level="INFO"):
    from logging.config import dictConfig
    log_levels = dict(CRITICAL=0, ERROR=1, WARNING=2, INFO=3, DEBUG=4)
    log_levels_rev = {v: k for k, v in log_levels.iteritems()}

    # add logger
    file_loggers = {'formatter': "precise",
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': logfile,
                    'maxBytes': "2000000",
                    'backupCount': 5}

    childlevel = log_levels_rev[log_levels[level]+1] if log_levels[level]<=3 else level
    cfg = {'version': 1,'disable_existing_loggers': False}
    cfg['formatters']= {"precise": {
                "format": "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                'datefmt': '%Y-%m-%d %H:%M:%S'}}
    cfg['handlers']={
            'RecoRunner': dict(level=level,**file_loggers),
            'utils': dict(level=level, **file_loggers),
            'batch': dict(level=level, **file_loggers),
            'core': {
                'class': 'logging.StreamHandler',
                'level': level
            }
        }
    dictConfig(cfg)
