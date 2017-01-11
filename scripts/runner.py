#!/usr/local/bin/python2.7
# encoding: utf-8
"""
scripts.runner -- main script to execute DampeTaskRunner
@author:     S. Zimmer
@change:     2017-01-09:    initial import
             2017-01-11:    first complete implementation
@fixme:      force does nothing so far.
"""

import sys
from os import remove
from argparse import ArgumentParser
from base.core import RecoRunner, parse_config
from base.utils import ProcessResourceMonitor, sleep, touch, isfile
from multiprocessing import Process
from psutil import Process as PsProcess
import logging
from base.logger import initLogger


def main(argv=None):
    def run(cfg, log, pidfile):
        reco = RecoRunner(config=cfg)
        proc = Process(target=reco.execute)
        proc.start()
        log.info("started RecoRunner.")
        ps = PsProcess(proc.pid)
        prm = ProcessResourceMonitor(ps)
        while proc.is_alive():
            log.info(prm.queryResources())
            if not isfile(pidfile):
                log.warning("PID file removed; requested shutdown.")
                proc.terminate()
            else:
                sleep(600.)  # check every 5 minutes

        log.info("Execution complete")

    parser = ArgumentParser(description="main script to execute DAMPE Task Runner")
    parser.add_argument("-c","--config",dest='cfg',default=None,help='name of config.yaml file')
    parser.add_argument("-f","--force",action='store_true',default=False, help='re-execute runner even if running.')
    parser.add_argument("-D","--daemon",action='store_true',default=False, help='run in daemon mode')
    args = parser.parse_args()
    cfg = parse_config(args.cfg)

    logfile = cfg["global"].get("logfile","/tmp/test.log")
    loglevel= cfg["global"].get("loglevel","INFO")
    pidfile = cfg['global'].get("pidfile", "/tmp/runner.pid")
    touch(pidfile)

    parent = child = "DEBUG"
    if isinstance(loglevel,list):
        if len(loglevel)>1:
            parent, child = loglevel[0],loglevel[1]
        else:
            loglevel = loglevel[0]
    if isinstance(loglevel,str): parent = child = loglevel
    else: raise Exception("could not interpret log level.")
    initLogger(logfile, level=parent, childlevel=child)
    log = logging.getLogger("core")

    if not args.daemon:
        run(args.cfg, log, pidfile)
        remove(pidfile)
    else:
        log.info("running in daemon mode, will only terminate if pid file is removed")
        while isfile(pidfile):
            run(args.cfg,log, pidfile)
        log.info("PID file removed, shutting down daemon.")

if __name__ == "__main__":
    sys.exit(main())
