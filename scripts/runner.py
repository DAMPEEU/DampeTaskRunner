#!/usr/local/bin/python2.7
# encoding: utf-8
'''
scripts.runner -- main script to execute DampeTaskRunner
@author:     S. Zimmer
@change:     2017-01-09:    initial import
'''

import sys
from argparse import ArgumentParser
from base.core import RecoRunner


def main(argv=None): 
    parser = ArgumentParser(description="main script to execute DAMPE Task Runner")
    parser.add_argument("-c","--config",dest='cfg',default=None,help='name of config.yaml file')
    parser.add_argument("-f","--force",action='store_true',default=False, help='re-execute runner even if not running.')
    args = parser.parse_args()
    reco = RecoRunner()
    proc = Process(target=reco.execute)
    proc.start()
    ps = ps_proc(proc.pid)
    prm = ProcessResourceMonitor(ps)
    while proc.is_alive():
        


if __name__ == "__main__":
    sys.exit(main())
    