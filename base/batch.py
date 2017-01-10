"""
 @brief: wrapper to handle PBS submission
 @author: S. Zimmer
"""
from re import findall
from os import getenv
from base.utils import run as __run__

def __regexId__(_str):
    """ returns the batch Id using some regular expression, pbs specific """
    # default:
    bk = -1
    res = findall(r"\d+", _str)
    if len(res):
        bk = int(res[0])
    return bk

def submit(cmd):
    """ convenience method to wrap batch submission, will return jobID"""
    rc, output, error = __run__(cmd)
    return __regexId__(output)

def queryJobs():
    """ returns a dict of jobs and status """
    jobs = {}
    usr = getenv("PBS_USER","user")
    cmd = "qstat -u {user}".format(user=usr)
    rc, output, error = __run__(cmd)
    if rc:
        raise Exception("error, RC=%i, error msg follows \n %s" % (rc, error))
    lines = output.split("\n")
    if len(lines) > 4:
        rest = output[5:]
        for line in rest:
            line_s = line.split()
            if len(line_s) < 10:
                continue
            jobId = __regexId__(line_s[0])
            status= line_s[-2]
            jobs[jobId]=status
    return jobs