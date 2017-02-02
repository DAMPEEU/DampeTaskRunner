"""
 @brief: wrapper to handle PBS submission
 @author: S. Zimmer
"""
import logging
from re import findall
from os import getenv, environ
from os.path import abspath
from base.utils import run as __run__

log = logging.getLogger("core")

class hpc(object):
    user = None
    final_statii = []
    executor = None
    def __regexId__(self,_str):
        """ returns the batch Id using some regular expression, pbs specific """
        # default:
        bk = -1
        res = findall(r"\d+", _str)
        if len(res):
            bk = int(res[0])
        return bk

    def getFinalStatii(self):
        return tuple(self.final_statii)

    def setSubmitter(self,cmd):
        self.executor = cmd

    def setUser(self,usr):
        self.user = usr

    def __submit__(self,cmd,dry=False,verbose=True):
        """ convenience method to wrap batch submission, will return jobID"""
        if verbose:
            self.log.info("bash: %s",cmd)
        if dry:
            self.log.info("running in DRY mode, do not submit anything.")
            return -1
        rc, output, error = __run__(cmd)
        if rc:
            raise RuntimeError(error)
        return self.__regexId__(output)

class pbs(hpc):
    final_statii = ["C"]
    executor = "bsub"
    def queryJobs(self):
        """ returns a dict of jobs and status """
        jobs = {}
        cmd = "qstat -u {user}".format(user=self.user)
        rc, output, error = __run__(cmd)
        if rc:
            msg = "error, RC=%i, error msg follows \n %s" % (rc, error)
            log.error(msg)
            raise Exception(msg)
        lines = output.split("\n")
        if len(lines) > 4:
            rest = output[5:]
            for line in rest:
                line_s = line.split()
                if len(line_s) < 10:
                    continue
                jobId = self.__regexId__(line_s[0])
                status = line_s[-2]
                jobs[jobId] = status
        return jobs

    def submit(self,**kwargs):
        """
            dictionary of keywords to be parsed
        """
        executable  = kwargs.get("executable")
        env         = dict(kwargs.get("env",{}))
        memory      = kwargs.get("memory",0.)
        queue       = kwargs.get("queue",None)
        dry         = bool(kwargs.get("dry",False))
        verbose     = bool(kwargs.get("verbose",True))
        for key,value in env.iteritems():
            environ[key]=value
        if queue is None: raise Exception("must provide queue")
        cmd="{sub} -q {queue} -v {env} -l mem={memory}"\
            " -l vmem={memory} {executable}".format(sub=self.executor,queue=queue,
                                                    env=",".join(env.keys()),
                                                    memory=memory,executable=executable)
        return self.__submit__(cmd,verbose=verbose,dry=dry)

class slurm(hpc):
    final_statii = ["CA","F","TO","CD","SE"]
    executor = "sbatch"
    def queryJobs(self):
        """ returns a dict of jobs and status """
        jobs = {}
        cmd = "squeue -u {user} ".format(user=self.user)
        log.error("**DEBUG** status cmd: %s",cmd)
        rc, output, error = __run__(cmd)
        if rc:
            msg = "error, RC=%i, error msg follows \n %s" % (rc, error)
            log.error(msg)
            raise Exception(msg)
        lines = output.split("\n")
        if len(lines) == 1: jobs = {}
        else:
            lines = lines[1:-1]
            for line in lines[1:-1]:
                while "\n" in line: line = line.replace("\n","")
                my_line = [f for f in line.split(" ") if not f == ""]
                jobId = my_line[0]
                if "_" in jobId: jobId = int(jobId.split("_")[0])
                status = my_line[4]
                user   = my_line[3]
                if not user == self.user: continue
                log.error(str(my_line))
                while " " in status: status = status.replace(" ","")
                jobs[jobId] = status
        return jobs

    def submit(self,**kwargs):
        """
            dictionary of keywords to be parsed
        """
        executable  = kwargs.get("executable")
        env         = dict(kwargs.get("env",{}))
        memory      = kwargs.get("memory",0.)
        cpu         = kwargs.get("cpu",0.)
        dry         = bool(kwargs.get("dry",False))
        verbose     = bool(kwargs.get("verbose",True))

        for key,value in env.iteritems():
            environ[key]=value

        if cpu == 0.: raise Exception("must provide cpu time")
        cmd="{sub} -t {cpu} --mem={memory} --export={env} --workdir={wd} {executable}".format(sub=self.executor,
                                                                                wd=abspath("."),
                                                                                env=",".join(env.keys()),
                                                                                memory=memory, cpu=cpu,
                                                                                executable=executable)
        log.info(cmd)
        return self.__submit__(cmd,verbose=verbose,dry=dry)
