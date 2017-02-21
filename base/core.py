'''
Created on Jan 9, 2017

@author: zimmer
'''
import logging
from numpy import array, array_split, savetxt, arange
from datetime import datetime
from tqdm import tqdm
from random import randint
from fnmatch import fnmatch
from glob import glob
from sys import exit as sys_exit
from shutil import rmtree
from getpass import getuser
from os import environ, getenv, remove, chdir
from os.path import abspath, isdir, join as opjoin
from copy import deepcopy
from yaml import load as yload
from tempfile import NamedTemporaryFile
from base.utils import sleep, basename, abstractmethod, verifyDampeMC, mkdir, isfile, extractVersionTag
from base.utils import run as shell_call
from base.batch import slurm, pbs
from XRootD import client

class Runner(object):
    """

        abstract base class, put all the annoying stuff here, focus on implementation in specific Runner derivatives
        self.runCycle & self.initCycle should be implemented by inheriting classes

    """
    def __init__(self,config=None):
        # some default values.
        self.dry = False
        self.files_to_clean = []
        self.processed_files= []
        self.good = False
        self.launcher = None
        self.cycle = 0
        self.cycles= 1000
        self.daemon = {}
        self.software={}
        self.batch = {}
        self.storage = {}
        self.task = {}
        self.log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.initialize()
        self.workdir = None
        self.continue_cycle = True

    def setWorkDir(self,wd):
        if isdir(wd):
            rmtree(wd)
        mkdir(wd)
        self.workdir = wd

    def setDryRun(self):
        self.dry = True

    def initialize(self):
        if self.config is None: raise RuntimeError("must intialize with config file, found None")
        self.config = parse_config(self.config)
        self.config['task'] = self.config['tasks'].get(self.__class__.__name__,{})
        for groupKey in ['daemon','batch','software','storage','task']:
            group = self.config[groupKey]
            assert len(group.keys()), "{group} must contain more than 0 keys".format(group=groupKey)
            self.__dict__[groupKey].update(self.config[groupKey])
        assert self.storage.get("type","") in ['xrootd','local'], 'unsupported storage type'
        self.good = True
        self.log.info("setting software environment")
        # set software env
        self.cycles = self.daemon.get("cycles",1000)
        self.log.info("requested to run %i cycles",self.cycles)
        self.launcher = self.software.get("launcher",None)
        environ["DAMPE_PREREQUISITE_SCRIPT"]=self.software.get("externals_path","/tmp")
        environ["DAMPME_INSTALL_PATH"]=self.software.get("install_path","/tmp")
        environ["DAMPE_VERSION_TAG"]=self.software.get("version","v5r3p0")
        self.batch_system = self.batch.get("system","default")
        if self.batch_system == 'default':
            self.log.info("found no keyword for batch:system, assume default (PBS)")
            self.batch_system = 'pbs'
        assert self.batch_system in ['pbs','slurm'], "unsupported batch system"
        self.hpc = eval("%s()"%self.batch_system)
        executor = self.batch.get("submit_command","None")
        if executor != "None": self.hpc.setSubmitter(executor)
        self.hpc.setUser(self.batch.get("user",getuser()))
        for key,value in self.software.get("env_vars",{}).iteritems():
            environ[key]=value


    @abstractmethod
    def runCycle(self):
        return

    @abstractmethod
    def initCycle(self):
        return

    def getProxy(self):
        proxy = self.batch.get("proxy",None)
        if proxy is None: return True
        rc, out, err = shell_call(proxy)
        if rc:
            self.log.error(str(err))
            return False
        return True

    def sleep(self):
        """ sleep for some time """
        st = self.daemon.get("sleeptime",300)
        self.log.info("cycle completed, will sleep for %s",st)
        sleep(st)

    def cleanup(self):
        """ clean-up procedure """
        self.files_to_clean.append(self.daemon['pidfile'])
        for f in self.files_to_clean:
            self.log.debug("cleanup: remove %s",f)
            remove(f)
        sys_exit(0)

    def flush(self,keepErrors=False):
        """ cleanup in each cycle! """
        if not len(self.processed_files): return
        pattern = "{path}/{launcher}.*"
        if keepErrors:
            pattern = "{path}/*/{launcher}.o*"
        launcher = basename(self.launcher)
        files_to_remove = glob(pattern.format(path=abspath(basename(self.workdir)), launcher=launcher))
        self.log.info("cleanup cycle, found %i files to clean",len(files_to_remove))
        for f in files_to_remove:
            self.log.debug("cleanup: remove %s", f)
            remove(f)
        return

    def execute(self):
        """ this one executes stuff """
        while self.cycle < self.cycles:
            if not self.continue_cycle:
                break
            self.log.info("entering cycle %i/%i", self.cycle, self.cycles)
            self.getProxy()
            self.initCycle(addFiles=True if self.cycle == 0 else False)
            self.runCycle()
            if self.cycle > 1: self.flush()
            self.sleep()
            self.cycle += 1
        self.log.info("reached last cycle, start cleaning procedure")
        self.cleanup()
        # reached last cycle

class RecoRunner(Runner):
    """ this runner does the following each cycle:

        init: create a new listing, comparing what's in input & output folders, verify files, if requested
        runCycle: loop over jobs and submit new job with chunk
    """
    files_to_process = []
    storage_type = 'local'
    jobs = {}

    def __get_xrd_base__(self):
        kret = ""
        if self.storage_type == 'xrootd':
            kret = "root://{server}:{port}/{base_dir}".format(server=self.storage.get("server", "localhost"),
                                                              port=int(self.storage.get("port", "1094")),
                                                              base_dir=self.storage.get("basedir", "/tmp"))
        return kret

    def runCycle(self):
        """ run in each cycle """
        chdir(self.workdir)
        # next, split list into chunks.
        jobs_in_batch = {}
        try:
            self.log.info("querying HPC system")
            jobs_in_batch = self.hpc.queryJobs()
            self.log.info("jobs currently in system %i",len(jobs_in_batch.keys()))
        except Exception as err:
            self.log.exception("exception in trying to retrieve jobs")
            raise
        self.log.info("finished querying Batch system")
        for job,status in jobs_in_batch.iteritems():
            if job in self.jobs.keys():
                if status in self.hpc.getFinalStatii():
                    del self.jobs[job]
                else:
                    self.jobs[job]=status
        if self.cycle > 0:
            if not len(jobs_in_batch):
                self.log.info("found no more running jobs, flushing memory.")
                self.jobs = {}

        nchunks = self.batch.get("max_jobs",10) - len(self.jobs.keys())
        if not nchunks:
            self.log.info("all available slots are occupied, do nothing.")
            return
        nfiles  = self.task.get("max_files_per_job",10)
        maxfiles = nfiles * nchunks
        self.log.info("#chunks %i | #files %i | #total files %i",nchunks, nfiles, maxfiles)


        def infile2outfile(infile,target='xrootd',method='simu:reco'):
            vtag = getenv("DAMPE_VERSION_TAG","v5r3p0")
            ctag = extractVersionTag(infile)
            lfn = infile
            server = ""
            if infile.startswith("root://"):
                server = "root://{server}".format(server=lfn.split("/")[2])
                lfn = lfn.replace(server,"")
            lfn_in = lfn
            outfile = deepcopy(lfn_in)
            if ctag != vtag:
                self.log.debug('DEBUG %s -> %s',vtag, ctag)
                while ctag in outfile:
                    outfile = outfile.replace(ctag,vtag)

            methods = ['simu:reco']
            assert method in methods, "have not implemented other methods yet, signal urgency to zimmer@cern.ch"
            if method == 'simu:reco':
                outfile = (outfile.replace(".mc.root",".reco.root")).replace("mc/simu","mc/reco")
            lfn_out= outfile
            if target == 'xrootd':
                return "{server}{lfn}".format(server=server,lfn=lfn_out)
            return lfn_out

        files = []
        verify = self.task.get("verify_output",False)
        if verify:
            self.log.info("Requested verification of output files prior to submitting jobs")
        else:
            self.log.info("skipping verification")
        base_dirs = self.task.get("output_root",["/tmp"])

        steps = int(maxfiles/10.)
        files_per_chunk = maxfiles / nchunks
        progress = 0
        start = datetime.now()
        self.log.info("processing %i files this cycle",len(self.files_to_process))
        skipped_files = []
        files_already_processed = []
        files_already_there = []
        nfiles_added = 0
        for i,f in enumerate(self.files_to_process):
            skip = False
            if len(files) >= maxfiles:
                self.log.info("progress: 100 percent - reached maximum number of files to process this cycle: %i",len(files))
                break
            if nfiles_added >= steps:
                progress += 10
                self.log.info("progress: %i percent", progress)
                nfiles_added = 0
            fname = basename(f)
            if fname in self.processed_files:
                self.log.debug("file already being processed.")
                skipped_files.append(fname)
                files_already_processed.append(fname)
                skip = True
            infile = f
            outfilesF = []
            for base_dir in base_dirs:
                if skip: continue
                base_dir
                self.log.debug("using basedir: %s",base_dir)
                target = 'local'
                if "@XROOTD:BASEDIR" in base_dir:
                    base_dir = base_dir.replace("@XROOTD:BASEDIR", self.__get_xrd_base__())
                    target = 'xrootd'
                outfile = infile2outfile(infile,target=target)
                if target == 'local':
                    outfile = "".join([base_dir,outfile])
                    while "//" in outfile:
                        outfile = outfile.replace("//","/")
                outfilesF.append(outfile)
                if isfile(outfile):
                    self.log.debug("found %s already",outfile)
                    if verify:
                        if verifyDampeMC(outfile):
                            self.log.debug("verification of ROOT file successful, skipping")
                            skip = True
                        else:
                            self.log.debug("verification of ROOT file failed")
                            continue # move on to 2nd method.
                    else:
                        self.log.debug("skipping verification, skipping file.")
                        skip = True
            if skip:
                skipped_files.append(fname)
                files_already_there.append(fname)
            else:
                # file not being present, should process
                self.log.debug("FILE: %s -> %s", infile, outfilesF[0])
                files.append((infile, outfilesF[0]))
                self.processed_files.append(fname)
                nfiles_added+=1
        stop = datetime.now()
        dt = (stop - start).total_seconds()/60.
        self.log.info("finished assembling list of %i processed files (took %i minutes to complete.)",len(files),int(dt))
        self.log.info("skipped %i files, %i of which are already on xrootd, %i already processed",len(skipped_files),
                        len(files_already_there),len(files_already_processed))

        if not len(files):
            self.log.info("found no files to submit this cycle, return")
            self.continue_cycle = False
            return
        self.log.info("submitting %i files this cycle",len(files))
        memory= self.batch.get("mem","100Mb")

        chunks = array_split(array(files),nchunks)
        self.log.info("preparing %i chunks this cycle",len(chunks))
        for i,chunk in enumerate(chunks):
            #self.log.debug(dict(chunk.tolist()))
            tf = NamedTemporaryFile(dir=self.workdir,delete=False)
            savetxt(tf.name,chunk,fmt="%s",header="chunk %i"%(i+1),delimiter=" ")
            self.log.debug("chunkfile: %s",tf.name)
            full_cmd = "{cmd} -t run.txt".format(cmd=self.task.get("command","python"))
            environ['INPUTFILE']=tf.name
            environ['TMP_INPUT']="run.txt"
            environ["EXEC_DIR_ROOT"] = "/tmp"
            environ["DAMPECOMMAND"] = full_cmd
            environ["FILES_TO_CLEANUP"]=abspath(tf.name)
            my_env_keys = "DAMPE_PREREQUISITE_SCRIPT,DAMPE_LOGLEVEL,EXEC_DIR_ROOT,TMP_INPUT,"\
                          "INPUTFILE,DAMPME_INSTALL_PATH,DAMPECOMMAND"

            my_env = {key:str(getenv(key)) for key in my_env_keys.split(",")}
            my_env['CUSTOM_SLEEP']=str(randint(0,30))


            my_dict = dict(env=my_env, executable=self.launcher, memory=memory)
            if self.batch_system == "pbs":
                my_dict["queue"]=self.batch.get("queue","short")
            elif self.batch_system=="slurm":
                my_dict["cpu"]=self.batch.get("cpu","1440")
                my_dict["partition"]=self.batch.get("partition","debug")
            else:
                raise NotImplementedError("currently only supported batch systems are: PBS, SLURM")

            jobId = -1
            try:
                #self.dry = True # REMOVE WHEN DONE!
                self.log.info("submitting chunk %i/%i: ", i + 1, nchunks)
                jobId = self.hpc.submit(dry=self.dry,verbose=False,workdir=self.workdir,**my_dict)
                if self.dry: continue
            except Exception as err:
                self.log.error(str(err))
                raise
            self.jobs[jobId]="Q" if self.batch_system == "pbs" else "PD"
            self.log.info("submitted job %s",jobId)

    def initCycle(self,addFiles=True):
        """ initialize each cycle """
        wd = self.task.get("workdir","/tmp/runner")
        wd = opjoin(wd,"cycle_{i}".format(i=self.cycle+1))
        self.setWorkDir(wd)
        chdir(self.workdir)
        # need to fill files_to_process
        def lfn(parent,child,xc=None):
            if xc is None: return ""
            return "root://{server}/{fname}".format(fname=opjoin(parent,child),server=xc.url.hostid)

        files_to_process = []
        pattern = self.task.get("pattern","*")
        if not isinstance(pattern,list):
            pattern = [pattern]
        base_dir = self.task.get("input_root","/tmp")
        if not base_dir.startswith("@XROOTD:BASEDIR"):
            self.log.info("processing local files")
            for p in pattern:
                files_to_process += [abspath(f) for f in glob("{base}/{pattern}/*.root".format(base=base_dir,pattern=p))]
        else:
            self.log.info("processing remote files")
            server = "root://{server}:{port}".format(server=self.storage.get("server","localhost"),
                                                                   port=int(self.storage.get("port",8000)))
            xc = client.FileSystem(server)
            self.log.debug("REMOTE server: %s",server)
            base_dir = base_dir.replace("@XROOTD:BASEDIR",self.storage.get("basedir",""))
            self.log.debug("REMOTE base dir: %s",base_dir)
            is_ok, folders = xc.dirlist(base_dir)
            if not is_ok.ok:
                self.log.error(is_ok.message)
                return
            else:
                tasks = []
                if addFiles:
                    for p in pattern:
                        tasks += [opjoin(folders.parent,entry.name) for entry in folders.dirlist if fnmatch(entry.name,p)]
                    self.log.info("found %i tasks, querying for files.",len(tasks))
                    loop = enumerate(tasks)
                    if self.dry: loop = tqdm(loop)
                    for i, task in loop:
                        self.log.info("%i/%i: working on task: %s",i+1,len(tasks),task)
                        is_ok, folders = xc.dirlist(task)
                        if not is_ok.ok:
                            self.log.error(is_ok.message)
                            continue
                        files_to_add = []
                        for entry in folders.dirlist:
                            self.log.debug(str(entry))
                            lfn_name = lfn(folders.parent, entry.name, xc=xc)
                            self.log.debug(lfn_name)
                            if fnmatch(lfn_name,"*.root"): files_to_add.append(lfn_name)
                        self.log.info("adding %i files to processing list",len(files_to_add))
                        files_to_process += files_to_add

#  < < SUBJECT FOR REMOVAL > >
#        if len(files_to_process):
#            self.log.info("check input files")
#            loop = files_to_process
#            if self.dry: loop = tqdm(loop)
#            for f in loop:
#                if f in self.processed_files: continue # skip
#                if isfile(f):
#                    if f in self.files_to_process:
#                        self.log.debug("file already in list of files to process")
#                        continue
#                    self.files_to_process.append(f)
#                else:
#                    self.log.error("could not add %s",f)
#            self.log.info("found %i files to process this cycle",len(self.files_to_process))

        if len(files_to_process):
            self.log.info("check for duplicates in input")
            self.files_to_process = list(set(files_to_process))

def parse_config(cfg):
    config = yload(open(abspath(cfg)))
    assert isinstance(config, dict), "must be dictionary type"
    for groupKey in ['daemon','batch', 'software', 'storage']:
        group = config.get(groupKey, {})
        assert isinstance(group, dict), "{group} must be of type dictionary".format(group=groupKey)
    return config