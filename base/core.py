'''
Created on Jan 9, 2017

@author: zimmer
'''
import logging
from tqdm import tqdm
from fnmatch import fnmatch
from glob import glob
from sys import exit as sys_exit
from shutil import rmtree
from os import environ, remove, chdir
from os.path import abspath, isdir, join as opjoin
from copy import deepcopy
from yaml import load as yload
from tempfile import NamedTemporaryFile
from base.utils import sleep, abstractmethod, verifyDampeMC, mkdir, isfile, get_chunks
from base.batch import submit, queryJobs
from XRootD import client

class Runner(object):
    """

        abstract base class, put all the annoying stuff here, focus on implementation in specific Runner derivatives
        self.runCycle & self.initCycle should be implemented by inheriting classes

    """
    def __init__(self,config=None):
        # some default values.
        self.files_to_clean = []
        self.good = False
        self.launcher = None
        self.daemon = {}
        self.software={}
        self.batch = {}
        self.storage = {}
        self.task = {}
        self.log = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.initialize()

    def initialize(self):
        if self.config is None: raise RuntimeError("must intialize with config file, found None")
        self.config = parse_config(self.config)
        for groupKey in ['daemon','software','storage','task']:
            self.__dict__[groupKey].update(self.config[groupKey])
        assert self.storage.type in ['xrootd','local'], 'unsupported storage type'
        self.good = True
        self.log.info("setting software environment")
        # set software env
        self.launcher = self.software.get("launcher",None)
        environ["DAMPE_PREREQUISITE_SCRIPT"]=self.software.get("externals_path","/tmp")
        environ["DAMPME_INSTALL_PATH"]=self.software.get("install_path","/tmp")
        for key,value in self.software.get("env_vars",{}):
            environ[key]=value

    @abstractmethod
    def runCycle(self):
        return

    @abstractmethod
    def initCycle(self):
        return

    def sleep(self):
        """ sleep for some time """
        st = self.daemon.get("sleeptime",300)
        self.log.info("cycle completed, will sleep for %s"st)
        sleep(st)

    def cleanup(self):
        """ clean-up procedure """
        for f in self.files_to_clean:
            self.log.debug("cleanup: remove %s",f)
            remove(f)
        sys_exit(0)

    def execute(self):
        """ this one executes stuff """
        while self.cycle < self.cycles:
            self.log.info("entering cycle %i/%i", self.cycle, self.cycles)
            self.initCycle()
            self.runCycle()
            self.sleep()
            self.cyle += 1
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

    def runCycle(self):
        """ run in each cycle """

        def get_xrd_base(   ):
            kret = ""
            if self.storage_type == 'xrootd':
                kret = "root://{server}:{port}/{base_dir}".format(server=self.storage.get("server", "localhost"),
                                                                  port=int(self.storage.get("port", "1094")),
                                                                  base_dir=self.storage.get("basedir", "/tmp"))
            return kret

        def infile2outfile(infile,base_dir,method='simu:reco'):
            outfile = deepcopy(infile)
            methods = ['simu:reco']
            assert method in methods, "have not implemented other methods yet, signal urgency to zimmer@cern.ch"
            if method == 'simu:reco':
                outfile = outfile.replace(".mc.root",".reco.root")
                outfile = opjoin(base_dir,outfile.replace("mc/simu","mc/reco"))
            return outfile

        infiles = outfiles = []
        verify = self.task.get("verify_output",False)
        base_dirs = self.storage.get("output_root",["/tmp"])

        for f in self.files_to_process:
            infile = f
            bad_file = False
            while len(base_dirs):
                base_dir = base_dirs[0]
                if "@XROOTD:BASEDIR" in base_dir:
                    base_dir = base_dir.replace("@XROOTD:BASEDIR", get_xrd_base())
                outfile = infile2outfile(infile,base_dir)
                if isfile(outfile):
                    self.log.debug("found %s already",outfile)
                    if verify:
                        if verifyDampeMC(outfile):
                            self.log.info("verification of ROOT file successful, skipping")
                        else:
                            self.log.warning("verification of ROOT file failed")
                            bad_file = True
                else:
                    bad_file = True
                if bad_file:
                    base_dirs.pop(0) # remove the first element
            if bad_file:
                continue
            infiles.append(infile)
            outfiles.append(outfile)

        # query the job status
        jobs_in_batch = {}
        try:
            jobs_in_batch = queryJobs()
        except Exception as err:
            self.log.error(str(err))
        for job,status in jobs_in_batch.iteritems():
            if job in self.jobs.keys():
                if status == "C":
                    del self.jobs[job]
                else:
                    self.jobs[job]=status

        # next, split list into chunks.
        nchunks = self.task.get("max_jobs",10) - len(self.jobs.keys())
        maxfiles = self.task.get("max_files_per_job",10)*nchunks
        queue = self.batch.get("queue","short")
        if len(infiles) >= maxfiles:
            infiles = infiles[0:maxfiles-1]
            outfiles= outfiles[0:maxfiles-1]

        # evenly split chunks.
        in_chunks = get_chunks(infiles,nchunks)
        out_chunks= get_chunks(outfiles,nchunks)

        for i in tqdm(range(nchunks)):
            chunk = dict(zip(in_chunks[i],out_chunks[i]))
            tf = NamedTemporaryFile(dir="/tmp",delete=False)
            tf.write("# chunk %i\n"%i)
            tf.write("\n".join(["{infile} {outfile}".format(infile=key,outfile=value) for key,value in chunk.iteritems()]))
            tf.close()
            full_cmd = "{cmd} -t run.txt".format(cmd=self.task.get("command","python"))
            environ['INPUTFILE']=tf.name
            environ['TMP_INPUT']="run.txt"
            environ["EXEC_DIR_ROOT"] = "/tmp"
            environ["DAMPECOMMAND"] = full_cmd
            environ["FILES_TO_CLEANUP"]=tf.name
            cmd = "qsub -q {queue} -v DAMPE_PREREQUISITE_SCRIPT,DAMPE_LOGLEVEL,EXEC_DIR_ROOT" \
                  ",TMP_INPUT,INPUTFILE,DAMPME_INSTALL_PATH,DAMPECOMMAND,CUSTOM_SLEEP -l mem=6000mb" \
                  "-l vmem=6000mb {launcher}".format(launcher=self.launcher, queue=queue)
            self.log.info("submitting chunk %i: %s",i, cmd)
            jobId = -1
            try:
                jobId = submit(cmd)
            except Exception as err:
                self.log.error(str(err))
                continue
            self.jobs[jobId]="Q"
            self.files_to_clean.append(abspath(tf.name))

    def initCycle(self):
        """ initialize each cycle """
        wd = self.task.get("workdir","/tmp/runner")
        if isdir(wd):
            rmtree(wd)
        mkdir(wd)
        chdir(wd)
        # need to fill files_to_process
        def lfn(parent,child,xc=None):
            if xc is None: return ""
            return "root://{server}/{fname}".format(fname=opjoin(parent,child),server=xc.url.hostid)

        files_to_process = []
        pattern = self.task.get("pattern","*")
        base_dir = self.task.get("input_root","/tmp")
        if not base_dir.startswith("@XROOTD:BASEDIR"):
            self.log.info("processing local files")
            files_to_process = [abspath(f) for f in glob("{base}/{pattern}/*.root".format(base=base_dir,pattern=pattern))]
        else:
            self.log.info("processing remote files")
            xc = client.FileSystem("root://{server}:{port}".format(server=self.storage.get("server","localhost"),
                                                                   port=int(self.storage.get("port",8000))))

            base_dir = base_dir.replace("@XROOTD:BASEDIR","")
            is_ok, folders = xc.dirlist(base_dir)
            if not is_ok.ok:
                self.log.error(is_ok.message)
            else:
                tasks = [opjoin(folders.parent,entry.name) for entry in folders.dirlist if fnmatch(entry.name,pattern)]
                self.log.info("found %i tasks",len(tasks))
                for i, task in tqdm(enumerate(tasks)):
                    self.log.info("working on %i task",i)
                    is_ok, folders = xc.dirlist(task)
                    if not is_ok.ok:
                        self.log.error(is_ok.message)
                        continue
                    files_to_process += [lfn(folders.parent, entry.name) for entry in folders.dirlist
                                         if fnmatch(entry.name,"*.root")]
        if len(files_to_process):
            self.log.info("check input files")
            for f in tqdm(files_to_process):
                if isfile(f): self.files_to_process.append(f)
                else:
                    self.log.error("could not add %s",f)
            self.log.info("found %i files to process this cycle",len(self.files_to_process))


def parse_config(cfg):
    config = yload(open(abspath(cfg)))
    assert isinstance(config, dict), "must be dictionary type"
    for groupKey in ['daemon', 'software', 'storage', 'task']:
        group = config.get(groupKey, {})
        assert isinstance(group, dict) and len(group.keys()), "group must contain dictionary with >0 entries"
    return config