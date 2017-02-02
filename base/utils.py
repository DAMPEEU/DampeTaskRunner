import logging
from time import sleep as time_sleep
from psutil import AccessDenied, Process as psutil_proc
from os import makedirs, utime
from os.path import isfile as op_isfile, isdir, basename as op_basename
from subprocess import PIPE, Popen
from re import finditer

# add XRootD python bindings: http://xrootd.org/doc/python/xrootd-python/index.html
from XRootD import client
from XRootD.client.flags import OpenFlags, MkDirFlags, AccessMode

# suppress ROOT warnings.
from ROOT import gROOT, TChain
gROOT.ProcessLine("gErrorIgnoreLevel = 3002;")

log = logging.getLogger("utils")


def extractVersionTag(lfn):
    """ returns the proper version tag from a long file."""
    #print 'extracting version tag'
    pattern = "v[0-9]+r[0-9]+p[0-9]+"
    if "trunk" in lfn:
        pattern = "trunk-r[0-9]+"
    match = None
    #print 'lfn: ',lfn
    for m in finditer(pattern,lfn):
        match = m
        #print match
    if match is None:
        raise Exception("could not find version tag.")
    tag = lfn[match.start():match.end()]
    # drop beta...
    if 'b' in tag: tag = tag.replace("b","")
    #print 'TAG: ',tag
    return tag


def basename(lfn):
    if lfn.startswith("root://"):
        server = "root://{server}".format(server=lfn.split("/")[2])
        lfn = lfn.replace(server,"")
    return op_basename(lfn)


def touch(path):
    with open(path, 'a'):
        utime(path, None)


def mkdir(Dir):
    if Dir.startswith("root://"):
        server = Dir.split("/")[2]
        lfn = Dir.replace("root://{server}/".format(server=server),"")
        xc = client.FileSystem("root://{server}".format(server=server))
        mode = AccessMode.OR | AccessMode.OW | AccessMode.OX | AccessMode.GR | AccessMode.UR
        is_ok, reply = xc.mkdir(lfn,flags=MkDirFlags.MAKEPATH,mode=mode)
        if not is_ok.ok:
            #print is_ok.message
            raise IOError(is_ok.message)
        log.debug(is_ok.message)
    else:
        if not isdir(Dir):
            makedirs(Dir)


def isfile(mpath,**kwargs):
    """ returns if file is present, and wraps xrootd protocol"""
    if mpath.startswith("root:"):
        server = mpath.split("/")[2]
        lfn = mpath.replace("root://{server}/".format(server=server),"")
        rc = client.FileSystem(server)
        is_ok, res = rc.stat(lfn)
        if not is_ok.ok:
            #print is_ok.message
            log.warning(is_ok.message)
            return False
        if res.flags < OpenFlags.READ:
            msg = 'user has insufficient permissions to READ file.'
            #print msg
            log.error(msg)
            return False
        else:
            return True
    else:
        return op_isfile(mpath)






def run(cmd):
    """
    :param cmd: command string to execute
    :return: return code, stderr, stdout
    """
    tsk = Popen(cmd.split(), stdout=PIPE, stderr=PIPE, shell=True)
    rc = tsk.wait()
    output = tsk.stdout.read()
    error = tsk.stderr.read()
    if rc:
        msg = "error, RC=%i, error msg follows \n %s" % (rc, error)
        log.error(msg)
        raise RuntimeError(msg)
    return rc, output, error


def verifyDampeMC(fn, ftype = 'reco'):
    """ open root file, check if branches are in there and if metdata is != 0, else return false """
    assert ftype in ['reco','digi'], "must be of type digi or reco"
    branches = ['StkKalmanTracks','DmpEvtBgoRec','StkClusterCollection','DmpEvtPsdRec','DmpGlobTracks']
    if ftype == 'digi':
        branches = ['DmpStkDigitsCollection','DmpEvtNudRaw','DmpEvtBgoHits','DmpPsdHits']
    tch = TChain("CollectionTree")
    tch.Add(fn)
    if not tch.GetEntries():
        return False
    try:
        for b in branches:
            r = tch.FindBranch(b)
            assert r is not None, 'missing branch : %s'%b
    except AssertionError:
        return False
    return True


def abstractmethod(method):
    """
    An @abstractmethod member fn decorator.
    (put this in some library somewhere for reuse).
    http://stackoverflow.com/a/36097662
    """
    def default_abstract_method(*args, **kwargs):
        raise NotImplementedError('call to abstract method '
                                  + repr(method))
    default_abstract_method.__name__ = method.__name__
    return default_abstract_method


def parse_sleep(sleep):
    MINUTE = 60
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY
    if isinstance(sleep, float) or isinstance(sleep, int):
        return sleep
    elif isinstance(sleep, str):
        try:
            return float(sleep)
        except ValueError:
            pass

        if sleep.endswith('s'):
            return float(sleep.strip('s'))
        elif sleep.endswith('m'):
            return float(sleep.strip('m')) * MINUTE
        elif sleep.endswith('h'):
            return float(sleep.strip('h')) * HOUR
        elif sleep.endswith('d'):
            return float(sleep.strip('d')) * DAY
        elif sleep.endswith('w'):
            return float(sleep.strip('w')) * WEEK
        else:
            raise ValueError
    else:
        raise ValueError


def sleep(sleep):
    return time_sleep(parse_sleep(sleep))


class ResourceMonitor(object):
    memory = 0.
    usertime = 0.
    systime = 0.


    def __init__(self):
        self.query()


    def query(self):
        from resource import getrusage, RUSAGE_SELF
        usage = getrusage(RUSAGE_SELF)
        self.usertime = usage[0]
        self.systime = usage[1]
        # http://stackoverflow.com/questions/938733/total-memory-used-by-python-process
        self.memory = getrusage(RUSAGE_SELF).ru_maxrss * 1e-6  # mmemory in Mb


    def getMemory(self, unit='Mb'):
        self.query()
        if unit in ['Mb', 'mb', 'mB', 'MB']:
            return float(self.memory)
        elif unit in ['kb', 'KB', 'Kb', 'kB']:
            return float(self.memory) * 1024.
        elif unit in ['Gb', 'gb', 'GB', 'gB']:
            return float(self.memory) / 1024.
        return 0.


    def getCpuTime(self):
        self.query()
        return self.systime


    def getWallTime(self):
        self.query()
        return self.usertime


    def getEfficiency(self):
        self.query()
        return float(self.usertime) / float(self.systime)


    def __repr__(self):
        self.query()
        user = self.usertime
        sys = self.systime
        mem = self.memory
        return "usertime=%s systime=%s mem %s Mb" % (user, sys, mem)


class ProcessResourceMonitor(ResourceMonitor):
    # here we overload the init method to add a variable to the class


    def __init__(self, ps):
        if not isinstance(ps, psutil_proc):
            raise Exception("must be called from a psutil instance!")
        self.user = 0
        self.system = 0
        self.memory = 0
        self.debug = False
        self.ps = ps
        self.query()


    def getMemory(self, unit='Mb'):
        self.query()
        if unit in ['Mb', 'mb', 'mB', 'MB']:
            return float(self.memory)
        elif unit in ['kb', 'KB', 'Kb', 'kB']:
            return float(self.memory) * 1024.
        elif unit in ['Gb', 'gb', 'GB', 'gB']:
            return float(self.memory) / 1024.
        return 0.


    def getCpuTime(self):
        self.query()
        return self.user + self.system


    def queryResources(self):
        return "MEM: {memory} MB  -- CPU: {cpu} seconds".format(memory=self.getMemory(),cpu=self.getCpuTime())


    def free(self):
        self.user = 0
        self.system = 0
        self.memory = 0


    def query(self):
        dbg = self.debug
        self.free()
        cpu = self.ps.cpu_times()
        # collect parent usage
        usr = cpu.user
        sys = cpu.system
        mem = self.ps.memory_info().rss / float(2 ** 20)
        msg = 'parent: pid %i mem %1.1f sys %1.1f usr %1.1f' % (self.ps.pid, mem, sys, usr)
        if dbg: print '**DEBUG**:',msg
        log.debug(msg)
        child_pids = []
        for child in self.ps.children(recursive=True):
            if int(child.pid) not in child_pids:
                try:
                    ch = self._getChildUsage(child)
                    ch['pid'] = int(child.pid)
                    ch['total'] = ch['user'] + ch['system']
                    msg = 'CHILD FOOTPRINT: {pid} MEM {memory}'\
                              'USR {user} SYS {system} TOT {total}'.format(**ch)
                    log.debug(msg)
                    if dbg:
                        print '**DEBUG**:',msg
                    usr += ch['user']
                    sys += ch['system']
                    mem += ch['memory']
                    child_pids.append(int(child.pid))
                except AccessDenied:
                    msg = 'could not access %i, skipping.' % int(child.pid)
                    log.warning(msg)
                    print msg
        self.user = usr
        self.system = sys
        self.memory = mem
        msg = 'child pids today : %s' % str( child_pids )
        msg+= 'TOTAL this cycle: mem=%1.1f sys=%1.1f usr=%1.1f' % (self.memory, self.system, self.user)
        log.debug(msg)
        if dbg: print '**** DEBUG ****',msg


    def _getChildUsage(self, ps):
        if not isinstance(ps, psutil_proc):
            raise Exception("must be called from a psutil instance!")
        cpu = ps.cpu_times()
        usr = cpu.user
        sys = cpu.system
        mem = ps.memory_info().rss / float(2 ** 20)
        return {'memory': mem, 'system': sys, 'user': usr}
