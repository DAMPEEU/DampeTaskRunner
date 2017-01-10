from time import sleep as time_sleep
from psutil import AccessDenied, Process as psutil_proc
from os.path import isfile
from subprocess import PIPE, Popen
from XRootD import client
from XrootD.client.flags import OpenFlags


def run(cmd):
    """
    :param cmd: command string to execute
    :return: return code, stderr, stdout
    """
    tsk = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
    rc = tsk.wait()
    output = tsk.stdout.read()
    error = tsk.stderr.read()
    if rc:
        raise Exception("error, RC=%i, error msg follows \n %s" % (rc, error))
    return rc, output, error

def get_chunks(MyList, n):
  return [MyList[x:x+n] for x in range(0, len(MyList), n)]

def isfile(mpath,**kwargs):
    """ returns if file is present, and wraps xrootd protocol"""
    if mpath.startswith("root:"):
        assert 'server' in kwargs, 'must provide server to go with'
        server = kwargs.get("server","localhost")
        if server == 'localhost': raise RuntimeError("found server localhost, expect remote")
        lfn = mpath.replace("{server}/".format(server=server),"")
        rc = client.FileSystem(server)
        is_ok, res = rc.stat(lfn)
        if not is_ok.ok:
            print is_ok.message
            return False
        if res.flags < OpenFlags.READ:
            print 'user has insufficient permissions to READ file.'
            return False
        else:
            return True
    else:
        return isfile(mpath)


def verifyDampeMC(self,fn, ftype = 'reco'):
    """ open root file, check if branches are in there and if metdata is != 0, else return false """

    assert ftype in ['reco','digi'], "must be of type digi or reco"
    branches = ['StkKalmanTracks','DmpEvtBgoRec','StkClusterCollection','DmpEvtPsdRec','DmpGlobTracks']
    if ftype == 'digi':
        branches = ['DmpStkDigitsCollection','DmpEvtNudRaw','DmpEvtBgoHits','DmpPsdHits']
    from ROOT import TChain
    tch = TChain("CollectionTree")
    tch.Add(fn)
    if not tch.GetEntries():
        return False
    try:
        for b in branches:
            r = tch.FindBranch(b)
            assert not(r == None), 'missing branch : %s'%b
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
        if dbg: print '**DEBUG**: parent: pid %i mem %1.1f sys %1.1f usr %1.1f' % (self.ps.pid, mem, sys, usr)
        child_pids = []
        for child in self.ps.children(recursive=True):
            if int(child.pid) not in child_pids:
                try:
                    ch = self._getChildUsage(child)
                    ch['pid'] = int(child.pid)
                    ch['total'] = ch['user'] + ch['system']
                    if dbg:
                        print '**DEBUG**: CHILD FOOTPRINT: {pid} MEM {memory} USR {user} SYS {system} TOT {total}'.format(
                            **ch)
                    usr += ch['user']
                    sys += ch['system']
                    mem += ch['memory']
                    child_pids.append(int(child.pid))
                except AccessDenied:
                    print 'could not access %i, skipping.' % int(child.pid)
        self.user = usr
        self.system = sys
        self.memory = mem
        if dbg: print 'child pids today : ', child_pids
        if dbg: print '**** DEBUG **** TOTAL this cycle: mem=%1.1f sys=%1.1f usr=%1.1f' % (
        self.memory, self.system, self.user)

    def _getChildUsage(self, ps):
        if not isinstance(ps, psutil_proc):
            raise Exception("must be called from a psutil instance!")
        cpu = ps.cpu_times()
        usr = cpu.user
        sys = cpu.system
        mem = ps.memory_info().rss / float(2 ** 20)
        return {'memory': mem, 'system': sys, 'user': usr}

