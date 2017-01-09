'''
Created on Jan 9, 2017

@author: zimmer
'''


class RecoRunner(object):
    def __init__(self,**kwargs):
        self.cycles= # number of cyles to run
        self.cycle = 0
        self.sleep = # sleep time between cycles
    
    def initCycle(self):
        """ initialize each cycle """
        pass
    
    def runCycle(self):
        """ run in each cycle """
        pass

    def sleep(self):
        """ sleep for some time """
        pass
    
    def execute(self):
        """ this one executes stuff """
        while self.cycle < self.cycles:
            self.initCycle()
            self.runCycle()
            self.sleep()
        # reached last cycle
    
