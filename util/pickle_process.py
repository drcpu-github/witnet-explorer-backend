from multiprocessing import Process
from multiprocessing.process import AuthenticationString

class PickleProcess(Process):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, daemon=None):
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)

    def __getstate__(self):
        state = self.__dict__.copy()
        conf = state['_config']
        if 'authkey' in conf:
            conf['authkey'] = bytes(conf['authkey'])
        return state

    def __setstate__(self, state):
        state['_config']['authkey'] = AuthenticationString(state['_config']['authkey'])
        self.__dict__.update(state)
