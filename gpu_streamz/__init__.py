from subprocess import check_output
from time import time

import pandas as pd
from streamz.dataframe.holoviews import DataFrame
from streamz.sources import PeriodicCallback, Source
from tornado.ioloop import IOLoop
from tornado import gen


def _parse_value(line, unit):
    return line.split(':')[-1].rstrip(unit).strip()


def _nvidia_smi():
    """
    Determine GPU stats using nvidia-smi command line tool.

    This is an ungodly hack.
    """
    status = check_output(['nvidia-smi', '-q'])
    status = status.decode('utf-8').split('\n')
    util_found = False
    memory_found = False
    for i in range(len(status)):
        line = status[i]
        if line.lstrip().startswith('Utilization'):
            for j in range(i + 1, len(status)):
                if util_found:
                    break
                sub_line = status[j]
                if sub_line.lstrip().startswith('Gpu'):
                    # '        Gpu                         : 9 %'
                    util = int(_parse_value(sub_line, '%'))
                    util_found = True
        if line.lstrip().startswith('FB Memory Usage'):
            total = None
            used = None
            for j in range(i + 1, len(status)):
                if total is not None and used is not None:
                    break
                sub_line = status[j]
                
                if sub_line.lstrip().startswith('Total'):
                    total = int(_parse_value(sub_line, 'MiB'))

                if sub_line.lstrip().startswith('Used'):
                    used = int(_parse_value(sub_line, 'MiB'))
            memory = float(used) / total * 100
            memory_found = True
                    
    if not util_found or not memory_found:
        return (-1, -1)
            
    return (util, memory)


def _make_df(tup):
    last, now, freq = tup
    index = pd.DatetimeIndex(start=(last + freq.total_seconds()) * 1e9,
                             end=now * 1e9, 
                             freq=freq,
                             name='Time since start')
    utilization, memory = _nvidia_smi()
    df = pd.DataFrame({'Utilization (%)': utilization,
                       'Memory (%)': memory},
                      index=index)
    return df


class GPUStream(DataFrame):
    """
    Reformatted version of the Random streamz dataframe.
    https://github.com/mrocklin/streamz/blob/master/streamz/dataframe/core.py
    """

    def __init__(self, freq='5ms', interval='100ms', dask=False):
        if dask:
            from streamz.dask import DaskStream
            source = DaskStream()
            loop = source.loop
        else:
            source = Source()
            loop = IOLoop.current()
        self.freq = pd.Timedelta(freq)
        self.interval = pd.Timedelta(interval).total_seconds()
        self.source = source
        self.continue_ = [True]

        stream = self.source.map(_make_df)
        example = _make_df((time(), time(), self.freq))

        super(GPUStream, self).__init__(stream, example)

        loop.add_callback(self._cb, self.interval, self.freq, self.source,
                          self.continue_)

    def __del__(self):
        self.stop()

    def stop(self):
        self.continue_[0] = False

    @staticmethod
    @gen.coroutine
    def _cb(interval, freq, source, continue_):
        last = time()
        while continue_[0]:
            yield gen.sleep(interval)
            now = time()
            yield source._emit((last, now, freq))
            last = now
            

class GPUMonitor:

    def __init__(self, freq='5ms', interval='100ms', dask=False):

        self.stream = GPUStream(freq=freq, interval=interval, dask=dask)

    def start(self):
        return self.stream.plot(kind='line', ylim=(0, 100), 
                                title='GPU Monitor')
