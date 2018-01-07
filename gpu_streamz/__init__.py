from io import StringIO
from subprocess import check_output
from time import time

import pandas as pd
from streamz.dataframe.holoviews import DataFrame
from streamz.sources import PeriodicCallback, Source
from tornado.ioloop import IOLoop
from tornado import gen


def _nvidia_smi():
    """
    Determine GPU stats using nvidia-smi command line tool.

    Props to Ryan Vilim (@rvilim) for pointing out how to better use the 
    nvidia-smi command line arguments.
    """

    status = check_output(['nvidia-smi', 
                           '--query-gpu=utilization.gpu,utilization.memory', 
                           '--format=csv'])
    status = pd.read_csv(StringIO(status.decode('utf-8')))
    
    # Reformat column names.
    # (Need the col.strip() because sometimes there are preceding spaces)
    map_cols = {'utilization.gpu [%]': 'Utilization (%)',
                'utilization.memory [%]': 'Memory (%)'}
    status.columns = [map_cols[col.strip()] for col in status.columns]

    # Convert to numerical data
    for col in status.columns:
        status[col] = status[col].apply(lambda x: int(x.rstrip('%')))

    return status


def _make_df(tup):
    last, now, freq = tup
    index = pd.DatetimeIndex(start=(last + freq.total_seconds()) * 1e9,
                             end=now * 1e9, 
                             freq=freq,
                             name='Time since start')
    df = _nvidia_smi()

    if len(index) == 0:
        # When we first call this to initialize the dataframe, the index is 
        # empty
        return pd.DataFrame(columns=df.columns, index=index)

    df = pd.concat([df for _ in range(len(index))])
    df.index = index
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
