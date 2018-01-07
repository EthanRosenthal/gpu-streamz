# gpu-streamz

Live GPU monitoring with streamz


## Installation

```bash
git clone https://github.com/EthanRosenthal/gpu-streamz.git
cd gpu-streamz

conda env create -f environment.yml
conda activate gpu-streamz

# Install this streamz fork
pip install -e git+https://github.com/philippjfr/streamz.git@ced72b2583292decf928fae75613c20d068ab874#egg=streamz

# Install gpu_streamz
pip install -e .
```


## Usage

```python
from gpu_streamz import GPUMonitor

monitor = GPUMonitor()
monitor.start()
```
