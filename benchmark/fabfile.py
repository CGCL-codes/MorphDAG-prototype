# Copyright(C) Facebook, Inc. and its affiliates.
from tkinter.tix import Tree
from fabric import task
from benchmark.local import LocalBench
from benchmark.lan import LanBench

from benchmark.utils import BenchError, Print, PathMaker


@task
def local(ctx):
    ''' Run benchmarks on localhost '''
    # Usage
    # If you change the 'shard_num', you need to set 'regenconfig' to True
    # 2. run benchmark: fab local
    bench_params = {
      'shard_num': 2,
      'shard_size': 11,
      'cycle': 100,
      'rate': 2000,
      'batch_size': 300,
      'measure_rounds': 300, # rounds
      'duration': 60,
      'simlatency': 20.0,
      'regenconfig': True,
    }
    try:
        ret = LocalBench(bench_params).run()
    except BenchError as e:
        Print.error(e)


@task
def lan(ctx):
    bench_params = {
      'shard_num': 8,
      'shard_size': 6,
      'cycle': 50,
      'rate': 2000,
      'batch_size': 500,
      'measure_rounds': 300, # rounds
      'duration': 200,
      'simlatency': 20.0,
      'regenconfig': True,
    }
    try:
        ret = LanBench(bench_params).run()
    except BenchError as e:
        Print.error(e)