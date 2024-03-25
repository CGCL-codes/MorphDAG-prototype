
from os.path import join
from random import sample
from re import findall, search
from statistics import mean
from glob import glob

from .utils import Print
from multiprocessing import Pool

from random import sample




class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, nodes, shard_nums, shard_size, batch_size, cycle, rate, measure_rounds, exec_duration, chaindata_MB_list, statedata_MB_list, machines):
        
        self.shard_nums = shard_nums
        self.shard_size = shard_size
        self.batch_size = batch_size
        self.cycle = cycle
        self.rate = rate
        self.measure_rounds = measure_rounds
        self.exec_duration = exec_duration
        self.chaindata_MB_list = chaindata_MB_list
        self.statedata_MB_list = statedata_MB_list
        self.machines = machines

        # Parse the nodes logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_node, nodes)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')

        duration_ms, mean_latency = zip(*results)
        self.duration_ms = mean(duration_ms)
        self.mean_latency = mean(mean_latency)
        # print(duration_ms)
        # print(mean_latency)

        # self.tps = self.batch_size * self.shard_size * 100 / self.duration * self.shard_nums 
        self.tps = self.batch_size * self.shard_size * self.measure_rounds / self.duration_ms * 1000
    
    def _parse_node(self, log):

        tmp = findall(r'commit latency:  (\d+)  ms', log)
        latency = [int(d) for d in tmp] 
        # print(latency)

        s = f'commit round:  {self.measure_rounds}  duration:  (\d+)  ms'
        tmp = findall(s, log)
        duration_ms = int(tmp[0])
        # print(duration)

        return duration_ms, mean(latency)
        
    def result(self):
        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Total node number: {self.shard_nums * self.shard_size} node(s)\n'
            f' Shard number: {self.shard_nums} shard(s)\n'
            f' Committee size: {self.shard_size} node(s)\n'
            f' Tx sending cycles: {self.cycle}\n'
            f' Tx sending rate per round: {self.rate}\n'
            f' Round number for measuring tps: {self.measure_rounds} rounds\n'
            f' Batch size: {self.batch_size} txs\n'
            f' Execution time: {round(self.exec_duration):,} s\n'
            f' Machines: {self.machines} s\n'
            
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS of a single committee: {round(self.tps):,} tx/s\n'
            f' Consensus latency: {round(self.mean_latency):,} ms\n'
            f' Duration of {self.measure_rounds} rounds: {round(self.duration_ms):,} ms\n'
            f' Chaindata storage cost (MB): {self.chaindata_MB_list} \n'
            f' Statedata storage cost (MB): {self.statedata_MB_list} \n'            
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a+') as f:
            f.write(self.result())



    @classmethod
    def process(cls, directory, shard_nums, shard_size, batch_size, cycle, rate, measure_rounds, exec_duration, chaindata_MB_list, statedata_MB_list, machines=1):
        assert isinstance(directory, str)

        nodes = []
        for filename in sorted(glob(join(directory, 'node-*.log'))):
            # print(filename)
            if filename == join(directory, f'node-{shard_nums}-6.log'):
              continue
            with open(filename, 'r') as f:
                nodes += [f.read()]
        return cls(nodes, shard_nums, shard_size, batch_size, cycle, rate, measure_rounds, exec_duration, chaindata_MB_list, statedata_MB_list, machines)
