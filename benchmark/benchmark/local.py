# Copyright(C) Facebook, Inc. and its affiliates.
from copy import deepcopy
import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep
import time
from os.path import join

from .commands import CommandMaker
from .logs import LogParser, ParseError

from .utils import BenchError, Print, PathMaker, progress_bar
from collections import OrderedDict
import os
class LocalBench:

    def __init__(self, bench_parameters_dict):
      self.shard_num = bench_parameters_dict['shard_num']
      self.shard_size = bench_parameters_dict['shard_size']
      self.batch_size = bench_parameters_dict['batch_size']
      self.cycle = bench_parameters_dict['cycle']
      self.rate = bench_parameters_dict['rate']
      self.measure_rounds = bench_parameters_dict['measure_rounds']
      self.duration = bench_parameters_dict['duration']
      self.regenconfig = bench_parameters_dict['regenconfig']
      self.simlatency = bench_parameters_dict['simlatency']


    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)


    def run(self):
      
        if self.regenconfig:
          # re-generate config
          Print.heading('Re-generate config for local benchmark')

          cmd = f'{CommandMaker.genlocalconfig(self.shard_num, self.shard_size, self.simlatency)}'
          subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        
        # complie
        cmd = f'{CommandMaker.compile()}'
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        cmd = f'{CommandMaker.alias_binaries()}'
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        Print.heading('Starting local benchmark')
        # Kill any previous testbed.
        self._kill_nodes()
        cmd = f'{CommandMaker.clean_logs()}'
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
        cmd = f'{CommandMaker.clean_dbs()}'
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # run all nodes
        print("shard number: ", self.shard_num , "shard size: ", self.shard_size)
        for shardId in range(self.shard_num+1):
          for nodeId in range(self.shard_size+1):
            if shardId != self.shard_num and nodeId == self.shard_size:
              continue 
            if shardId == self.shard_num and nodeId != self.shard_size:
              continue
            cmd = CommandMaker.run_node(
                shardid=shardId,
                nodeid=nodeId,
                cycle=self.cycle,
                rate=self.rate,
                config_path="./config",
                batchsize=self.batch_size
            )
            log_file = PathMaker.log_file(shardId, nodeId)
            print(cmd)
            # print(log_file)
            self._background_run(cmd, log_file)

        
        Print.info(f'Running benchmark ({self.duration} sec)...')
        print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), "Waiting...")
        for _ in progress_bar(range(self.duration), prefix=f'Running benchmark ({self.duration} sec):'):
            sleep(1)       
        print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), "Done...")
        self._kill_nodes()


        chaindata = [[0]*self.shard_size for i in range(self.shard_num)]
        statedata = [[0]*self.shard_size for i in range(self.shard_num)]
        for shardId in range(self.shard_num+1):
            for nodeId in range(self.shard_size+1):
                if shardId != self.shard_num and nodeId == self.shard_size:
                  continue 
                if shardId == self.shard_num and nodeId != self.shard_size:
                  continue
                if shardId == self.shard_num and nodeId == self.shard_size:
                  break

                data_cmd_MB = f'du --block-size=1M --max-depth=1 *data_{shardId}_{nodeId}'

                out_bytes = subprocess.check_output([data_cmd_MB], shell=True, stderr=subprocess.DEVNULL)
                lines = out_bytes.decode('utf-8').split('\n')
                node_chaindata_MB = int(lines[0].split('\t')[0])
                node_statedata_MB = int(lines[1].split('\t')[0])
                chaindata[shardId][nodeId] = node_chaindata_MB
                statedata[shardId][nodeId] = node_statedata_MB


        # parse logs
        Print.info('Parsing logs and computing performance...')
        logger = LogParser.process(PathMaker.logs_path(), self.shard_num,self.shard_size, self.batch_size, self.cycle, self.rate, self.measure_rounds, self.duration, chaindata, statedata)
        print(logger.result())

        logger.print(PathMaker.result_file(
            'local',
            self.shard_num,
            self.shard_size,
            self.batch_size,
            self.measure_rounds
        ))        
