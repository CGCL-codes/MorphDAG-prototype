# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from .utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return f'rm go-PlainDAG'

    @staticmethod
    def clean_dbs():
      return 'rm -r *data_*'

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'


    @staticmethod
    def compile():
        return f'cd .. ; go build'

    @staticmethod
    def genlocalconfig(shard_num, shard_size, simlatency):
      return (f'go run ../main.go -o 2 -sn {shard_num} -ss {shard_size} -sl {simlatency}')

    @staticmethod
    def genlankeys(port):
      assert isinstance(port, int)
      return (f'./go-PlainDAG -o 0 -p {port}')

    @staticmethod
    def gentskeys(shard_size):
      assert isinstance(shard_size, int)
      return (f'./go-PlainDAG -o 3 -ss {shard_size}')

            
    def run_node(shardid, nodeid, cycle, rate, config_path, batchsize):
        assert isinstance(shardid, int)
        assert isinstance(nodeid, int)
        assert isinstance(config_path, str)
        return (f'./go-PlainDAG -o 1 -s {shardid} -n {nodeid} -c {cycle} -r {rate} -f {config_path} -b {batchsize}')


    @staticmethod
    def alias_binaries():
        node = '../go-PlainDAG'
        return f'rm go-PlainDAG; ln -s {node} .'

    @staticmethod
    def kill():
        return 'tmux kill-server'

