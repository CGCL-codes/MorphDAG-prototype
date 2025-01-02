
from fabric import Connection, ThreadingGroup as Group
from .commands import CommandMaker
import subprocess
from .utils import BenchError, Print, PathMaker, progress_bar
from fabric.exceptions import GroupException
import yaml
import os
from os.path import basename, splitext
import time
import threading
from .logs import LogParser, ParseError

class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass



class LanBench:

    def __init__(self, bench_parameters_dict):
      self.shard_num = bench_parameters_dict['shard_num']
      self.shard_size = bench_parameters_dict['shard_size']
      self.batch_size = bench_parameters_dict['batch_size']
      self.cycle = bench_parameters_dict['cycle']
      self.rate = bench_parameters_dict['rate']  
      self.measure_rounds = bench_parameters_dict['measure_rounds']
      self.duration = bench_parameters_dict['duration']
      self.simlatency = bench_parameters_dict['simlatency']
      self.regenconfig = bench_parameters_dict['regenconfig']
      self.config_path = 'lan_config'
      self.user='root'
      self.connect = {"password": '374473'}
      self.hosts = [
            '10.176.50.35:32775', #
            '10.176.50.16:32774', #
            '10.176.50.36:32792', # 
            '10.176.50.15:32777', # 
            '10.176.50.21:32772', #
            '10.176.50.26:49182', # 
            '10.176.50.18:49178', # 
            # '10.176.50.11:49174',
            # '10.176.50.37:32770',
        ]  
      self.begin_port = 5000

    

        
    def get_next_host(self, id):       
      return self.hosts[id % len(self.hosts)]

    def genlankeys(self, host, port):
      try:
        c = Connection(host, user=self.user, connect_kwargs=self.connect)
        res = c.run(CommandMaker.genlankeys(port), hide=True) # res.stdout is a str
        lines = res.stdout.split('\n')
        # print(lines)
        Pubkey = lines[0]
        Prvkey = lines[1]
        return Pubkey, Prvkey
      except (GroupException, ExecutionError) as e:
          e = FabricError(e) if isinstance(e, GroupException) else e
          raise BenchError('Failed to genlankeys', e)

    def complie_and_upload(self):
      # complie
      cmd = f'{CommandMaker.compile()}'
      subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

      # upload binary file
      hosts = self.hosts # get node ips
      print("all hosts: ", hosts)
      try:
          g = Group(*hosts, user=self.user, connect_kwargs=self.connect)
          cmd = [f'({CommandMaker.cleanup()} || true)']
          g.run(' && '.join(cmd), hide=True)
          g.put(PathMaker.binary_path(), '.')
          Print.heading(f'upload binary file for {len(hosts)} hosts')
      except (GroupException, ExecutionError) as e:
          e = FabricError(e) if isinstance(e, GroupException) else e
          raise BenchError('Failed to upload binary file', e)


    def genlanconfig_and_upload(self): 
      # # read configs.yaml
      # file = open(self.config_file_path, 'r', encoding="utf-8")
      # file_data = file.read()
      # file.close()
      # data = yaml.load(file_data, Loader=yaml.FullLoader)
      # # print(data)

      # generate tskeys
      cmd = f'{CommandMaker.gentskeys(self.shard_size)}'
      out_bytes = subprocess.check_output([cmd], shell=True, stderr=subprocess.DEVNULL)
      lines = out_bytes.decode('utf-8').split('\n')
      # print(lines) # lines[-1] = ''
      shareAsStrings = lines[:-2]
      tsPubkey = lines[-2]


      idIpMap = {}
      idPubkeymap = {}
      idPrvkeyMap = {}
      idPortMap = {}
      idNameMap ={}
      self.idHostMap = {}

      # generate keys for each node
      print("shards: ", self.shard_num , "shard size: ", self.shard_size)
      for shardId in range(self.shard_num+1):
        for nodeId in range(self.shard_size+1): 
          if shardId != self.shard_num and nodeId == self.shard_size:
            continue 
          if shardId == self.shard_num and nodeId != self.shard_size:
            continue
          id = f'{shardId}-{nodeId}' 
          if shardId == self.shard_num and nodeId == self.shard_size: # client            
            cmd = f'{CommandMaker.genlankeys(self.begin_port)}'
            out_bytes = subprocess.check_output([cmd], shell=True, stderr=subprocess.DEVNULL)
            lines = out_bytes.decode('utf-8').split('\n')
            # print(lines)
            Pubkey = lines[0]
            Prvkey = lines[1]
            idIpMap[id] = '0.0.0.0'

          else:
            next_host_id = shardId * self.shard_size + nodeId
            host = self.get_next_host(next_host_id)
            print("host for node ", id,": ", host)
            Pubkey, Prvkey = self.genlankeys(host, self.begin_port)

            self.idHostMap[id] = host

            idIpMap[id] = host.split(':')[0]

          idPubkeymap[id] = Pubkey
          idPrvkeyMap[id] = Prvkey
          idPortMap[id] = self.begin_port
          idNameMap[id] = "node-" + id
          self.begin_port+=1

      # generate config file for each node and upload
      for shardId in range(self.shard_num+1):
        for nodeId in range(self.shard_size+1): 
            if shardId != self.shard_num and nodeId == self.shard_size:
              continue 
            if shardId == self.shard_num and nodeId != self.shard_size:
              continue
          
            # generate config file
            id = f'{shardId}-{nodeId}'
            nodename = "node-"+id
            config_yaml_object = {
               'id': id,
               'nodename': nodename,
               'ip': idIpMap[id],
               'p2p_port': idPortMap[id],
               'private_key': idPrvkeyMap[id],
               'public_key': idPubkeymap[id],

               'shard_number': self.shard_num,
               'shard_size': self.shard_size,
               'id_name': idNameMap,
               'id_ip': idIpMap,
               'id_p2p_port': idPortMap,
               'id_public_key': idPubkeymap,

                'tspubkey': tsPubkey,
                'tsshare': shareAsStrings[nodeId],
                'simlatency': self.simlatency
            }
            config_file = os.path.join(self.config_path, f'{nodename}.yaml')
            file = open(config_file, 'w', encoding='utf-8')
            yaml.dump(config_yaml_object, file)
            file.close()

            if shardId == self.shard_num and nodeId == self.shard_size:
              break
            # upload config file
            target_host = self.idHostMap[id]
            try:
              c = Connection(target_host, user=self.user, connect_kwargs=self.connect)
              c.put(config_file, '.')
            except (GroupException, ExecutionError) as e:
                e = FabricError(e) if isinstance(e, GroupException) else e
                raise BenchError('Failed to genlankeys', e)


    def _kill_nodes(self, hosts, delete_logs, delete_dbs):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)
        
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        delete_dbs = CommandMaker.clean_dbs() if delete_dbs else 'true'
        cmd = [f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts,  user=self.user, connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
            g.run(delete_logs, hide=True)
            g.run(delete_dbs, hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))        
              

    def run(self):
      # complie and upload the latest binary file
      self.complie_and_upload()

      # generate config file for each node and upload
      self.genlanconfig_and_upload()


      Print.heading('Starting lan benchmark')
      # Kill any previous testbed.
      self._kill_nodes(list(self.hosts), delete_logs=True, delete_dbs=True)

      cmd = f'{CommandMaker.clean_logs()}'
      subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
      cmd = f'{CommandMaker.clean_dbs()}'
      subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)


      # run all nodes
      print("shards: ", self.shard_num , "shard size: ", self.shard_size)
      for shardId in range(self.shard_num+1):
        for nodeId in range(self.shard_size+1):
            if shardId != self.shard_num and nodeId == self.shard_size:
              continue 
            if shardId == self.shard_num and nodeId != self.shard_size:
              continue
            if shardId == self.shard_num and nodeId == self.shard_size:
              break
            id = f'{shardId}-{nodeId}'
            host = self.idHostMap[id]
            cmd = CommandMaker.run_node(
                shardid=shardId,
                nodeid=nodeId,
                cycle=self.cycle,
                rate=self.rate,
                config_path="./",
                batchsize=self.batch_size
            )
            log_file = PathMaker.log_file(shardId, nodeId)
            print(cmd)
            # print(log_file)
            t = threading.Thread(target=self._background_run, args=(host, cmd, log_file,))
            t.start()

      # run client locally
      id = f'{self.shard_num}-{self.shard_size}'
      cmd = CommandMaker.run_node(
        shardid=shardId,
        nodeid=nodeId,
        cycle=self.cycle,
        rate=self.rate,        
        config_path=self.config_path,
        batchsize=self.batch_size
      )
      log_file = PathMaker.log_file(shardId, nodeId)
      print(cmd)
      self._background_run_local(cmd, log_file)

      # run 'duration' seconds
      Print.info(f'Running benchmark ({self.duration} sec)...')
      print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), "Waiting...")
      for _ in progress_bar(range(self.duration), prefix=f'Running benchmark ({self.duration} sec):'):
          time.sleep(1)       
      print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), "Done...")
      self._kill_nodes(list(self.hosts), delete_logs=False, delete_dbs=False)

      # download logs
      chaindata, statedata = self.download_logs()

      # parse logs
      Print.info('Parsing logs and computing performance...')
      logger = LogParser.process(PathMaker.logs_path(), self.shard_num,self.shard_size, self.batch_size, self.cycle, self.rate, self.measure_rounds, self.duration, chaindata, statedata, len(self.hosts))
      print(logger.result())

      logger.print(PathMaker.result_file(
          'lan',
          self.shard_num,
          self.shard_size,
          self.batch_size,
          self.measure_rounds,
          len(self.hosts)
      ))  


    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} 2> {log_file}"'
        c = Connection(host,  user=self.user, connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _background_run_local(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def download_logs(self):  
      self.shard_size = 6
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
              id = f'{shardId}-{nodeId}'
              host = self.idHostMap[id]

              c = Connection(host, user=self.user, connect_kwargs=self.connect)
              c.get(
                  PathMaker.log_file(shardId, nodeId), 
                  local=PathMaker.log_file(shardId, nodeId)
              )
              # chaindata_cmd_MB = f'du --block-size=1M --max-depth=1 chaindata_{shardId}_{nodeId}'
              # statedata_cmd_MB = f'du --block-size=1M --max-depth=1 chaindata_{shardId}_{nodeId}'
              data_cmd_MB = f'du --block-size=1M --max-depth=1 *data_{shardId}_{nodeId}'
              res = c.run(data_cmd_MB, hide=True) # res.stdout is a str
              # print(host, '\t', res.stdout)
              lines = res.stdout.split('\n')
              node_chaindata_MB = int(lines[0].split('\t')[0])
              node_statedata_MB = int(lines[1].split('\t')[0])
              chaindata[shardId][nodeId] = node_chaindata_MB
              statedata[shardId][nodeId] = node_statedata_MB


      print("chaindata: ", chaindata)
      print("statedata: ", statedata)
      return chaindata, statedata


    