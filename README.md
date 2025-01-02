## Description
The repository hosts MorphDAG 1.0, a Golang implementation of an elastic DAG-based blockchain system supporting fast commitment, concurrent execution, and adaptive sharding. 
The prototype of MorphDAG 1.0 is affiliated with the National Key Research and Development (R&D) Program Project, 2021YFB2700700.


## Usage


### 1. Precondition
necessary software environment:
- installation of go

Remove any previous Go installation by deleting the /usr/local/go folder (if it exists), then extract the archive you just downloaded into /usr/local, creating a fresh Go tree in /usr/local/go:
```shell script
$ rm -rf /usr/local/go && tar -C /usr/local -xzf go1.22.1.linux-amd64.tar.gz
```

Add /usr/local/go/bin to the PATH environment variable.
```shell script
export PATH=$PATH:/usr/local/go/bin
```
- installation of python
```shell script
sudo apt-get install python3.7
```


- installation of pip
``` shell script
$ curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py  
$ sudo python get-pip.py    
```


### 2. Steps to run MorphDAG
#### 2.1 Install fabric on the work computer

- installation of fabric

Fabric is best installed via pip:
``` shell script
pip install fabric
```


#### 2.2 Login replicas without passwords 
Enable workcomputer to login in servers and clients without passwords.

Commands below are run on the *work computer*.
```shell script
# Generate the ssh keys (if not generated before)
ssh-keygen
ssh-copy-id -i ~/.ssh/id_rsa.pub $IP_ADDR_OF_EACH_SERVER
```

#### 2.3 Install Go-related modules/packages

```shell
# Enter the directory `go-MorphDAG`
go mod tidy
```

#### 2.4 local testnetwork configuration

Run MorphDAG on a local testnetwork. Parameters of the local testnetwork are defined in the file `benchmark/fabfile.py`.
The default configuration is as follows:
```python
  bench_params = {
      'shard_num': 2,
      'shard_size': 11,
      'cycle': 100,
      'rate': 2000,
      'batch_size': 300,
      'measure_rounds': 300,
      'duration': 60,
      'regenconfig': True,
    }
```

You can use the following command to start a local testnetwork
```shell
fab local
```

#### 2.5 lan testnetwork configuration

Run MorphDAG on a lan testnetwork. Parameters of the lan testnetwork are defined in the file `benchmark/fabfile.py`.
The default configuration is as follows:
```python
     bench_params = {
      'shard_num': 8,
      'shard_size': 6,
      'cycle': 50,
      'rate': 2000,
      'batch_size': 500,
      'measure_rounds': 300, 
      'duration': 200,
      'regenconfig': True,
    }
```
For node configuration such as ips and ports, you can refer to the `benchmark/lan.py` file. 



#### 2.6 Run Client

For stable testing, the client is instantiated together with the workserver. Normally, the number of client in each shard is set to 1 and its index is $n+1$ where $n$ is the number of replicas a shard contains.




