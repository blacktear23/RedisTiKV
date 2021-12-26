# RedisTikvPoc
[POC] Redis Module for TiKV

This is a POC repositority. This Redis Module will add branch new Redis commands to operate TiKV data.

After build the module you can use Redis's `MODULE LOAD` command load it.

## Build

```
> cargo build
```

**Notice:** If using GCC 11 you should add `-include /usr/include/c++/11*/limits` to `CXXFLAGS`.

## Usage

```
> module load libredistikv.so
> tikv.conn 127.0.0.1:2379
> tikv.put key value
> tikv.get key
> tikv.load key
> tikv.del key
> tikv.scan prefix 10
> tikv.delrange start-key end-key
> pd.members 127.0.0.1:2379
```

## Commands

#### Operate TiKV
* tikv.conn [PDSERVERADDR] [PDSERVERADDR] ... : connect to TiKV cluster, PDSERVERADDR is optional default is 127.0.0.1:2379
* tikv.set [KEY] [VALUE]: put a Key-Value pair into TiKV cluster.
* tikv.get [KEY]: read a key's value from TiKV cluster.
* tikv.del [KEY1] [KEY2] ...: delete keys from TiKV cluster.
* tikv.load [KEY]: read a key's value from TiKV cluster and use SET command save the key-value pair into Redis memory.
* tikv.scan [STARTKEY] [ENDKEY] [LIMIT]: scan TiKV cluster data's using given range `STARTKEY` to `ENDKEY` and return `LIMIT` rows. If `ENDKEY` is ignored the range is from `STARTKEY` to end.
* tikv.delrange [STARTKEY] [ENDKEY]: use delete\_range API to delete many key's from TiKV cluster.
* tikv.mget [KEY1] [KEY2] ...: same as Redis MGET.
* tikv.mset [KEY1] [VALUE1] [KEY2] [VALUE2] ...: same as Redis MSET.

#### Get PD API data
* pd.members [PDSERVERADDR]: request PD to get cluster members data.

## Module Parameters

```
module load libredistikv.so [replacesys]
```

* replacesys: replace system command. If add this parameter RedisTiKV will try to add GET, SET command using TIKV.GET, TIKV.SET

## Benchmark

In `bench` folder it contains a golang written program to do some basic performance test. As a result, read or write data from TiKV cluster will always slower than Redis SET and GET command.

As the implementation of Redis Module, it using block\_client api to make it concurrency ready. So **ONE** operate latency will not get the top performance. And this is why benchmark for `tikv` serise commands is using worker pool to test in concurrency mode. As we can see if more workers in pool the better performance gets.

## Replace System Commands

Change the `redis.conf` file, use `rename-command` configuration to change system command name to other name, then add `replacesys` parameter to `MODULE LOAD` command or `loadmodule` configuration command.
For example:

#### redis.conf

```
rename-command SET OSET
rename-command GET OGET

loadmodule /usr/local/lib/libredistikv.so replacesys
```

Then the `GET` and `SET` command is replaced by `TIKV.GET` and `TIKV.SET`.
