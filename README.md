# RedisTiKV
Redis Module for TiKV

This Redis Module will add branch new Redis commands to operate TiKV data. As Redis 4.0 introduce Redis Module feature, a branch new modules is provided to enhance Redis features. So this is why we want to implements a TiKV Redis Module to let Redis Server operate TiKV directly.

Beside operate the Redis like data structure in TiKV, we can also add some command for Redis to operate other component of TiDB. For example, connect PD server's HTTP api to operate PD server; connect TiDB server to execute SQL commands.

## Build

```
> cargo build
```

**Notice:** If using GCC 11 you should add `-include /usr/include/c++/11*/limits` to `CXXFLAGS`.

After build the module you can use Redis `MODULE LOAD` command load it.

## Usage

```
> module load libredistikv.so
> tikv.conn 127.0.0.1:2379
> tikv.put key value
> tikv.get key
> tikv.load key
> tikv.del key
> tikv.scan prefix 10
```

## Commands

#### Operate TiKV
* tikv.conn [PDSERVERADDR] [PDSERVERADDR] ... : connect to TiKV cluster, PDSERVERADDR is optional default is 127.0.0.1:2379
* tikv.set [KEY] [VALUE]: put a Key-Value pair into TiKV cluster.
* tikv.get [KEY]: read a key's value from TiKV cluster.
* tikv.del [KEY1] [KEY2] ...: delete keys from TiKV cluster.
* tikv.scan [STARTKEY] [ENDKEY] [LIMIT]: scan TiKV cluster data's using given range `STARTKEY` to `ENDKEY` and return `LIMIT` rows. If `ENDKEY` is ignored the range is from `STARTKEY` to end.
* tikv.mget [KEY1] [KEY2] ...: Same as Redis MGET.
* tikv.mset [KEY1] [VALUE1] [KEY2] [VALUE2] ...: Same as Redis MSET.
* tikv.exists [KEY1] [KEY2] ...: Same as Redis EXISTS.
* tikv.setnx [KEY] [VALUE]: Set Key-Value pair data to TiKV if Key not exists. Using RawKV.
* tikv.hset [KEY] [FIELD1] [VALUE1]: Hash set.
* tikv.hget [KEY] [FIELD1]: Hash get.
* tikv.hmset [KEY] [FIELD1] [VALUE1] [FIELD2] [VALUE2] ...: Hash multi set.
* tikv.hmget [KEY] [FIELD1] [FIELD2] ...: Hash multi get.
* tikv.hgetall [KEY]: Hash get all key and value pairs.
* tikv.hkeys [KEY]: Hash get all keys.
* tikv.hvals [KEY]: Hash get all values.
* tikv.hexists [KEY] [FIELD]: Hash test field exists.
* tikv.lpush [KEY] [VALUE1] [VALUE2]...: List left push.
* tikv.rpush [KEY] [VALUE1] [VALUE2]...: List right push.
* tikv.lrange [KEY] [LEFTPOS] [RIGHTPOS]: List start index to right index values.
* tikv.lpop [KEY] [NUM]: List left pop NUM elements.
* tikv.rpop [KEY] [NUM]: List right pop NUM elements.
* tikv.lindex [KEY] [INDEX]: List get index element.
* tikv.ldel [KEY]: List key delete.
* tikv.cget [KEY]: Get key's data from Redis memory first if Null then get it from TiKV and then cache it into Redis.
* tikv.cset [KEY] [VALUE]: Put a Key-Value pair into TiKV, if successed, then put it into Redis.
* tikv.cdel [KEY1] [KEY2]..: Delete key data from Redis cache first and then delete it from TiKV.
* tikv.status: Get metrics info from RedisTiKV module.


## Module Parameters

```
module load libredistikv.so [replacesys (cache|nocache)] [execmode (async|sync)] [pdaddrs PD_ADDR1,PD_ADDR2] [instanceid INSTANCE_ID] [enablepromhttp]
```

* replacesys: replace system command with cache(or nocache) mode. If add this parameter RedisTiKV will try to add GET, SET command using TIKV.GET, TIKV.SET
* enablepromhttp: will start a HTTP server listen to `127.0.0.1:9898` for expose prometheus metrics data.
* pdaddrs: connect to TiKV with followed PD addresses when module loaded. Many address separated by `,`
* instanceid: instance id, followed with a number. It will encoded as uint64 and add to the key prefix to support multi user.
* execmode: async means execute TiKV query in async mode, sync means in block mode. Default is async mode.

## Benchmark

In `bench` folder it contains a golang written program to do some basic performance test. As a result, read or write data from TiKV cluster will always slower than Redis SET and GET command.

As the implementation of Redis Module, it using block\_client API to make it concurrency ready. So **ONE** operate latency will not get the top performance. And this is why benchmark for `tikv` serise commands is using worker pool to test in concurrency mode. As we can see if more workers in pool the better performance gets.

## Replace System Commands

Change the `redis.conf` file, use `rename-command` configuration to change system command name to other name, then add `replacesys` parameter to `MODULE LOAD` command or `loadmodule` configuration command.
For example:

#### redis.conf

```
rename-command SET OSET
rename-command GET OGET

loadmodule /usr/local/lib/libredistikv.so replacesys nocache
```

Then the `GET` and `SET` command is replaced by `TIKV.GET` and `TIKV.SET`.

#### System Commands can be Replaced

* SET
* GET
* MSET
* MGET
* EXISTS
* INCR
* DECR
* DEL

#### Lua Script Support

When Lua Script calling RedisTiKV commands, RedisTiKV will switch this commands execute mode from `async` to `sync` then Lua Script can running correctly. And if the commands executing in Lua Script it will also wait other async commands finish.

So if running Lua Script using RedisTiKV commands will block other commands executes to make sure atomic. And the performance will drop when Lua Script is running.

#### About Redis 7.0

After Redis 7.0 module will check the replaced command's name and origin name, and this will make the RedisTiKV module loading fail. We can just change `module.c` for one line code to make RedisTiKV support. `[reference](https://github.com/blacktear23/redis/commit/cb98efae7182b28486d84931a41e66034d8d799a#diff-6109c354d7e009093f811238069b581bcb9bdbfc638d7d089814031776801632L986)`

And Redis 7.0 is including a PR that can make blocked client process more faster than old version.

## About Key Encoding

If we not encode KEY, you can get and set any TiKV data. So this is very danger for using RedisModule with a TiKV which provide data for TiDB service. And without key encoding the module can not support multi data type such as Hash or List. So add a Key prefix is safe than without it and we can support more data type. The current key encoding format is:

```
x$R_[INSTANCE_ID(8Byte)]_[DATATYPE(1Byte)]_[KEY(nByte)]
```

As the description it will use `x$R_` as fixed prefix for RedisModule used data. `INSTANCE_ID` use 8 Bytes (uint64) to determin the instance, `DATATYPE` use 1 Byte to determine the data type for value. Such as Raw, Hash and etc.

Data Types may provided:

* String: String type key, used by GET, SET series commands, use char `R`
* Hash: Hash type key, used by HGET, HSET series commands, use char `H`
* List: List type key, used by LPOP, LPUSH series commands, use char `L`

**Note:** Key encoding is a draft. So it may change in future.