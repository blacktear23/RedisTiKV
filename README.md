# RedisTikvPoc
[POC] Redis Module for TiKV

This is a POC repositority. This Redis Module will add branch new Redis commands to operate TiKV data.

After build the module you can use Redis's `MODULE LOAD` command load it.

## Build

```
> cargo build
```

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
```

## Commands

* tikv.conn [PDSERVERADDR] [PDSERVERADDR] ... : connect to TiKV cluster, PDSERVERADDR is optional default is 127.0.0.1:2379
* tikv.set [KEY] [VALUE]: put a Key-Value pair into TiKV cluster.
* tikv.get [KEY]: read a key's value from TiKV cluster.
* tikv.del [KEY1] [KEY2] ...: delete keys from TiKV cluster.
* tikv.load [KEY]: read a key's value from TiKV cluster and use SET command save the key-value pair into Redis memory.
* tikv.scan [STARTKEY] [ENDKEY] [LIMIT]: scan TiKV cluster data's using given range `STARTKEY` to `ENDKEY` and return `LIMIT` rows. If `ENDKEY` is ignored the range is from `STARTKEY` to end.
* tikv.delrange [STARTKEY] [ENDKEY]: use delete\_range API to delete many key's from TiKV cluster.
* tikv.mget [KEY1] [KEY2] ...: same as Redis MGET.
* tikv.mset [KEY1] [VALUE1] [KEY2] [VALUE2] ...: same as Redis MSET.
* pd.members [PDSERVERADDR]: request PD to get cluster members data.

## Benchmark

In `bench` folder it contains a golang written program to do some basic performance test. As a result, read or write data from TiKV cluster will always slower than Redis SET and GET command.

As the implementation of Redis Module, it using block\_client api to make it concurrency ready. So **ONE** operate latency will not get the top performance. And this is why benchmark for `tikv` serise commands is using worker pool to test in concurrency mode. As we can see if more workers in pool the better performance gets.

## Replace System Commands

Change the `redis.conf` file, use `rename-command` configuration to change system command name to other name, then change the `src/lib.rs` file add replaced command into redis module. 

For example:

#### redis.conf

```
rename-command SET OSET
rename-command GET OGET

loadmodule /usr/local/lib/libredistikv.so
```

#### src/lib.rs

```
// register functions
redis_module! {
    name: "tikv",
    version: 1,
    data_types: [],
    init: tikv_init,
    deinit: tikv_deinit,
    commands: [
        ["tikv.mul", curl_mul, "", 0, 0, 0],
        ["tikv.echo", curl_echo, "", 0, 0, 0],
        ["tikv.curl", async_curl, "", 0, 0, 0],
        ["tikv.tcurl", thread_curl, "", 0, 0, 0],
        ["tikv.conn", tikv_connect, "", 0, 0, 0],
        ["tikv.get", tikv_get, "", 0, 0, 0],
        ["tikv.put", tikv_put, "", 0, 0, 0],
        ["tikv.set", tikv_put, "", 0, 0, 0],
        ["tikv.del", tikv_del, "", 0, 0, 0],
        ["tikv.delrange", tikv_del_range, "", 0, 0, 0],
        ["tikv.load", tikv_load, "", 0, 0, 0],
        ["tikv.scan", tikv_scan, "", 0, 0, 0],
        ["tikv.close", tikv_close, "", 0, 0, 0],
        ["tikv.mget", tikv_batch_get, "", 0, 0, 0],
        ["tikv.mput", tikv_batch_put, "", 0, 0, 0],
        ["tikv.mset", tikv_batch_put, "", 0, 0, 0],
        ["get", tikv_get, "", 0, 0, 0],
        ["set", tikv_put, "", 0, 0, 0],
    ],
}
```

Then the `GET` and `SET` command is replaced by `TIKV.GET` and `TIKV.SET`.
