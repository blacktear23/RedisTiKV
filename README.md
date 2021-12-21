# RedisTikvPoc
[POC] Redis Module for TiKV 

This is a POC repositority.

# Build

cargo build

# Usage

```
> module load libredistikv.so
> tikv.conn 127.0.0.1:2379
> tikv.put key value
> tikv.get key
> tikv.load key
> tikv.del key
> tikv.scan prefix 10
```
