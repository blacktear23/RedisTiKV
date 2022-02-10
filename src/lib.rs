#[macro_use]
extern crate redis_module;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

extern crate etcd_client;
extern crate hyper;
extern crate reqwest;
extern crate serde_json;
extern crate tikv_client;
extern crate tokio;
extern crate gperftools;
extern crate thiserror;

mod init;
mod encoding;
mod utils;
mod client;
mod commands;
mod metrics;

use init::{tikv_deinit, tikv_init};
use commands::*;

// register functions
redis_module! {
    name: "tikv",
    version: 1,
    data_types: [],
    init: tikv_init,
    deinit: tikv_deinit,
    commands: [
        // TiKV Connection
        ["tikv.conn", tikv_connect, "", 0, 0, 0],
        ["tikv.close", tikv_close, "", 0, 0, 0],
        ["tikv.status", tikv_status, "", 0, 0, 0],
        // String Commands
        ["tikv.get", tikv_raw_get, "", 0, 0, 0],
        ["tikv.set", tikv_raw_set, "", 0, 0, 0],
        ["tikv.setnx", tikv_raw_setnx, "", 0, 0, 0],
        ["tikv.del", tikv_raw_del, "", 0, 0, 0],
        ["tikv.incr", tikv_raw_incr, "", 0, 0, 0],
        ["tikv.incrby", tikv_raw_incrby, "", 0, 0, 0],
        ["tikv.decr", tikv_raw_decr, "", 0, 0, 0],
        ["tikv.decrby", tikv_raw_decrby, "", 0, 0, 0],
        ["tikv.mget", tikv_raw_batch_get, "", 0, 0, 0],
        ["tikv.mset", tikv_raw_batch_set, "", 0, 0, 0],
        ["tikv.redis_set", tikv_redis_set, "", 0, 0, 0],
        // TiKV hash series
        ["tikv.hset", tikv_hset, "", 0, 0, 0],
        ["tikv.hget", tikv_hget, "", 0, 0, 0],
        ["tikv.hgetall", tikv_hget_all, "", 0, 0, 0],
        ["tikv.hkeys", tikv_hkeys, "", 0, 0, 0],
        ["tikv.hvals", tikv_hvals, "", 0, 0, 0],
        ["tikv.hmset", tikv_hmset, "", 0, 0, 0],
        ["tikv.hmget", tikv_hmget, "", 0, 0, 0],
        ["tikv.hexists", tikv_hexists, "", 0, 0, 0],
        ["tikv.hdel", tikv_hdel, "", 0, 0, 0],
        // TiKV list series
        ["tikv.lpush", tikv_lpush, "", 0, 0, 0],
        ["tikv.rpush", tikv_rpush, "", 0, 0, 0],
        ["tikv.lrange", tikv_lrange, "", 0, 0, 0],
        ["tikv.llen", tikv_llen, "", 0, 0, 0],
        ["tikv.rpop", tikv_rpop, "", 0, 0, 0],
        ["tikv.lpop", tikv_lpop, "", 0, 0, 0],
        ["tikv.lindex", tikv_lindex, "", 0, 0, 0],
        ["tikv.ldel", tikv_ldel, "", 0, 0, 0],
        // Debug Usage Commands
        ["tikv.scan", tikv_raw_scan, "", 0, 0, 0],
        ["profiler.start", tikv_profile_start, "", 0, 0, 0],
        ["profiler.finish", tikv_profile_finish, "", 0, 0, 0],
        // TiKV rawkv admin
        ["tikv.cfscan", tikv_rawkv_cfscan, "", 0, 0, 0],
        ["tikv.dscan", tikv_rawkv_dscan, "", 0, 0, 0],
        ["tikv.lscan", tikv_rawkv_lscan, "", 0, 0, 0],
        ["tikv.wscan", tikv_rawkv_wscan, "", 0, 0, 0],
    ],
}
