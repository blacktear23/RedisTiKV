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

mod init;
mod pd;
mod tidb;
mod tikv;
mod utils;

use crate::pd::*;
use crate::tidb::*;
use crate::tikv::*;
use init::{tikv_deinit, tikv_init};

// register functions
redis_module! {
    name: "tikv",
    version: 1,
    data_types: [],
    init: tikv_init,
    deinit: tikv_deinit,
    commands: [
        // TiKV commands
        ["tikv.conn", tikv_connect, "", 0, 0, 0],
        ["tikv.close", tikv_close, "", 0, 0, 0],
        ["tikv.status", tikv_status, "", 0, 0, 0],
        // TiKV transaction
        ["tikv.begin", tikv_begin, "", 0, 0, 0],
        ["tikv.commit", tikv_commit, "", 0, 0, 0],
        ["tikv.rollback", tikv_rollback, "", 0, 0, 0],
        // TiKV string series
        ["tikv.get", tikv_get, "", 0, 0, 0],
        ["tikv.put", tikv_put, "", 0, 0, 0],
        ["tikv.set", tikv_put, "", 0, 0, 0],
        ["tikv.del", tikv_del, "", 0, 0, 0],
        ["tikv.scan", tikv_scan, "", 0, 0, 0],
        ["tikv.mget", tikv_batch_get, "", 0, 0, 0],
        ["tikv.mput", tikv_batch_put, "", 0, 0, 0],
        ["tikv.mset", tikv_batch_put, "", 0, 0, 0],
        ["tikv.exists", tikv_exists, "", 0, 0, 0],
        ["tikv.cget", tikv_cached_get, "", 0, 0, 0],
        ["tikv.cput", tikv_cached_put, "", 0, 0, 0],
        ["tikv.cset", tikv_cached_put, "", 0, 0, 0],
        ["tikv.cdel", tikv_cached_del, "", 0, 0, 0],
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
        ["tikv.ltrim", tikv_ltrim, "", 0, 0, 0],
        ["tikv.lpop", tikv_lpop, "", 0, 0, 0],
        ["tikv.lpos", tikv_lpos, "", 0, 0, 0],
        ["tikv.lindex", tikv_lindex, "", 0, 0, 0],
        // TiKV set series
        ["tikv.sadd", tikv_sadd, "", 0, 0, 0],
        ["tikv.scard", tikv_scard, "", 0, 0, 0],
        ["tikv.smembers", tikv_smembers, "", 0, 0, 0],
        // TiKV load and sync
        ["tikv.load", tikv_load, "", 0, 0, 0],
        ["tikv.scanload", tikv_scan_load, "", 0, 0, 0],
        ["tikv.sync", tikv_sync, "", 0, 0, 0],
        // TiKV rawkv
        ["tikv.rget", tikv_rawkv_get, "", 0, 0, 0],
        ["tikv.rput", tikv_rawkv_put, "", 0, 0, 0],
        ["tikv.rset", tikv_rawkv_put, "", 0, 0, 0],
        ["tikv.rdel", tikv_rawkv_del, "", 0, 0, 0],
        ["tikv.rscan", tikv_rawkv_scan, "", 0, 0, 0],
        ["tikv.rsetnx", tikv_rawkv_setnx, "", 0, 0, 0],
        ["tikv.rcset", tikv_rawkv_cached_put, "", 0, 0, 0],
        ["tikv.rcget", tikv_rawkv_cached_get, "", 0, 0, 0],
        ["tikv.rcdel", tikv_rawkv_cached_del, "", 0, 0, 0],
        ["tikv.rincr", tikv_rawkv_incr, "", 0, 0, 0],
        ["tikv.rdecr", tikv_rawkv_decr, "", 0, 0, 0],
        ["tikv.rincrby", tikv_rawkv_incrby, "", 0, 0, 0],
        ["tikv.rdecrby", tikv_rawkv_decrby, "", 0, 0, 0],
        // TiKV rawkv admin
        ["tikv.dscan", tikv_rawkv_dscan, "", 0, 0, 0],
        ["tikv.lscan", tikv_rawkv_lscan, "", 0, 0, 0],
        ["tikv.wscan", tikv_rawkv_wscan, "", 0, 0, 0],
        ["tikv.gc", tikv_gc, "", 0, 0, 0],
        // PD commands
        ["pd.apiget", pd_apiget, "", 0, 0, 0],
        ["pd.apipost", pd_apipost, "", 0, 0, 0],
        ["pd.apidelete", pd_apidelete, "", 0, 0, 0],
        ["pd.members", pd_members, "", 0, 0, 0],
        ["pd.stores", pd_stores, "", 0, 0, 0],
        ["pd.regions", pd_regions, "", 0, 0, 0],
        // TiDB commands
        ["tidb.conn", mysql_conn, "", 0, 0, 0],
        ["tidb.query", mysql_query, "", 0, 0, 0],
        ["tidb.exec", mysql_exec, "", 0, 0, 0],
        ["tidb.close", mysql_close, "", 0, 0, 0],
        ["tidb.begin", mysql_begin, "", 0, 0, 0],
        ["tidb.commit", mysql_commit, "", 0, 0, 0],
        ["tidb.rollback", mysql_rollback, "", 0, 0, 0],
        ["tidb.api", tidb_api, "", 0, 0, 0],
    ],
}
