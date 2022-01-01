#[macro_use]
extern crate redis_module;
#[macro_use]
extern crate lazy_static;

extern crate tokio;
extern crate reqwest;
extern crate tikv_client;

mod init;
mod utils;
mod commands;
mod hash_commands;
mod list;
mod list_commands;
mod tikv;
mod pd;
mod encoding;
mod tidb;

use init::{ tikv_init, tikv_deinit };
use commands::*;
use hash_commands::*;
use list_commands::*;
use pd::*;
use tidb::*;

// register functions
redis_module! {
    name: "tikv",
    version: 1,
    data_types: [],
    init: tikv_init,
    deinit: tikv_deinit,
    commands: [
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
        ["tikv.exists", tikv_exists, "", 0, 0, 0],
        ["tikv.hset", tikv_hset, "", 0, 0, 0],
        ["tikv.hget", tikv_hget, "", 0, 0, 0],
        ["tikv.hgetall", tikv_hget_all, "", 0, 0, 0],
        ["tikv.hkeys", tikv_hkeys, "", 0, 0, 0],
        ["tikv.hvals", tikv_hvals, "", 0, 0, 0],
        ["tikv.hmset", tikv_hmset, "", 0, 0, 0],
        ["tikv.hmget", tikv_hmget, "", 0, 0, 0],
        ["tikv.hexists", tikv_hexists, "", 0, 0, 0],
        ["tikv.lpush", tikv_lpush, "", 0, 0, 0],
        ["tikv.lrange", tikv_lrange, "", 0, 0, 0],
        ["pd.members", pd_members, "", 0, 0, 0],
        ["tidb.conn", mysql_conn, "", 0, 0, 0],
        ["tidb.query", mysql_query, "", 0, 0, 0],
        ["tidb.exec", mysql_exec, "", 0, 0, 0],
        ["tidb.close", mysql_close, "", 0, 0, 0],
        ["tidb.begin", mysql_begin, "", 0, 0, 0],
        ["tidb.commit", mysql_commit, "", 0, 0, 0],
        ["tidb.rollback", mysql_rollback, "", 0, 0, 0],
    ],
}
