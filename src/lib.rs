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
        // Debug Usage Commands
        ["tikv.scan", tikv_raw_scan, "", 0, 0, 0],
    ],
}
