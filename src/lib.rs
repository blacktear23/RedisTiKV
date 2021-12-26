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
mod tikv;
mod pd;

use init::{ tikv_init, tikv_deinit };
use commands::*;
use pd::*;

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
        ["pd.members", pd_members, "", 0, 0, 0],
    ],
}
