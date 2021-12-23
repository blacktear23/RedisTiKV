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

use init::{ tikv_init, tikv_deinit };
use commands::*;

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
        ["tikv.del", tikv_del, "", 0, 0, 0],
        ["tikv.load", tikv_load, "", 0, 0, 0],
        ["tikv.scan", tikv_scan, "", 0, 0, 0],
    ],
}
