use tikv_client::{TransactionClient, Transaction};
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, RwLock, Mutex};

pub mod encoding;
pub mod hash;
pub mod init;
pub mod list;
pub mod set;
pub mod string;
pub mod utils;
pub mod trans;
pub mod sync;
pub mod procexec;
pub mod metrics;

lazy_static! {
    pub static ref PD_ADDRS: Arc<RwLock<Option<Vec<String>>>> = Arc::new(RwLock::new(None));
    pub static ref TIKV_TRANSACTIONS: Arc<RwLock<HashMap<u64, Transaction>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref TIKV_TNX_CONN_POOL: Arc<Mutex<LinkedList<TransactionClient>>> = Arc::new(Mutex::new(LinkedList::new()));
}

pub static mut INSTANCE_ID: u64 = 0;

pub fn set_instance_id(id: u64) {
    unsafe {
        INSTANCE_ID = id;
    }
}

pub fn get_instance_id() -> u64 {
    unsafe {INSTANCE_ID}
}

// Export commands
pub use crate::tikv::{
    init::{
        tikv_connect, tikv_close,
    },
    trans::{
        tikv_begin, tikv_commit, tikv_rollback,
    },
    string::{
        tikv_get, tikv_put, tikv_batch_get, tikv_batch_put,
        tikv_del, tikv_exists, tikv_scan,
        tikv_cached_get, tikv_redis_set, tikv_cached_put, tikv_cached_del,
    },
    sync::{
        tikv_load, tikv_scan_load, tikv_sync,
    },
    hash::{
        tikv_hget, tikv_hset, tikv_hmset, tikv_hmget,
        tikv_hkeys, tikv_hvals, tikv_hexists, tikv_hget_all,
        tikv_hdel,
    },
    list::{
        tikv_lpush, tikv_rpush, tikv_lrange, tikv_lpop,
        tikv_ltrim, tikv_llen, tikv_lpos, tikv_lindex,
    },
    set::{
        tikv_sadd, tikv_scard, tikv_smembers,
    },
    procexec::{
        tikv_ctl,
    },
    metrics::{
        tikv_status, prometheus_server,
    },
};
