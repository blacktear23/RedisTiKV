use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex, RwLock};
use tikv_client::{RawClient, Transaction, TransactionClient};

pub mod encoding;
pub mod hash;
pub mod init;
pub mod list;
pub mod metrics;
pub mod procexec;
pub mod rawkv;
pub mod set;
pub mod string;
pub mod sync;
pub mod trans;
pub mod utils;

lazy_static! {
    pub static ref PD_ADDRS: Arc<RwLock<Option<Vec<String>>>> = Arc::new(RwLock::new(None));
    pub static ref TIKV_TRANSACTIONS: Arc<RwLock<HashMap<u64, Transaction>>> =
        Arc::new(RwLock::new(HashMap::new()));
    pub static ref TIKV_TNX_CONN_POOL: Arc<Mutex<LinkedList<TransactionClient>>> =
        Arc::new(Mutex::new(LinkedList::new()));
    // pub static ref TIKV_RAW_CLIENT: Arc<RwLock<Option<Box<RawClient>>>> =
    //    Arc::new(RwLock::new(None));
}

pub static mut TIKV_RAW_CLIENT: Option<RawClient> = None;

pub static mut INSTANCE_ID: u64 = 0;

pub fn set_instance_id(id: u64) {
    unsafe {
        INSTANCE_ID = id;
    }
}

pub fn get_instance_id() -> u64 {
    unsafe { INSTANCE_ID }
}

// Export commands
pub use crate::tikv::{
    hash::{
        tikv_hdel, tikv_hexists, tikv_hget, tikv_hget_all, tikv_hkeys, tikv_hmget, tikv_hmset,
        tikv_hset, tikv_hvals,
    },
    init::{tikv_close, tikv_connect},
    list::{
        tikv_lindex, tikv_llen, tikv_lpop, tikv_lpos, tikv_lpush, tikv_lrange, tikv_ltrim,
        tikv_rpush,
    },
    metrics::{prometheus_server, tikv_status},
    procexec::tikv_ctl,
    rawkv::{tikv_rawkv_del, tikv_rawkv_get, tikv_rawkv_put, tikv_rawkv_scan},
    set::{tikv_sadd, tikv_scard, tikv_smembers},
    string::{
        tikv_batch_get, tikv_batch_put, tikv_cached_del, tikv_cached_get, tikv_cached_put,
        tikv_del, tikv_exists, tikv_get, tikv_put, tikv_redis_set, tikv_scan,
    },
    sync::{tikv_load, tikv_scan_load, tikv_sync},
    trans::{tikv_begin, tikv_commit, tikv_rollback},
};
