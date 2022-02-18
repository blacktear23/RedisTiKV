use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, Mutex, RwLock};
use tikv_client::{RawClient, Transaction, TransactionClient};

pub mod string;
pub mod connection;
pub mod errors;
pub mod asyncs;
pub mod metrics;
pub mod profiler;
pub mod hash;
pub mod list;
pub mod admin;
pub mod set;
pub mod mock;

lazy_static! {
    pub static ref PD_ADDRS: Arc<RwLock<Option<Vec<String>>>> = Arc::new(RwLock::new(None));
    pub static ref TIKV_TRANSACTIONS: Arc<RwLock<HashMap<u64, Transaction>>> =
        Arc::new(RwLock::new(HashMap::new()));
    pub static ref TIKV_TNX_CONN_POOL: Arc<Mutex<LinkedList<TransactionClient>>> =
        Arc::new(Mutex::new(LinkedList::new()));
}

pub static mut TIKV_RAW_CLIENT: Option<RawClient> = None;
pub static mut TIKV_RAW_CLIENT_2: Option<RawClient> = None;

pub static mut INSTANCE_ID: u64 = 0;

pub fn set_instance_id(id: u64) {
    unsafe {
        INSTANCE_ID = id;
    }
}

pub fn get_instance_id() -> u64 {
    unsafe { INSTANCE_ID }
}

pub use crate::commands::{
    connection::{tikv_connect, tikv_close},
    metrics::tikv_status,
    string::{
        tikv_raw_get, tikv_raw_set, tikv_raw_del, tikv_raw_setnx,
        tikv_raw_cached_del, tikv_raw_cached_get, tikv_raw_cached_set,
        tikv_raw_incr, tikv_raw_incrby, tikv_raw_decr, tikv_raw_decrby,
        tikv_raw_exists, tikv_raw_batch_get, tikv_raw_batch_set,
        tikv_raw_scan, tikv_redis_set,
    },
    profiler::{
        tikv_profile_start, tikv_profile_finish,
    },
    hash::{
        tikv_hdel, tikv_hexists, tikv_hget, tikv_hget_all, tikv_hkeys,
        tikv_hmget, tikv_hmset, tikv_hset, tikv_hvals,
    },
    list::{
        tikv_lindex, tikv_llen, tikv_lrange, tikv_ldel,
        tikv_lpop, tikv_rpop, tikv_lpush, tikv_rpush, 
    },
    admin::{
        tikv_rawkv_cfscan, tikv_rawkv_dscan, tikv_rawkv_lscan, tikv_rawkv_wscan,
    },
    set::{
        tikv_sadd, tikv_scard, tikv_smembers,
    },
    mock::{
        tikv_mock_get,
    },
};