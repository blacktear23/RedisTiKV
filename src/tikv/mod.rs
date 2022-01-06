use tikv_client::{TransactionClient, Transaction};
use std::collections::{HashMap, LinkedList};
use std::sync::{Arc, RwLock, Mutex};

pub mod encoding;
pub mod hash;
pub mod init;
pub mod list;
pub mod string;
pub mod utils;
pub mod trans;

lazy_static! {
    pub static ref PD_ADDRS: Arc<RwLock<Option<Vec<String>>>> = Arc::new(RwLock::new(None));
    pub static ref TIKV_TRANSACTIONS: Arc<RwLock<HashMap<u64, Transaction>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref TIKV_TNX_CONN_POOL: Arc<Mutex<LinkedList<TransactionClient>>> = Arc::new(Mutex::new(LinkedList::new()));
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
        tikv_del, tikv_exists, tikv_scan, tikv_load,
    },
    hash::{
        tikv_hget, tikv_hset, tikv_hmset, tikv_hmget,
        tikv_hkeys, tikv_hvals, tikv_hexists, tikv_hget_all,
    },
    list::{
        tikv_lpush, tikv_rpush, tikv_lrange, tikv_lpop, tikv_ltrim, tikv_llen,
    }
};
