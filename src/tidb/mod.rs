use mysql_async::{Pool, Transaction};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

pub mod commands;
pub mod utils;

lazy_static! {
    pub static ref GLOBAL_MYSQL_POOL: Arc<RwLock<Option<Box<Pool>>>> = Arc::new(RwLock::new(None));
    pub static ref MYSQL_TRANSACTIONS: Arc<RwLock<HashMap<u64, Transaction<'static>>>> =
        Arc::new(RwLock::new(HashMap::new()));
}

// Export commands
pub use crate::tidb::commands::{
    mysql_begin, mysql_close, mysql_commit, mysql_conn, mysql_exec, mysql_query, mysql_rollback,
    tidb_api,
};
