use crate::pd::utils::get_pd_addr;
use crate::tidb::{GLOBAL_MYSQL_POOL, MYSQL_TRANSACTIONS};
use etcd_client::{Client, GetOptions};
use mysql_async::{Error, Pool, Transaction, Value};
use redis_module::RedisValue;
use serde_json::{Map, Value as JsonValue};
use std::borrow::Cow;

const TIDB_SERVER_INFO_PATH: &str = "/tidb/server/info";

pub fn has_transaction(cid: u64) -> bool {
    MYSQL_TRANSACTIONS.read().unwrap().contains_key(&cid)
}

pub fn put_transaction(cid: u64, txn: Transaction<'static>) {
    MYSQL_TRANSACTIONS.write().unwrap().insert(cid, txn);
}

pub fn get_transaction_conn(cid: u64) -> Transaction<'static> {
    MYSQL_TRANSACTIONS.write().unwrap().remove(&cid).unwrap()
}

pub fn get_pool() -> Result<Box<Pool>, Error> {
    let guard = GLOBAL_MYSQL_POOL.read().unwrap();
    match guard.as_ref() {
        Some(val) => {
            let pool = val.clone();
            Ok(pool)
        }
        None => Err(Error::Other(Cow::Owned(String::from("TiDB Not connected")))),
    }
}

fn parse_tidb_status_port(s: &str) -> Option<String> {
    match serde_json::from_str::<Map<String, JsonValue>>(s) {
        Ok(v) =>
        // TODO: support TLS.
        {
            Some(format!(
                "{}:{}",
                v["ip"].to_string().replace("\"", ""),
                v["status_port"]
            ))
        }
        Err(_) => None,
    }
}

pub async fn do_async_get_tidb_status_port() -> Option<String> {
    let pd_addr = get_pd_addr().unwrap();
    let mut client = Client::connect([pd_addr], None).await.unwrap();

    let resp = client
        .get(TIDB_SERVER_INFO_PATH, Some(GetOptions::new().with_prefix()))
        .await
        .unwrap();
    for kv in resp.kvs() {
        match parse_tidb_status_port(kv.value_str().unwrap()) {
            Some(s) => return Some(s),
            _ => (),
        }
    }
    None
}

#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub enum MySQLValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
}

impl From<MySQLValue> for RedisValue {
    fn from(item: MySQLValue) -> Self {
        match item {
            MySQLValue::Null => RedisValue::Null,
            MySQLValue::String(s) => RedisValue::BulkString(s),
            MySQLValue::Integer(i) => RedisValue::Integer(i),
            MySQLValue::Float(f) => RedisValue::Float(f),
        }
    }
}

impl From<Value> for MySQLValue {
    fn from(item: Value) -> Self {
        match item {
            Value::NULL => MySQLValue::Null,
            Value::Bytes(b) => MySQLValue::String(String::from_utf8_lossy(&b).to_string()),
            Value::Int(i) => MySQLValue::Integer(i as i64),
            Value::UInt(i) => MySQLValue::Integer(i as i64),
            Value::Float(f) => MySQLValue::Float(f as f64),
            Value::Double(f) => MySQLValue::Float(f as f64),
            Value::Date(y, m, d, h, min, s, ms) => {
                MySQLValue::String(format!("{}-{}-{} {}:{}:{}.{}", y, m, d, h, min, s, ms))
            }
            Value::Time(n, d, h, m, s, ms) => {
                MySQLValue::String(format!("{} {} {}:{}:{}.{}", n, d, h, m, s, ms))
            }
        }
    }
}
