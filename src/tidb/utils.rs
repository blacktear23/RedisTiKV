use std::borrow::Cow;
use mysql_async::{Pool, Error, Value, Transaction};
use redis_module::RedisValue;
use crate::tidb::{MYSQL_TRANSACTIONS, GLOBAL_MYSQL_POOL};

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
        },
        None => Err(Error::Other(Cow::Owned(String::from("TiDB Not connected"))))
    }
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
            Value::Date(y, m, d, h, min, s, ms) => MySQLValue::String(format!("{}-{}-{} {}:{}:{}.{}", y, m, d, h, min, s, ms)),
            Value::Time(n, d, h, m, s, ms) => MySQLValue::String(format!("{} {} {}:{}:{}.{}", n, d, h, m, s, ms)),
        }
    }
}