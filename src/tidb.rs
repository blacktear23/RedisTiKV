use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString};
use crate::utils::{ redis_resp, tokio_spawn };
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use mysql_async::{Pool, Opts, Error, Value, Params, TxOpts, Transaction};
use mysql_async::prelude::*;
use crate::tikv::{resp_ok, resp_sstr};
use std::borrow::Cow;
use crate::utils::get_client_id;

lazy_static! {
    pub static ref GLOBAL_MYSQL_POOL: Arc<RwLock<Option<Box<Pool>>>> = Arc::new(RwLock::new(None));
    pub static ref MYSQL_TRANSACTIONS: Arc<RwLock<HashMap<u64, Transaction<'static>>>> = Arc::new(RwLock::new(HashMap::new()));
}

fn has_transaction(cid: u64) -> bool {
    MYSQL_TRANSACTIONS.read().unwrap().contains_key(&cid)
}

fn put_transaction(cid: u64, txn: Transaction<'static>) {
    MYSQL_TRANSACTIONS.write().unwrap().insert(cid, txn);
}

fn get_transaction_conn(cid: u64) -> Transaction<'static> {
    MYSQL_TRANSACTIONS.write().unwrap().remove(&cid).unwrap()
}

pub async fn do_async_mysql_connect(url: &str) -> Result<RedisValue, Error> {
    let opts = Opts::try_from(url)?;
    let pool = Pool::new(opts);
    GLOBAL_MYSQL_POOL.write().unwrap().replace(Box::new(pool));
    Ok(resp_ok())
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

pub async fn do_async_mysql_exec(cid: u64, sql: &str, query: bool) -> Result<RedisValue, Error> {
    let mut values: Vec<Vec<MySQLValue>> = Vec::new();
    if has_transaction(cid) {
        let mut txn = get_transaction_conn(cid);
        let mut result = txn.exec_iter(sql, Params::Empty).await?;
        if query {
            result.for_each(|row| {
                let mut cols_data: Vec<MySQLValue> = Vec::new();
                for i in 0..row.len() {
                    let col_val = &row[i];
                    cols_data.push(Into::<MySQLValue>::into(col_val.to_owned()));
                }
                values.push(cols_data.into());
            }).await?;
            put_transaction(cid, txn);
            return Ok(Into::<RedisValue>::into(values))
        } else {
            let affect_rows = result.affected_rows();
            put_transaction(cid, txn);
            return Ok((affect_rows as i64).into())
        }
    } else {
        let pool = get_pool()?;
        let mut conn = pool.get_conn().await?;
        let mut result = conn.exec_iter(sql, Params::Empty).await?;
        result.for_each(|row| {
            let mut cols_data: Vec<MySQLValue> = Vec::new();
            for i in 0..row.len() {
                let col_val = &row[i];
                cols_data.push(Into::<MySQLValue>::into(col_val.to_owned()));
            }
            values.push(cols_data.into());
        }).await?;
        if query {
            result.for_each(|row| {
                let mut cols_data: Vec<MySQLValue> = Vec::new();
                for i in 0..row.len() {
                    let col_val = &row[i];
                    cols_data.push(Into::<MySQLValue>::into(col_val.to_owned()));
                }
                values.push(cols_data.into());
            }).await?;
            drop(conn);
            return Ok(Into::<RedisValue>::into(values))
        } else {
            let affect_rows = result.affected_rows();
            drop(conn);
            return Ok((affect_rows as i64).into())
        }
    }
}

pub async fn do_async_mysql_begin(cid: u64) -> Result<RedisValue, Error> {
    if has_transaction(cid) {
        return Err(Error::Other(Cow::Owned(String::from("Transaction Already Started"))));
    }
    let pool = get_pool()?;
    let txn = pool.start_transaction(TxOpts::default()).await?;
    put_transaction(cid, txn);
    Ok(resp_sstr("Transaction Start"))
}

pub async fn do_async_mysql_commit(cid: u64) -> Result<RedisValue, Error> {
    if !has_transaction(cid) {
        return Err(Error::Other(Cow::Owned(String::from("No Transaction Started"))));
    }
    let txn = get_transaction_conn(cid);
    let _ = txn.commit().await?;
    Ok(resp_sstr("Transaction Commited"))
}

pub async fn do_async_mysql_rollback(cid: u64) -> Result<RedisValue, Error> {
    if !has_transaction(cid) {
        return Err(Error::Other(Cow::Owned(String::from("No Transaction Started"))));
    }
    let txn = get_transaction_conn(cid);
    let _ = txn.rollback().await?;
    Ok(resp_sstr("Transaction Rollbacked"))
}

pub async fn do_async_close() -> Result<RedisValue, Error> {
    let pool = get_pool()?;
    *GLOBAL_MYSQL_POOL.write().unwrap() = None;
    pool.disconnect().await?;
    Ok(resp_sstr("Closed"))
}

pub fn mysql_begin(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let cid = get_client_id(ctx);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mysql_begin(cid).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn mysql_commit(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let cid = get_client_id(ctx);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mysql_commit(cid).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn mysql_rollback(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let cid = get_client_id(ctx);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mysql_rollback(cid).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn mysql_query(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let sql = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mysql_exec(cid, sql, true).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn mysql_conn(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let url = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mysql_connect(url).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn mysql_exec(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let sql = args.next_str()?;
    let blocked_client = ctx.block_client();
    let cid = get_client_id(ctx);
    tokio_spawn(async move {
        let res = do_async_mysql_exec(cid, sql, false).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn mysql_close(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_close().await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}