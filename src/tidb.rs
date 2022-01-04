use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString};
use crate::utils::{ redis_resp, tokio_spawn };
use std::sync::{Arc, RwLock};
use mysql_async::{Pool, Opts, Error, Value, Params};
use mysql_async::prelude::*;
use crate::tikv::{resp_ok, resp_sstr};
use std::borrow::Cow;

lazy_static! {
    pub static ref GLOBAL_MYSQL_POOL: Arc<RwLock<Option<Box<Pool>>>> = Arc::new(RwLock::new(None));
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

pub async fn do_async_mysql_query(sql: &str) -> Result<RedisValue, Error> {
    let pool = get_pool()?;
    let mut conn = pool.get_conn().await?;
    let mut result = conn.exec_iter(sql, Params::Empty).await?;
    let mut values: Vec<Vec<MySQLValue>> = Vec::new();
    result.for_each(|row| {
        let mut cols_data: Vec<MySQLValue> = Vec::new();
        for i in 0..row.len() {
            let col_val = &row[i];
            cols_data.push(Into::<MySQLValue>::into(col_val.to_owned()));
        }
        values.push(cols_data.into());
    }).await?;
    drop(conn);
    Ok(Into::<RedisValue>::into(values))
}

pub async fn do_async_mysql_exec(sql: &str) -> Result<RedisValue, Error> {
    let pool = get_pool()?;
    let mut conn = pool.get_conn().await?;
    let result = conn.exec_iter(sql, Params::Empty).await?;
    Ok((result.affected_rows() as i64).into())
}

pub async fn do_async_close() -> Result<RedisValue, Error> {
    let pool = get_pool()?;
    *GLOBAL_MYSQL_POOL.write().unwrap() = None;
    pool.disconnect().await?;
    Ok(resp_sstr("Closed"))
}

pub fn mysql_query(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let sql = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mysql_query(sql).await;
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
    tokio_spawn(async move {
        let res = do_async_mysql_exec(sql).await;
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