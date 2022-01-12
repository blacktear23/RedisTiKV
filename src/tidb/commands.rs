use crate::tidb::GLOBAL_MYSQL_POOL;
use crate::{pd::utils::*, tidb::utils::*, utils::*};
use mysql_async::{prelude::*, Error, Opts, Params, Pool, TxOpts};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use std::borrow::Cow;

pub async fn do_async_mysql_connect(url: &str) -> Result<RedisValue, Error> {
    let opts = Opts::try_from(url)?;
    let pool = Pool::new(opts);
    GLOBAL_MYSQL_POOL.write().unwrap().replace(Box::new(pool));
    Ok(resp_ok())
}

pub async fn do_async_mysql_exec(cid: u64, sql: &str, query: bool) -> Result<RedisValue, Error> {
    let mut values: Vec<Vec<MySQLValue>> = Vec::new();
    if has_transaction(cid) {
        let mut txn = get_transaction_conn(cid);
        let mut result = txn.exec_iter(sql, Params::Empty).await?;
        if query {
            result
                .for_each(|row| {
                    let mut cols_data: Vec<MySQLValue> = Vec::new();
                    for i in 0..row.len() {
                        let col_val = &row[i];
                        cols_data.push(Into::<MySQLValue>::into(col_val.to_owned()));
                    }
                    values.push(cols_data.into());
                })
                .await?;
            put_transaction(cid, txn);
            return Ok(Into::<RedisValue>::into(values));
        } else {
            let affect_rows = result.affected_rows();
            put_transaction(cid, txn);
            return Ok((affect_rows as i64).into());
        }
    } else {
        let pool = get_pool()?;
        let mut conn = pool.get_conn().await?;
        let mut result = conn.exec_iter(sql, Params::Empty).await?;
        result
            .for_each(|row| {
                let mut cols_data: Vec<MySQLValue> = Vec::new();
                for i in 0..row.len() {
                    let col_val = &row[i];
                    cols_data.push(Into::<MySQLValue>::into(col_val.to_owned()));
                }
                values.push(cols_data.into());
            })
            .await?;
        if query {
            result
                .for_each(|row| {
                    let mut cols_data: Vec<MySQLValue> = Vec::new();
                    for i in 0..row.len() {
                        let col_val = &row[i];
                        cols_data.push(Into::<MySQLValue>::into(col_val.to_owned()));
                    }
                    values.push(cols_data.into());
                })
                .await?;
            drop(conn);
            return Ok(Into::<RedisValue>::into(values));
        } else {
            let affect_rows = result.affected_rows();
            drop(conn);
            return Ok((affect_rows as i64).into());
        }
    }
}

pub async fn do_async_mysql_begin(cid: u64) -> Result<RedisValue, Error> {
    if has_transaction(cid) {
        return Err(Error::Other(Cow::Owned(String::from(
            "Transaction Already Started",
        ))));
    }
    let pool = get_pool()?;
    let txn = pool.start_transaction(TxOpts::default()).await?;
    put_transaction(cid, txn);
    Ok(resp_sstr("Transaction Start"))
}

pub async fn do_async_mysql_commit(cid: u64) -> Result<RedisValue, Error> {
    if !has_transaction(cid) {
        return Err(Error::Other(Cow::Owned(String::from(
            "No Transaction Started",
        ))));
    }
    let txn = get_transaction_conn(cid);
    let _ = txn.commit().await?;
    Ok(resp_sstr("Transaction Commited"))
}

pub async fn do_async_mysql_rollback(cid: u64) -> Result<RedisValue, Error> {
    if !has_transaction(cid) {
        return Err(Error::Other(Cow::Owned(String::from(
            "No Transaction Started",
        ))));
    }
    let txn = get_transaction_conn(cid);
    let _ = txn.rollback().await?;
    Ok(resp_sstr("Transaction Rollbacked"))
}

pub async fn do_async_mysql_close() -> Result<RedisValue, Error> {
    let pool = get_pool()?;
    *GLOBAL_MYSQL_POOL.write().unwrap() = None;
    pool.disconnect().await?;
    Ok(resp_sstr("Closed"))
}

pub fn tidb_api(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let blocked_client = ctx.block_client();
    match args.next_str()? {
        "schema" => tokio_spawn(async move {
            let tidb_host_and_status = do_async_get_tidb_status_port().await.unwrap();
            let url = format!("http://{}/schema", tidb_host_and_status);
            let res = do_async_get(&url).await;
            redis_resp(blocked_client, res);
        }),
        _ => (),
    }

    Ok(RedisValue::NoReply)
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
    let args = args.into_iter().skip(1);
    let sql_args: Vec<String> = args.map(|i| i.to_string_lossy()).collect();
    let sql: String = sql_args.join(" ").to_string();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mysql_exec(cid, &sql, true).await;
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
    let args = args.into_iter().skip(1);
    let sql_args: Vec<String> = args.map(|i| i.to_string_lossy()).collect();
    let sql: String = sql_args.join(" ").to_string();
    let blocked_client = ctx.block_client();
    let cid = get_client_id(ctx);
    tokio_spawn(async move {
        let res = do_async_mysql_exec(cid, &sql, false).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn mysql_close(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mysql_close().await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
