use crate::{
    utils::{redis_resp, resp_ok, get_client_id, tokio_spawn, resp_sstr},
    tikv::{
        PD_ADDRS,
        utils::*,
    },
};
use tikv_client::{Error, TransactionOptions, CheckLevel};
use redis_module::{Context, RedisError, RedisResult, RedisValue, RedisString};

pub async fn do_async_connect(addrs: Vec<String>) -> Result<RedisValue, Error> {
    PD_ADDRS.write().unwrap().replace(addrs.clone());
    Ok(resp_ok())
}

pub async fn do_async_close() -> Result<RedisValue, Error> {
    Ok(resp_sstr("Closed"))
}

pub async fn do_async_begin(cid: u64) -> Result<RedisValue, Error> {
    let _pd_addrs = get_pd_addrs()?;
    if has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from("Transaction already started")));
    }
    let conn = get_txn_client().await?;
    let txn = conn.begin_with_options(TransactionOptions::default().drop_check(CheckLevel::Warn)).await?;
    put_txn_client(conn);
    put_txn(cid, txn);
    Ok(resp_ok())
}

pub async fn do_async_commit(cid: u64) -> Result<RedisValue, Error> {
    let _ = get_pd_addrs()?;
    if !has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from("Transaction not started")));
    }
    let mut txn = get_txn(cid);
    txn.commit().await?;
    Ok(resp_ok())
}

pub async fn do_async_rollback(cid: u64) -> Result<RedisValue, Error> {
    let _ = get_pd_addrs()?;
    if !has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from("Transaction not started")));
    }
    let mut txn = get_txn(cid);
    txn.rollback().await?;
    Ok(resp_ok())
}

pub fn tikv_connect(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 1 {
        return Err(RedisError::WrongArity);
    }
    let guard = PD_ADDRS.read().unwrap();
    if guard.as_ref().is_some() {
        return Ok(resp_sstr("Already Connected"))
    }
    let mut addrs: Vec<String> = Vec::new();
    if args.len() == 1 {
        addrs.push(String::from("127.0.0.1:2379"));
    } else {
        addrs = args.into_iter().skip(1).map(|s| s.to_string()).collect();
    }

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_connect(addrs).await;
        redis_resp(blocked_client, res);
    });

    Ok(RedisValue::NoReply)
}

pub fn tikv_close(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_close().await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_begin(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let cid = get_client_id(ctx);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_begin(cid).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_commit(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let cid = get_client_id(ctx);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_commit(cid).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rollback(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let cid = get_client_id(ctx);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rollback(cid).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}