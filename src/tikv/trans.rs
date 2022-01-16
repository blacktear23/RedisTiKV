use crate::{
    tikv::utils::*,
    utils::{get_client_id, redis_resp, resp_ok, tokio_spawn},
};
use redis_module::{Context, RedisResult, RedisString, RedisValue};
use tikv_client::Error;

pub async fn do_async_begin(cid: u64) -> Result<RedisValue, Error> {
    let _pd_addrs = get_pd_addrs()?;
    if has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from(
            "Transaction already started",
        )));
    }
    let conn = get_txn_client().await?;
    let txn = conn.begin_with_options(get_transaction_option()).await?;
    put_txn_client(conn);
    put_txn(cid, txn);
    Ok(resp_ok())
}

pub async fn do_async_commit(cid: u64) -> Result<RedisValue, Error> {
    let _ = get_pd_addrs()?;
    if !has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from(
            "Transaction not started",
        )));
    }
    let mut txn = get_txn(cid);
    txn.commit().await?;
    Ok(resp_ok())
}

pub async fn do_async_rollback(cid: u64) -> Result<RedisValue, Error> {
    let _ = get_pd_addrs()?;
    if !has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from(
            "Transaction not started",
        )));
    }
    let mut txn = get_txn(cid);
    txn.rollback().await?;
    Ok(resp_ok())
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
