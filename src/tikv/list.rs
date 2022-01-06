use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString};
use crate::{
    utils::{redis_resp, resp_ok, get_client_id, tokio_spawn},
    tikv::{
        utils::*,
        encoding::*,
    },
};
use tikv_client::{Error};

pub async fn do_async_lpush(cid: u64, key: &str, elements: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    for (pos, e) in elements.iter().enumerate() {
        let _ = txn
            .put(
                encode_list_elem_key(key, l - pos as i64 - 1),
                e.to_owned(),
            )
            .await?;
    }

    txn
        .put(encoded_key, encode_list_meta(l - elements.len() as i64, r))
        .await?;

    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_lrange(cid: u64, key: &str, start: i64, stop: i64) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    let num = match stop {
        p if p > 0 => i64::min(stop - start, r - l) + 1,
        n if n < 0 => r + n - l + 1,
        _ => 0,
    };

    let start_pos = l + start;
    let end_pos = start_pos + num;

    let start_key = encode_list_elem_key(key, start_pos);
    let end_key = encode_list_elem_key(key, end_pos);
    let range = start_key..end_key;
    let result = txn.scan(range, num as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| Vec::from(Into::<Vec<u8>>::into(p.value().clone())))
        .collect();

    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_rpush(cid: u64, key: &str, elements: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    for (pos, e) in elements.iter().enumerate() {
        let _ = txn
            .put(
                encode_list_elem_key(key, r + pos as i64),
                e.to_owned(),
            )
            .await?;
    }

    txn
        .put(encoded_key, encode_list_meta(l, r + elements.len() as i64))
        .await?;

    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}


pub fn tikv_lpush(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let len = args.len() as i64;
    let blocked_client = ctx.block_client();
    let elements = args.map(|x| x.to_string_lossy()).collect();
    ctx.log_debug(&format!("Handle tikv_lpush commands, key: {}, elements: {:?}", key, elements));
    tokio_spawn(async move {
        let res = do_async_lpush(cid, key, elements).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::Integer(len))
}

pub fn tikv_lrange(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let start = args.next_str()?.parse::<i64>().unwrap();
    let end = args.next_str()?.parse::<i64>().unwrap();
    ctx.log_debug(&format!("Handle tikv_lrange commands, key: {}, start: {}, end: {}", key, start, end));
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_lrange(cid, key, start, end).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rpush(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let len = args.len() as i64;
    let blocked_client = ctx.block_client();
    let elements = args.map(|x| x.to_string_lossy()).collect();
    ctx.log_debug(&format!("Handle tikv_lpush commands, key: {}, elements: {:?}", key, elements));
    tokio_spawn(async move {
        let res = do_async_rpush(cid, key, elements).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::Integer(len))
}