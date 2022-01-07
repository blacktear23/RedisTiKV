use crate::{
    tikv::{encoding::*, utils::*},
    utils::{get_client_id, redis_resp, resp_int, tokio_spawn},
};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use tikv_client::Error;

pub async fn do_async_sadd(cid: u64, key: &str, members: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;

    let mut added_num: i64 = 0;
    for m in members.iter() {
        let k = encode_set_key(key, m);
        match txn.get(k.clone()).await? {
            None => {
                txn.put(k, crate::encoding::EMPTY_VALUE).await?;
                added_num += 1;
            },
            _ => (),
        }
    }
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_int(added_num))
}

pub async fn do_async_scard(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;

    let range = encode_set_key_prefix(key)..encode_set_key_prefix_end(key);
    let result = txn.scan(range, 10200).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_int(result.count() as i64))
}

pub async fn do_async_smembers(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;

    let range = encode_set_key_prefix(key)..encode_set_key_prefix_end(key);
    let result = txn.scan(range, 10200).await?;
    let values: Vec<_> = result
        .map(|p| Vec::from(decode_set_member_from_key(Into::<Vec<u8>>::into(p.key().to_owned()))))
        .collect();

    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub fn tikv_sadd(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let members = args.map(|x| x.to_string_lossy()).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_sadd(cid, key, members).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_scard(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_scard(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_smembers(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_smembers(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
