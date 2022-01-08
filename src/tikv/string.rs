use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString};
use crate::{
    utils::{redis_resp, resp_ok, get_client_id, tokio_spawn},
    tikv::{
        utils::*,
        encoding::*,
    },
};
use std::collections::HashMap;
use tikv_client::{KvPair, Error, Key, Value};

pub async fn do_async_get(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let value = txn.get(encode_key(DataType::Raw, key)).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(value.into())
}

pub async fn do_async_put(cid: u64, key: &str, val: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let ekey = encode_key(DataType::Raw, key);
    let _ = wrap_put(&mut txn, &ekey, val).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_batch_del(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let ekeys = encode_keys(DataType::Raw, keys);
    for i in 0..ekeys.len() {
        let key = ekeys[i].to_owned();
        let _ = txn.delete(key).await?;
    }
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_scan(cid: u64, prefix: &str, limit: u64) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_key(DataType::Raw, prefix)..encode_endkey(DataType::Raw);
    let result = txn.scan(range, limit as u32).await?;
    let values: Vec<_> = result.into_iter().map(|p| Vec::from([
            decode_key(Into::<Vec<u8>>::into(p.key().to_owned())),
            Into::<Vec<u8>>::into(p.value().clone())])).collect();
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_scan_range(cid: u64, start_key: &str, end_key: &str, limit: u64) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_key(DataType::Raw, start_key)..encode_key(DataType::Raw, end_key);
    let result = txn.scan(range, limit as u32).await?;
    let values: Vec<_> = result.into_iter().map(|p| Vec::from([
            decode_key(Into::<Vec<u8>>::into(p.key().to_owned())),
            Into::<Vec<u8>>::into(p.value().to_owned())])).collect();
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_batch_get(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let ekeys = encode_keys(DataType::Raw, keys);
    let result = wrap_batch_get(&mut txn, ekeys.clone()).await?;
    let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();
    let values: Vec<_> = ekeys.into_iter().map(|k| {
        let data = ret.get(Into::<Key>::into(k).as_ref());
        match data {
            Some(val) => {
                Into::<TiKVValue>::into(val.clone())
            },
            None => {
                TiKVValue::Null
            }
        }
    }).collect();
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_batch_put(cid: u64, kvs: Vec<KvPair>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    for i in 0..kvs.len() {
        let kv = kvs[i].to_owned();
        let _ = wrap_put(&mut txn,
            &String::from_utf8(kv.key().to_owned().into()).unwrap(),
            &String::from_utf8(kv.value().to_owned().into()).unwrap()).await?;
    }
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_exists(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let ekeys = encode_keys(DataType::Raw, keys);
    let result = wrap_batch_get(&mut txn, ekeys).await?;
    let num_items = result.len();
    finish_txn(cid, txn, in_txn).await?;
    Ok(RedisValue::Integer(num_items as i64))
}

pub fn tikv_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_get(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_put(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_put(cid, key, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_del(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let keys: Vec<String> = args.into_iter().skip(1).map(|s| s.to_string()).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_del(cid, keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)

}

pub fn tikv_scan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let num_args = args.len();
    let mut args = args.into_iter().skip(1);
    let start_key = args.next_str()?;
    let end_key: &str;
    if num_args > 3 {
        end_key = args.next_str()?;
    } else {
        end_key = "";
    }
    let limit = args.next_u64()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        if num_args == 3 {
            let res = do_async_scan(cid, start_key, limit).await;
            redis_resp(blocked_client, res);
        } else {
            let res = do_async_scan_range(cid, start_key, end_key, limit).await;
            redis_resp(blocked_client, res);
        }
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_batch_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let cid = get_client_id(ctx);
    let keys: Vec<String> = args.into_iter().skip(1).map(|s| s.to_string()).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_get(cid, keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_batch_put(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let num_kvs = args.len() - 1;
    if num_kvs % 2 != 0 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut kvs: Vec<KvPair> = Vec::new();
    let mut args = args.into_iter().skip(1);
    for _i in 0..num_kvs/2 {
        let key = args.next_str()?;
        let value = args.next_str()?;
        let kv = KvPair::from((encode_key(DataType::Raw, key).to_owned(), value.to_owned()));
        kvs.push(kv);
    }
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_put(cid, kvs).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_exists(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let keys: Vec<String> = args.into_iter().skip(1).map(|s| s.to_string()).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_exists(cid, keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}