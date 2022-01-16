use crate::{
    tikv::{
        encoding::*,
        metrics::{REQUEST_CMD_COUNTER, REQUEST_COUNTER},
        string::do_async_batch_put,
        utils::*,
    },
    utils::{get_client_id, redis_resp, resp_int, resp_ok, tokio_spawn},
};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use std::collections::HashMap;
use tikv_client::{Error, KvPair};

pub async fn do_async_hget(cid: u64, key: &str, field: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let value = txn.get(encode_hash_key(key, field)).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(value.into())
}

pub async fn do_async_hput(
    cid: u64,
    key: &str,
    field: &str,
    val: &str,
) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let ekey = encode_hash_key(key, field);
    let _ = wrap_put(&mut txn, &ekey, val).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_hscan(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = txn.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    let _ = result.into_iter().for_each(|p| {
        values.push(decode_hash_field(
            Into::<Vec<u8>>::into(p.key().to_owned()),
            key,
        ));
        values.push(Into::<Vec<u8>>::into(p.value().to_owned()));
    });
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_batch_hget(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let result = wrap_batch_get(&mut txn, keys.clone()).await?;
    let mut kvret: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    result.into_iter().for_each(|p| {
        let key = Into::<Vec<u8>>::into(p.key().to_owned());
        let value = Into::<Vec<u8>>::into(p.value().to_owned());
        kvret.insert(key, value);
    });
    let values: Vec<_> = keys
        .into_iter()
        .map(|k| {
            let data = kvret.get::<Vec<u8>>(&k.into());
            match data {
                Some(val) => Into::<TiKVValue>::into(val.to_owned()),
                None => TiKVValue::Null,
            }
        })
        .collect();
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_batch_hdel(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let mut count = 0;
    for i in 0..keys.len() {
        let key = keys[i].to_owned();
        txn.delete(key).await?;
        count += 1;
    }
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_int(count))
}

pub async fn do_async_hscan_fields(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = txn.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter().for_each(|p| {
        values.push(decode_hash_field(
            Into::<Vec<u8>>::into(p.key().to_owned()),
            key,
        ));
    });
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_hscan_values(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = txn.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter().for_each(|p| {
        values.push(Into::<Vec<u8>>::into(p.value().to_owned()));
    });
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_hexists(cid: u64, key: &str, field: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let result = wrap_batch_get(&mut txn, vec![encode_hash_key(key, field)]).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(RedisValue::Integer(result.len() as i64))
}

pub fn tikv_hset(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hset"]).inc();
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hput(cid, key, field, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hget"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hget(cid, key, field).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hget_all(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hgetall"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hmset(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hmset"]).inc();
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let num_kvs = args.len() - 2;
    if num_kvs % 2 != 0 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut kvs: Vec<KvPair> = Vec::new();
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    for _i in 0..num_kvs / 2 {
        let field = args.next_str()?;
        let value = args.next_str()?;
        let kv = KvPair::from((encode_hash_key(key, field).to_owned(), value.to_owned()));
        kvs.push(kv);
    }
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_put(cid, kvs).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hmget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hmget"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let fields: Vec<String> = args.map(|s| encode_hash_key(key, &s.to_string())).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_hget(cid, fields).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hkeys(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hkeys"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan_fields(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hvals(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hvals"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan_values(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hexists(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hexists"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hexists(cid, key, field).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hdel(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["hdel"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let fields: Vec<String> = args.map(|s| encode_hash_key(key, &s.to_string())).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_hdel(cid, fields).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
