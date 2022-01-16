use crate::{
    tikv::{encoding::*, utils::*},
    utils::{get_client_id, redis_resp, redis_resp_with_ctx, resp_int, tokio_spawn},
};
use redis_module::{
    BlockedClient, Context, KeyType, NextArg, RedisError, RedisResult, RedisString, RedisValue,
    ThreadSafeContext,
};
use tikv_client::{Error, Key, Value};

fn key_to_string(key: &Key) -> Result<String, Error> {
    match String::from_utf8(decode_key(Into::<Vec<u8>>::into(key.to_owned()))) {
        Ok(val) => Ok(val),
        Err(err) => Err(Error::StringError(err.to_string())),
    }
}

fn value_to_string(val: &Value) -> Result<String, Error> {
    match String::from_utf8(val.to_owned()) {
        Ok(val) => Ok(val),
        Err(err) => Err(Error::StringError(err.to_string())),
    }
}

pub async fn do_async_load(
    ctx: &ThreadSafeContext<BlockedClient>,
    cid: u64,
    keys: Vec<&str>,
) -> Result<RedisValue, Error> {
    let mut count: usize = 0;
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    for i in 0..keys.len() {
        let key = keys[i];
        let value = txn.get(encode_key(DataType::Raw, key)).await?;
        if value.is_none() {
            continue;
        }
        let data = String::from_utf8(value.unwrap());
        if data.is_err() {
            continue;
        }
        ctx.lock().call("SET", &[key, &data.unwrap()]).unwrap();
        count += 1;
    }
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_int(count as i64))
}

pub async fn do_async_sync(
    cid: u64,
    keys: Vec<String>,
    values: Vec<String>,
) -> Result<RedisValue, Error> {
    let mut count: usize = 0;
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    for i in 0..keys.len() {
        let key = keys[i].to_owned();
        let value = values[i].to_owned();
        let _ = txn.put(encode_key(DataType::Raw, &key), value).await?;
        count += 1;
    }
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_int(count as i64))
}

pub async fn do_async_scan_load(
    ctx: &ThreadSafeContext<BlockedClient>,
    cid: u64,
    prefix: &str,
    limit: u64,
) -> Result<RedisValue, Error> {
    let mut count: usize = 0;
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_key(DataType::Raw, prefix)..encode_endkey(DataType::Raw);
    let result = txn.scan(range, limit as u32).await?;
    finish_txn(cid, txn, in_txn).await?;
    result.into_iter().for_each(|kv| {
        let key = key_to_string(kv.key());
        if key.is_err() {
            // Skip cannot decoded string
            return;
        }
        let value = value_to_string(kv.value());
        if value.is_err() {
            // Skip cannot decoded string
            return;
        }
        count += 1;
        ctx.lock()
            .call("SET", &[&key.unwrap(), &value.unwrap()])
            .unwrap();
    });
    Ok(resp_int(count as i64))
}

pub async fn do_async_scan_range_load(
    ctx: &ThreadSafeContext<BlockedClient>,
    cid: u64,
    start_key: &str,
    end_key: &str,
    limit: u64,
) -> Result<RedisValue, Error> {
    let mut count: usize = 0;
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_key(DataType::Raw, start_key)..encode_key(DataType::Raw, end_key);
    let result = txn.scan(range, limit as u32).await?;
    finish_txn(cid, txn, in_txn).await?;
    result.into_iter().for_each(|kv| {
        let key = key_to_string(kv.key());
        if key.is_err() {
            // Skip cannot decoded string
            return;
        }
        let value = value_to_string(kv.value());
        if value.is_err() {
            // Skip cannot decoded string
            return;
        }
        count += 1;
        ctx.lock()
            .call("SET", &[&key.unwrap(), &value.unwrap()])
            .unwrap();
    });
    Ok(resp_int(count as i64))
}

pub fn tikv_scan_load(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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
        let tctx = ThreadSafeContext::with_blocked_client(blocked_client);
        if num_args == 3 {
            let res = do_async_scan_load(&tctx, cid, start_key, limit).await;
            redis_resp_with_ctx(&tctx, res);
        } else {
            let res = do_async_scan_range_load(&tctx, cid, start_key, end_key, limit).await;

            redis_resp_with_ctx(&tctx, res);
        }
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_load(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let mut keys: Vec<&str> = Vec::new();
    for _i in 0..args.len() {
        let key = args.next_str()?;
        keys.push(key);
    }
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let tctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let res = do_async_load(&tctx, cid, keys).await;
        redis_resp_with_ctx(&tctx, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_sync(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let mut keys: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    for _i in 0..args.len() {
        let key = args.next_arg()?;
        let value = ctx.open_key(&key);
        if value.key_type() != KeyType::String {
            continue;
        }
        let data = value.read()?;
        if data.is_none() {
            continue;
        }
        keys.push(key.to_string());
        values.push(data.unwrap());
    }
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_sync(cid, keys, values).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
