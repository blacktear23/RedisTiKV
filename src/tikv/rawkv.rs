use crate::{
    tikv::{encoding::*, metrics::*, TIKV_RAW_CLIENT},
    utils::{redis_resp, redis_resp_with_ctx, resp_int, resp_ok, sleep, tokio_spawn},
};
use redis_module::{
    BlockedClient, Context, KeyType, NextArg, RedisError, RedisResult, RedisString, RedisValue,
    ThreadSafeContext,
};
use tikv_client::{Error, Value};

pub async fn do_async_rawkv_batch_del(keys: Vec<String>) -> Result<RedisValue, Error> {
    let client = unsafe { TIKV_RAW_CLIENT.as_ref().unwrap() };
    let ekeys = encode_rawkv_keys(DataType::Raw, keys);
    let _ = client.batch_delete(ekeys).await?;
    Ok(resp_ok())
}

async fn wrap_rawkv_get(key: String) -> Result<Option<Value>, Error> {
    let client = unsafe { TIKV_RAW_CLIENT.as_ref().unwrap() };
    let mut last_err: Option<Error> = None;
    for i in 0..2000 {
        match client.get(key.clone()).await {
            Ok(val) => {
                return Ok(val);
            }
            Err(err) => {
                if let Error::RegionError(ref _rerr) = err {
                    last_err.replace(err);
                    sleep(std::cmp::min(2 + i, 500)).await;
                    continue;
                }
                if let Error::EntryNotFoundInRegionCache = err {
                    last_err.replace(err);
                    sleep(std::cmp::min(2 + i, 500)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
    match last_err {
        Some(err) => Err(err),
        None => Ok(None),
    }
}

async fn wrap_rawkv_put(key: String, val: &str) -> Result<(), Error> {
    let client = unsafe { TIKV_RAW_CLIENT.as_ref().unwrap() };
    let mut last_err: Option<Error> = None;
    for i in 0..2000 {
        match client.put(key.clone(), val.to_owned()).await {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                if let Error::RegionError(ref _rerr) = err {
                    last_err.replace(err);
                    sleep(std::cmp::min(2 + i, 500)).await;
                    continue;
                }
                if let Error::EntryNotFoundInRegionCache = err {
                    last_err.replace(err);
                    sleep(std::cmp::min(2 + i, 500)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
    match last_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

async fn wrap_rawkv_cas(key: String, prev_val: Option<Value>, val: &str) -> Result<u64, Error> {
    let client = unsafe { TIKV_RAW_CLIENT.as_ref().unwrap() };
    let mut last_err: Option<Error> = None;
    for i in 0..2000 {
        match client
            .with_atomic_for_cas()
            .compare_and_swap(key.clone(), prev_val.clone(), val.to_owned())
            .await
        {
            Ok((_, swapped)) => {
                if swapped {
                    return Ok(1);
                } else {
                    return Ok(0);
                }
            }
            Err(err) => {
                if let Error::RegionError(ref _rerr) = err {
                    last_err.replace(err);
                    sleep(std::cmp::min(2 + i, 500)).await;
                    continue;
                }
                return Err(err);
            }
        }
    }
    match last_err {
        Some(err) => Err(err),
        None => Ok(0),
    }
}

pub async fn do_async_rawkv_put_not_exists(key: &str, value: &str) -> Result<RedisValue, Error> {
    let ekey = encode_rawkv_key(DataType::Raw, key);
    let val = wrap_rawkv_cas(ekey, None.into(), value).await?;
    Ok(RedisValue::Integer(val as i64))
}

pub async fn do_async_rawkv_get(key: &str) -> Result<RedisValue, Error> {
    let ekey = encode_rawkv_key(DataType::Raw, key);
    let val = wrap_rawkv_get(ekey).await?;
    Ok(val.into())
}

pub async fn do_async_rawkv_put(key: &str, val: &str) -> Result<RedisValue, Error> {
    let ekey = encode_rawkv_key(DataType::Raw, key);
    let _ = wrap_rawkv_put(ekey, val).await?;
    Ok(resp_ok())
}

pub async fn do_async_rawkv_scan(prefix: &str, limit: u64) -> Result<RedisValue, Error> {
    let client = unsafe { TIKV_RAW_CLIENT.as_ref().unwrap() };
    let range = encode_rawkv_key(DataType::Raw, prefix)..encode_rawkv_endkey(DataType::Raw);
    let result = client.scan(range, limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                decode_rawkv_key(Into::<Vec<u8>>::into(p.key().to_owned())),
                Into::<Vec<u8>>::into(p.value().clone()),
            ])
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_rawkv_scan_range(
    start_key: &str,
    end_key: &str,
    limit: u64,
) -> Result<RedisValue, Error> {
    let client = unsafe { TIKV_RAW_CLIENT.as_ref().unwrap() };
    let range =
        encode_rawkv_key(DataType::Raw, start_key)..encode_rawkv_key(DataType::Raw, end_key);
    let result = client.scan(range, limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                decode_key(Into::<Vec<u8>>::into(p.key().to_owned())),
                Into::<Vec<u8>>::into(p.value().to_owned()),
            ])
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_rawkv_cached_get(
    ctx: &ThreadSafeContext<BlockedClient>,
    key: String,
) -> Result<RedisValue, Error> {
    let ekey = encode_rawkv_key(DataType::Raw, &key);
    let value = wrap_rawkv_get(ekey).await?;
    if value.is_none() {
        return Ok(RedisValue::Null);
    }

    let data = String::from_utf8(value.clone().unwrap());
    if data.is_ok() {
        match ctx.lock().call("TIKV.REDIS_SET", &[&key, &data.unwrap()]) {
            Err(err) => {
                return Err(Error::StringError(err.to_string()));
            }
            _ => {}
        }
    }
    match value {
        Some(val) => Ok(val.into()),
        _ => Ok(RedisValue::Null),
    }
}

pub async fn do_async_rawkv_cached_put(
    ctx: &ThreadSafeContext<BlockedClient>,
    key: String,
    val: &str,
) -> Result<RedisValue, Error> {
    let ekey = encode_rawkv_key(DataType::Raw, &key.clone());
    let _ = wrap_rawkv_put(ekey, val).await?;
    match ctx.lock().call("TIKV.REDIS_SET", &[&key.clone(), val]) {
        Err(err) => {
            return Err(Error::StringError(err.to_string()));
        }
        _ => {}
    };
    Ok(resp_int(1))
}

pub async fn do_async_rawkv_incr(key: String, inc: bool, step: i64) -> Result<RedisValue, Error> {
    let ekey = encode_rawkv_key(DataType::Raw, &key.clone());
    let mut new_int: i64 = 0;
    let mut swapped = false;
    for i in 0..2000 {
        let prev: Option<Vec<u8>>;
        let prev_int: i64;
        match wrap_rawkv_get(ekey.clone()).await? {
            Some(val) => match String::from_utf8_lossy(&val).parse::<i64>() {
                Ok(ival) => {
                    prev_int = ival;
                    prev = Some(val.clone());
                }
                Err(err) => {
                    return Err(Error::StringError(err.to_string()));
                }
            },
            None => {
                prev = None;
                prev_int = 0;
            }
        }
        if inc {
            new_int = prev_int + step;
        } else {
            new_int = prev_int - step;
        }
        let new_val = new_int.to_string();
        let ret = wrap_rawkv_cas(ekey.clone(), prev, &new_val).await?;
        if ret == 1 {
            swapped = true;
            break;
        }
        sleep(std::cmp::min(i, 200)).await;
    }
    if !swapped {
        Err(Error::StringError(String::from("Cannot swapped")))
    } else {
        Ok(resp_int(new_int))
    }
}

pub fn tikv_rawkv_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["get"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_get(key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_put(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["set"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_put(key, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_del(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["del"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let keys: Vec<String> = args.into_iter().skip(1).map(|s| s.to_string()).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_batch_del(keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_scan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["scan"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
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
            let res = do_async_rawkv_scan(start_key, limit).await;
            redis_resp(blocked_client, res);
        } else {
            let res = do_async_rawkv_scan_range(start_key, end_key, limit).await;
            redis_resp(blocked_client, res);
        }
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_setnx(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["setnx"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_put_not_exists(key, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_cached_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["get"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    // Try to get from redis
    let rkey = ctx.open_key(&key);
    if rkey.key_type() == KeyType::String {
        match rkey.read()? {
            Some(val) => {
                return Ok(val.into());
            }
            _ => {}
        }
    }
    let blocked_client = ctx.block_client();
    let skey = key.to_string_lossy();
    tokio_spawn(async move {
        let tctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let res = do_async_rawkv_cached_get(&tctx, skey).await;
        redis_resp_with_ctx(&tctx, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_cached_put(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["set"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let value = args.next_str()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let tctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let res = do_async_rawkv_cached_put(&tctx, key.to_string(), value).await;
        redis_resp_with_ctx(&tctx, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_cached_del(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["del"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let args = args.into_iter().skip(1);
    let mut keys: Vec<String> = Vec::new();
    args.for_each(|s| {
        keys.push(s.to_string());
        let rkey = ctx.open_key_writable(&s);
        match rkey.delete() {
            Err(err) => {
                ctx.log_notice(&format!(
                    "Delete Redis Key {} got error: {}",
                    s.to_string(),
                    err.to_string()
                ));
            }
            _ => {}
        }
    });
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_batch_del(keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_incr(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["incr"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_incr(key.to_owned(), true, 1).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_decr(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["decr"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_incr(key.to_owned(), false, 1).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_incrby(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["incr"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let step = args.next_i64()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_incr(key.to_owned(), true, step).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_decrby(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["decr"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let step = args.next_i64()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_incr(key.to_owned(), false, step).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
