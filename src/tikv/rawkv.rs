use crate::{
    tikv::{encoding::*, metrics::*, TIKV_RAW_CLIENT},
    utils::{redis_resp, resp_ok, tokio_spawn, sleep},
};
use redis_module::{
    Context, NextArg, RedisError, RedisResult, RedisString, RedisValue,
};
use tikv_client::{Error, Value};

/*
fn get_client() -> Result<&'static Box<RawClient>, Error> {
    let guard = TIKV_RAW_CLIENT.read().unwrap();
    let client = guard.as_ref().unwrap().clone();
    return Ok(client);
}
*/

pub async fn do_async_rawkv_batch_del(keys: Vec<String>) -> Result<RedisValue, Error> {
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap()};
    let ekeys = encode_rawkv_keys(DataType::Raw, keys);
    let _ = client.batch_delete(ekeys).await?;
    Ok(resp_ok())
}

async fn wrap_rawkv_get(key: String) -> Result<Option<Value>, Error> {
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap()};
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
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap()};
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
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap()};
    let mut last_err: Option<Error> = None;
    for i in 0..2000 {
        match client.with_atomic_for_cas().compare_and_swap(key.clone(), prev_val.clone(), val.to_owned()).await {
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
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap()};
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
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap()};
    let range = encode_rawkv_key(DataType::Raw, start_key)..encode_rawkv_key(DataType::Raw, end_key);
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