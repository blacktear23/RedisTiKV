use std::collections::HashMap;

use redis_module::{RedisValue, ThreadSafeContext, BlockedClient};
use tikv_client::{Key, Value, KvPair};
use crate::{commands::errors::{AsyncResult, RTError}, encoding::{KeyEncoder, KeyDecoder}, utils::{resp_ok, resp_int, sleep}};
use super::get_client;

pub async fn do_async_rawkv_batch_del(keys: Vec<String>) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekeys = KeyEncoder::new().encode_strings(keys);
    let _ = client.batch_delete(ekeys).await?;
    Ok(resp_ok())
}

pub async fn do_async_rawkv_put_not_exists(key: &str, value: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(key);
    let (_, swapped) = client.compare_and_swap(ekey, None.into(), value).await?;
    if swapped {
        Ok(RedisValue::Integer(1))
    } else {
        Ok(RedisValue::Integer(0))
    } 
}

pub async fn do_async_rawkv_get(key: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(key);
    let val = client.get(ekey).await?;
    Ok(val.into())
}

pub async fn do_async_rawkv_put(key: &str, val: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(key);
    let _ = client.put(ekey, val).await?;
    Ok(resp_int(1))
}

pub async fn do_async_rawkv_scan(prefix: &str, limit: u64) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let decoder = KeyDecoder::new();
    let range = encoder.encode_string(prefix)..encoder.encode_string_end();
    let result = client.scan(range.into(), limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                decoder.decode_string(p.key().to_owned()),
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
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let decoder = KeyDecoder::new();
    let range = encoder.encode_string(start_key)..encoder.encode_string(end_key);
    let result = client.scan(range.into(), limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                decoder.decode_string(p.key().to_owned()),
                Into::<Vec<u8>>::into(p.value().to_owned()),
            ])
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_rawkv_cached_get(
    ctx: &ThreadSafeContext<BlockedClient>,
    key: String,
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(&key);
    let value = client.get(ekey).await?;
    if value.is_none() {
        return Ok(RedisValue::Null);
    }

    let data = String::from_utf8(value.clone().unwrap());
    if data.is_ok() {
        match ctx.lock().call("TIKV.REDIS_SET", &[&key, &data.unwrap()]) {
            Err(err) => {
                return Err(RTError::StringError(err.to_string()));
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
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(&key.clone());
    let _ = client.put(ekey, val).await?;
    match ctx.lock().call("TIKV.REDIS_SET", &[&key.clone(), val]) {
        Err(err) => {
            return Err(RTError::StringError(err.to_string()));
        }
        _ => {}
    };
    Ok(resp_int(1))
}

pub async fn do_async_rawkv_incr(
    key: String, inc: bool, step: i64
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(&key.clone());
    let mut new_int: i64 = 0;
    let mut swapped = false;
    for i in 0..2000 {
        let prev: Option<Vec<u8>>;
        let prev_int: i64;
        match client.get(ekey.clone()).await? {
            Some(val) => match String::from_utf8_lossy(&val).parse::<i64>() {
                Ok(ival) => {
                    prev_int = ival;
                    prev = Some(val.clone());
                }
                Err(err) => {
                    return Err(RTError::StringError(err.to_string()));
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
        let (_, ret) = client.compare_and_swap(ekey.clone(), prev, &new_val).await?;
        if ret {
            swapped = true;
            break;
        }
        sleep(std::cmp::min(i, 200)).await;
    }
    if !swapped {
        Err(RTError::StringError(String::from("Cannot swapped")))
    } else {
        Ok(resp_int(new_int))
    }
}

pub async fn do_async_rawkv_exists(keys: Vec<String>) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekeys = KeyEncoder::new().encode_strings(keys);
    let result = client.batch_get(ekeys).await?;
    let num_items = result.len();
    Ok(resp_int(num_items as i64))
}

pub async fn do_async_rawkv_batch_get(keys: Vec<String>) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekeys = KeyEncoder::new().encode_strings(keys);
    let result = client.batch_get(ekeys.clone()).await?;
    let ret: HashMap<Key, Value> = result
        .into_iter()
        .map(|pair| (pair.0, pair.1))
        .collect();
    let values: Vec<RedisValue> = ekeys
        .into_iter()
        .map(|k| {
            let data = ret.get(Into::<Key>::into(k).as_ref());
            match data {
                Some(val) => Into::<RedisValue>::into(val.clone()),
                None => RedisValue::Null,
            }
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_rawkv_batch_put(kvs: Vec<KvPair>) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let num_keys = kvs.len();
    let _ = client.batch_put(kvs).await?;
    Ok(resp_int(num_keys as i64))
}