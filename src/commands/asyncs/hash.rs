use std::collections::HashMap;

use redis_module::RedisValue;
use tikv_client::{Key, Value};
use crate::{
    encoding::{KeyEncoder, KeyDecoder}, commands::errors::AsyncResult,
    utils::resp_int,
};
use super::get_client;

pub async fn do_async_hget(key: &str, field: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_hash(key, field);
    let value = client.get(ekey).await?;
    Ok(value.into())
}

pub async fn do_async_hput(
    key: &str,
    field: &str,
    val: &str,
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_hash(key, field);
    let _ = client.put(ekey, val).await?;
    Ok(resp_int(1))
}

pub async fn do_async_hscan(key: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let decoder = KeyDecoder::new();
    let range = encoder.encode_hash_start(key)..encoder.encode_hash_end(key);
    let result = client.scan(range.into(), 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    let _ = result.into_iter().for_each(|p| {
        values.push(decoder.decode_hash_field(p.key().to_owned(), key));
        values.push(Into::<Vec<u8>>::into(p.value().to_owned()));
    });
    Ok(values.into())
}

pub async fn do_async_batch_hget(keys: Vec<Key>) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let result = client.batch_get(keys.clone()).await?;
    let ret: HashMap<Key, Value> = result
        .into_iter()
        .map(|pair| (pair.0, pair.1))
        .collect();
    let values: Vec<RedisValue> = keys
        .into_iter()
        .map(|k| {
            let data = ret.get(Into::<Key>::into(k).as_ref());
            match data {
                Some(val) => val.to_owned().into(),
                None => RedisValue::Null,
            }
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_batch_hdel(keys: Vec<Key>) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let num_keys = keys.len();
    let _ = client.batch_delete(keys).await?;
    Ok(resp_int(num_keys as i64))
}

pub async fn do_async_hscan_fields(key: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let decoder = KeyDecoder::new();
    let range = encoder.encode_hash_start(key)..encoder.encode_hash_end(key);
    let result = client.scan(range.into(), 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter().for_each(|p| {
        values.push(decoder.decode_hash_field(
            p.key().to_owned(),
            key,
        ));
    });
    Ok(values.into())
}

pub async fn do_async_hscan_values(key: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let range = encoder.encode_hash_start(key)..encoder.encode_hash_end(key);
    let result = client.scan(range.into(), 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter().for_each(|p| {
        values.push(Into::<Vec<u8>>::into(p.value().to_owned()));
    });
    Ok(values.into())
}

pub async fn do_async_hexists(key: &str, field: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let result = client.batch_get(vec![encoder.encode_hash(key, field)]).await?;
    Ok(resp_int(result.len() as i64))
}