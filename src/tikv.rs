use crate::encoding::*;
use crate::init::{GLOBAL_CLIENT, GLOBAL_TXN_CLIENT};
use redis_module::{RedisString, RedisValue};
use std::collections::HashMap;
use tikv_client::{Error, Key, KvPair, RawClient, TransactionClient};

pub fn resp_ok() -> RedisValue {
    RedisValue::SimpleStringStatic("OK")
}

pub fn resp_sstr(val: &'static str) -> RedisValue {
    RedisValue::SimpleStringStatic(val)
}

pub fn get_client() -> Result<Box<RawClient>, Error> {
    let guard = GLOBAL_CLIENT.read().unwrap();
    match guard.as_ref() {
        Some(val) => {
            let client = val.clone();
            Ok(client)
        }
        None => Err(tikv_client::Error::StringError(String::from(
            "TiKV Not connected",
        ))),
    }
}

pub async fn do_async_connect(addrs: Vec<String>) -> Result<RedisValue, Error> {
    let client = RawClient::new(addrs.clone()).await?;
    let txn_client = TransactionClient::new(addrs.clone()).await?;
    GLOBAL_CLIENT.write().unwrap().replace(Box::new(client));
    GLOBAL_TXN_CLIENT
        .write()
        .unwrap()
        .replace(Box::new(txn_client));
    Ok(resp_ok())
}

pub async fn do_async_get(key: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let value = client.get(encode_key(DataType::Raw, key)).await?;
    Ok(value.into())
}

pub async fn do_async_hget(key: &str, field: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let value = client.get(encode_hash_key(key, field)).await?;
    Ok(value.into())
}

pub async fn do_async_get_raw(key: &str) -> Result<Vec<u8>, Error> {
    let client = get_client()?;
    let value = client.get(encode_key(DataType::Raw, key)).await?;
    Ok(value.unwrap())
}

pub async fn do_async_put(key: &str, val: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let _ = client
        .put(encode_key(DataType::Raw, key), val.to_owned())
        .await?;
    Ok(resp_ok())
}

pub async fn do_async_hput(key: &str, field: &str, val: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let _ = client
        .put(encode_hash_key(key, field), val.to_owned())
        .await?;
    Ok(resp_ok())
}

pub async fn do_async_lpush(key: &str, elements: Vec<String>) -> Result<RedisValue, Error> {
    let client = get_client()?;

    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(client.get(encoded_key.clone()).await?);

    for (pos, e) in elements.iter().enumerate() {
        let _ = client
            .put(encode_list_elem_key(key, (l - pos as i64) as u32), e.to_owned())
            .await?;
    }

    client.put(encoded_key, encode_list_meta(l - elements.len() as i64, r)).await?;
    Ok(resp_ok())
}

pub async fn do_async_lrange(key: &str, start: i32, stop: i32) -> Result<RedisValue, Error> {
    let client = get_client()?;

    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(client.get(encoded_key.clone()).await?);

    let num = match stop {
        p if p > 0 => i64::min((stop - start + 1) as i64, r - l),
        n if n < 0 => r + n as i64 - l,
        _ => 0,
    };

    let start_pos = l + start as i64;
    let end_pos = start_pos + num;

    let start_key = encode_list_elem_key(key, start_pos as u32);
    let mut end_key = encode_list_elem_key(key, end_pos as u32);
    // need to add an extra byte because `scan` of TiKV client for upper range is exclusive.
    end_key.push(0);
    let range = start_key..end_key;
    let result = client.scan(range, num as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| Vec::from(Into::<Vec<u8>>::into(p.value().clone())))
        .collect();
    Ok(values.into())
}

pub async fn do_async_rpush(key: &str, elements: Vec<String>) -> Result<RedisValue, Error> {
    let client = get_client()?;

    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(client.get(encoded_key.clone()).await?);

    for (pos, e) in elements.iter().enumerate() {
        let _ = client
            .put(encode_list_elem_key(key, (r + pos as i64) as u32), e.to_owned())
            .await?;
    }

    client.put(encoded_key, encode_list_meta(l, r + elements.len() as i64)).await?;
    Ok(resp_ok())
}

pub async fn do_async_batch_del(keys: Vec<String>) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let _ = client
        .batch_delete(encode_keys(DataType::Raw, keys))
        .await?;
    Ok(resp_ok())
}

pub async fn do_async_scan(prefix: &str, limit: u64) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = encode_key(DataType::Raw, prefix)..encode_endkey(DataType::Raw);
    let result = client.scan(range, limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                decode_key(Into::<Vec<u8>>::into(p.key().to_owned())),
                Into::<Vec<u8>>::into(p.value().clone()),
            ])
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_scan_range(
    start_key: &str,
    end_key: &str,
    limit: u64,
) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = encode_key(DataType::Raw, start_key)..encode_key(DataType::Raw, end_key);
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

pub async fn do_async_delete_range(start_key: &str, end_key: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = encode_key(DataType::Raw, start_key)..encode_key(DataType::Raw, end_key);
    let result = client.delete_range(range).await?;
    Ok(result.into())
}

pub async fn do_async_close() -> Result<RedisValue, Error> {
    let _ = get_client()?;
    *GLOBAL_CLIENT.write().unwrap() = None;

    *GLOBAL_TXN_CLIENT.write().unwrap() = None;
    Ok(resp_sstr("Closed"))
}

pub async fn do_async_batch_get(keys: Vec<String>) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let result = client
        .batch_get(
            encode_keys(DataType::Raw, keys.clone())
                .iter()
                .map(|k| Key::from(k.to_owned())),
        )
        .await?;
    let mut kvret: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    result.into_iter().for_each(|p| {
        let key = decode_key(Into::<Vec<u8>>::into(p.key().to_owned()));
        let value = Into::<Vec<u8>>::into(p.value().to_owned());
        kvret.insert(key, value);
    });
    let values: Vec<_> = keys
        .into_iter()
        .map(|k| {
            let data = kvret.get::<Vec<u8>>(&k.into());
            match data {
                Some(val) => Into::<RedisValue>::into(val.to_owned()),
                None => RedisValue::Null,
            }
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_batch_hget(keys: Vec<String>) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let result = client
        .batch_get(keys.iter().map(|k| Key::from(k.to_owned())))
        .await?;
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
                Some(val) => Into::<RedisValue>::into(val.to_owned()),
                None => RedisValue::Null,
            }
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_batch_put(kvs: Vec<KvPair>) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let _ = client.batch_put(kvs).await?;
    Ok(resp_ok())
}

pub async fn do_async_exists(keys: Vec<String>) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let result = client
        .batch_get(
            encode_keys(DataType::Raw, keys)
                .iter()
                .map(|k| Key::from(k.to_owned())),
        )
        .await?;
    Ok(RedisValue::Integer(result.len() as i64))
}

pub async fn do_async_hscan(key: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = client.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    let _ = result.into_iter().for_each(|p| {
        values.push(decode_hash_field(
            Into::<Vec<u8>>::into(p.key().to_owned()),
            key,
        ));
        values.push(Into::<Vec<u8>>::into(p.value().to_owned()));
    });
    Ok(values.into())
}

pub async fn do_async_hscan_fields(key: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = client.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter().for_each(|p| {
        values.push(decode_hash_field(
            Into::<Vec<u8>>::into(p.key().to_owned()),
            key,
        ));
    });
    Ok(values.into())
}

pub async fn do_async_hscan_values(key: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = client.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter().for_each(|p| {
        values.push(Into::<Vec<u8>>::into(p.value().to_owned()));
    });
    Ok(values.into())
}

pub async fn do_async_hexists(key: &str, field: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let result = client.batch_get(vec![encode_hash_key(key, field)]).await?;
    Ok(RedisValue::Integer(result.len() as i64))
}
