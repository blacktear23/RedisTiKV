use redis_module::{ RedisValue };
use tikv_client::{RawClient, Error, Key};
use crate::init::GLOBAL_CLIENT;
use std::collections::HashMap;

pub fn get_client() -> Result<Box<RawClient>, Error> {
    let guard = GLOBAL_CLIENT.read().unwrap();
    match guard.as_ref() {
        Some(val) => {
            let client = val.clone();
            Ok(client)
        },
        None => Err(tikv_client::Error::StringError(String::from("Not connected")))
    }
}

pub async fn do_async_connect(addrs: Vec<String>) -> Result<RedisValue, Error> {
    let client = RawClient::new(addrs).await?;
    GLOBAL_CLIENT.write().unwrap().replace(Box::new(client));
    Ok("OK".into())
}

pub async fn do_async_get(key: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let value = client.get(key.to_owned()).await?;
    Ok(value.into())
}

pub async fn do_async_get_raw(key: &str) -> Result<Vec<u8>, Error> {
    let client = get_client()?;
    let value = client.get(key.to_owned()).await?;
    Ok(value.unwrap())
}

pub async fn do_async_put(key: &str, val: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let value = client.put(key.to_owned(), val.to_owned()).await?;
    Ok(value.into())
}

pub async fn do_async_del(key: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let value = client.delete(key.to_owned()).await?;
    Ok(value.into())
}

pub async fn do_async_scan(prefix: &str, limit: u64) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = prefix.to_owned()..;
    let result = client.scan(range, limit as u32).await?;
    let values: Vec<_> = result.into_iter().map(|p| Vec::from([Into::<Vec<u8>>::into(p.key().clone()), Into::<Vec<u8>>::into(p.value().clone())])).collect();
    Ok(values.into())
}

pub async fn do_async_scan_range(start_key: &str, end_key: &str, limit: u64) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = start_key.to_owned()..end_key.to_owned();
    let result = client.scan(range, limit as u32).await?;
    let values: Vec<_> = result.into_iter().map(|p| Vec::from([Into::<Vec<u8>>::into(p.key().clone()), Into::<Vec<u8>>::into(p.value().clone())])).collect();
    Ok(values.into())
}

pub async fn do_async_delete_range(key_start: &str, key_end: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = key_start.to_owned()..key_end.to_owned();
    let result = client.delete_range(range).await?;
    Ok(result.into())
}

pub async fn do_async_close() -> Result<RedisValue, Error> {
    let _ = get_client()?;
    *GLOBAL_CLIENT.write().unwrap() = None;
    Ok("Closed".into())
}

pub async fn do_async_batch_get(keys: Vec<String>) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let result = client.batch_get(keys.iter().map(|k| {Key::from(k.to_owned())})).await?;
    let mut kvret: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    result.into_iter().for_each(|p| {
        let key = Into::<Vec<u8>>::into(p.key().to_owned());
        let value = Into::<Vec<u8>>::into(p.value().to_owned());
        kvret.insert(key, value);
    });
    let values: Vec<_> = keys.into_iter().map(|k| {
        let data = kvret.get::<Vec<u8>>(&k.into());
        match data {
            Some(val) => {
                Into::<RedisValue>::into(val.to_owned())
            },
            None => {
                RedisValue::Null
            }
        }
    }).collect();
    Ok(values.into())
}
