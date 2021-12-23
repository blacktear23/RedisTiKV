use redis_module::{ RedisValue };
use tikv_client::RawClient;
use crate::init::GLOBAL_CLIENT;

pub fn get_client() -> Result<Box<RawClient>, tikv_client::Error> {
    let guard = GLOBAL_CLIENT.read().unwrap();
    match guard.as_ref() {
        Some(val) => {
            let client = val.clone();
            Ok(client)
        },
        None => Err(tikv_client::Error::StringError(String::from("Not connected")))
    }
}

pub async fn do_async_connect(pd_addr: &str) -> Result<RedisValue, tikv_client::Error> {
    let mut addrs = Vec::new();
    if pd_addr == "" {
        addrs.push("127.0.0.1:2379");
    } else {
        addrs.push(pd_addr);
    }
    let client = RawClient::new(addrs).await?;
    GLOBAL_CLIENT.write().unwrap().replace(Box::new(client));
    Ok("OK".into())
}

pub async fn do_async_get(key: &str) -> Result<RedisValue, tikv_client::Error> {
    let client = get_client()?;
    let value = client.get(key.to_owned()).await?;
    Ok(value.into())
}

pub async fn do_async_get_raw(key: &str) -> Result<Vec<u8>, tikv_client::Error> {
    let client = get_client()?;
    let value = client.get(key.to_owned()).await?;
    Ok(value.unwrap())
}

pub async fn do_async_put(key: &str, val: &str) -> Result<RedisValue, tikv_client::Error> {
    let client = get_client()?;
    let value = client.put(key.to_owned(), val.to_owned()).await?;
    Ok(value.into())
}

pub async fn do_async_del(key: &str) -> Result<RedisValue, tikv_client::Error> {
    let client = get_client()?;
    let value = client.delete(key.to_owned()).await?;
    Ok(value.into())
}

pub async fn do_async_scan(prefix: &str, limit: u64) -> Result<RedisValue, tikv_client::Error> {
    let client = get_client()?;
    let range = prefix.to_owned()..;
    let result = client.scan(range, limit as u32).await?;
    let values: Vec<_> = result.into_iter().map(|p| Vec::from([Into::<Vec<u8>>::into(p.key().clone()), Into::<Vec<u8>>::into(p.value().clone())])).collect();
    Ok(values.into())
}

pub async fn do_async_delete_range(key_start: &str, key_end: &str) -> Result<RedisValue, tikv_client::Error> {
    let client = get_client()?;
    let range = key_start.to_owned()..key_end.to_owned();
    let result = client.delete_range(range).await?;
    Ok(result.into())
}
