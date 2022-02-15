use crate::commands::errors::AsyncResult;
use redis_module::RedisValue;
use tikv_client::ColumnFamily;
use super::get_client;


pub async fn do_async_rawkv_ascan(
    cf: ColumnFamily,
    prefix: &str,
    limit: u64,
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let range = prefix.to_owned()..;
    let result = client.with_cf(cf).scan(range, limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                Into::<Vec<u8>>::into(p.key().to_owned()),
                Into::<Vec<u8>>::into(p.value().clone()),
            ])
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_rawkv_ascan_range(
    cf: ColumnFamily,
    start_key: &str,
    end_key: &str,
    limit: u64,
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let range = start_key.to_owned()..end_key.to_owned();
    let result = client.with_cf(cf).scan(range, limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                Into::<Vec<u8>>::into(p.key().to_owned()),
                Into::<Vec<u8>>::into(p.value().to_owned()),
            ])
        })
        .collect();
    Ok(values.into())
}