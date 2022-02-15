use super::get_client;
use crate::{
    encoding::{KeyEncoder, KeyDecoder, EMPTY_VALUE},
    commands::errors::AsyncResult,
    utils::resp_int,
};
use redis_module::RedisValue;

pub async fn do_async_sadd(key: &str, members: Vec<String>) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let mut added_num: i64 = 0;
    for m in members.iter() {
        let ekey = encoder.encode_set(key, m);
        let (_, swapped) = client.compare_and_swap(ekey, None, EMPTY_VALUE).await?;
        if swapped {
            added_num += 1;
        }
    }
    Ok(resp_int(added_num))
}

pub async fn do_async_scard(key: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let range = encoder.encode_set_start(key)..encoder.encode_set_end(key);
    let result = client.scan(range.into(), 10200).await?;
    Ok(resp_int(result.len() as i64))
}

pub async fn do_async_smembers(key: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let decoder = KeyDecoder::new();
    let range = encoder.encode_set_start(key)..encoder.encode_set_end(key);
    let result = client.scan(range.into(), 10200).await?;
    let values: Vec<Vec<u8>> = result
        .into_iter()
        .map(|p| {
            Vec::from(decoder.decode_set_member(p.key().to_owned(), key))
        })
        .collect();
    Ok(values.into())
}