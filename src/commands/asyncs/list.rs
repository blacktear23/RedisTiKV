use super::get_client;
use crate::{
    encoding::{KeyEncoder, KeyDecoder}, commands::errors::{AsyncResult, RTError},
    utils::{resp_int, sleep, resp_ok}, client::RawClientWrapper,
};
use redis_module::RedisValue;
use tikv_client::{Key};

#[derive(Copy, Clone, Debug)]
pub enum ListDirection {
    Left,
    Right,
}

async fn push_adjust_meta(
    client: &RawClientWrapper,
    encoder: &KeyEncoder,
    mkey: Key,
    dir: ListDirection,
    size: usize,
) -> AsyncResult<(i64, Vec<i64>)> {
    let decoder = KeyDecoder::new();
    for t in 0..2000 {
        let mval = client.get(mkey.clone()).await?;
        let (l, r) = decoder.decode_list_meta(mval.clone().into());
        let mut nl = l;
        let mut nr = r;
        let mut ret: Vec<i64> = Vec::new();
        match dir {
            ListDirection::Left => {
                for i in 0..size {
                    let idx = l - i as i64 - 1;
                    ret.push(idx);
                }
                nl = l - size as i64;
            }
            ListDirection::Right => {
                for i in 0..size {
                    let idx = r + i as i64;
                    ret.push(idx)
                }
                nr = r + size as i64;
            }
        }
        let nmval = encoder.encode_list_meta(nl, nr);
        let (_, swapped) = client.compare_and_swap(mkey.clone(), mval, nmval).await?;
        if swapped {
            let size = nr - nl;
            return Ok((size, ret));
        }
        sleep(std::cmp::min(t, 200)).await;
    }
    Err(RTError::StringError(String::from("Cannot set list meta data")))
}

async fn pop_adjust_meta(
    client: &RawClientWrapper,
    encoder: &KeyEncoder,
    mkey: Key,
    dir: ListDirection,
    size: i64,
) -> AsyncResult<(bool, (i64, i64))> {
    let decoder = KeyDecoder::new();
    for t in 0..2000 {
        let mval = client.get(mkey.clone()).await?;
        let (l, r) = decoder.decode_list_meta(mval.clone().into());
        let mut nl = l;
        let mut nr = r;
        let key_pair: (i64, i64);
        let count = i64::min(size as i64, r - l);
        if count == 0 {
            return Ok((false, (0, 0)));
        }
        match dir {
            ListDirection::Left => {
                nl = l + count;
                key_pair = (l, l+count);
            }
            ListDirection::Right => {
                nr = r - count - 1;
                key_pair = (r - count -1, r - 1);
            }
        }
        let nmval = encoder.encode_list_meta(nl, nr);
        let (_, swapped) = client.compare_and_swap(mkey.clone(), mval, nmval).await?;
        if swapped {
            return Ok((true, key_pair));
        }
        sleep(std::cmp::min(t, 200)).await;
    }
    Err(RTError::StringError(String::from("Cannot set list meta data")))
}

pub async fn do_async_push(
    key: &str,
    elements: Vec<String>,
    dir: ListDirection,
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let emkey = encoder.encode_list_meta_key(key);
    let (size, idxes) = push_adjust_meta(&client, &encoder, emkey, dir, elements.len()).await?;
    for (pos, e) in elements.iter().enumerate() {
        if let Some(idx) = idxes.get(pos) {
            let ekey = encoder.encode_list_elem_key(key, *idx);
            let _ = client.put(ekey, &e.to_owned()).await?;
        }
    }
    Ok(resp_int(size))
}

pub async fn do_async_pop(
    key: &str,
    count: i64,
    dir: ListDirection,
) -> AsyncResult<RedisValue> {
    println!("{:?}", &count);
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let emkey = encoder.encode_list_meta_key(key);
    let (notempty, (lkey, rkey)) = pop_adjust_meta(&client, &encoder, emkey, dir, count).await?;
    if !notempty {
        return Ok(RedisValue::Null);
    }
    let start_key = encoder.encode_list_elem_key(key, lkey);
    let end_key = encoder.encode_list_elem_key(key, rkey);
    let range = start_key..end_key;
    println!("{:?}", &range);
    let result = client.scan(range.into(), count as u32).await?;
    println!("{:?}", &result);
    let mut rkeys: Vec<Key> = Vec::new();
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter()
        .for_each(|kv| {
            rkeys.push(kv.key().clone());
            values.push(kv.value().to_owned().into());
        });
    let _ = client.batch_delete(rkeys).await?;
    Ok(values.into())
}

pub async fn do_async_llen(key: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let emkey = KeyEncoder::new().encode_list_meta_key(key);
    let decoder = KeyDecoder::new();
    let mval = client.get(emkey.clone()).await?;
    let (l, r) = decoder.decode_list_meta(mval.clone().into());
    Ok(resp_int(r - l))
}

pub async fn do_async_lrange(
    key: &str,
    start: i64,
    stop: i64,
) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let decoder = KeyDecoder::new();
    let encoder = KeyEncoder::new();
    let emkey = encoder.encode_list_meta_key(key);
    let mval = client.get(emkey.clone()).await?;
    let (l, r) = decoder.decode_list_meta(mval.clone().into());

    let num = match stop {
        p if p > 0 => i64::min(stop - start, r - l) + 1,
        n if n < 0 => r + n - l + 1,
        _ => 0,
    };

    let start_pos = l + start;
    let end_pos = start_pos + num;

    let start_key = encoder.encode_list_elem_key(key, start_pos);
    let end_key = encoder.encode_list_elem_key(key, end_pos);
    let range = start_key..end_key;
    let result = client.scan(range.into(), num as u32).await?;
    let values: Vec<RedisValue> = result
        .into_iter()
        .map(|p| p.value().clone().into())
        .collect();
    Ok(values.into())
}

pub async fn do_async_lindex(key: &str, index: i64) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let decoder = KeyDecoder::new();
    let encoder = KeyEncoder::new();
    let emkey = encoder.encode_list_meta_key(key);
    let mval = client.get(emkey.clone()).await?;
    let (l, r) = decoder.decode_list_meta(mval.clone().into());

    let pos = if index < 0 { r + index } else { l + index };
    if pos < l || pos >= r {
        return Ok(RedisValue::Null);
    }
    let mkey = encoder.encode_list_elem_key(key, pos);
    let value = client.get(mkey).await?;
    Ok(value.into())
}

pub async fn do_async_ldel(key: &str) -> AsyncResult<RedisValue> {
    let client = get_client()?;
    let encoder = KeyEncoder::new();
    let emkey = encoder.encode_list_meta_key(key);
    let range = encoder.encode_list_elem_start(key)..encoder.encode_list_elem_end(key);
    let _ = client.batch_delete(vec![emkey]).await?;
    let _ = client.delete_range(range.into()).await?;
    Ok(resp_ok())
}

/*
fn adjust_offset(offset: i64, l: i64, r: i64) -> i64 {
    if offset < 0 {
        r + offset + 1
    } else {
        l + offset
    }
}

pub async fn do_async_ltrim(
    key: &str,
    start: i64,
    stop: i64,
) -> AsyncResult<RedisValue> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);
    if l == r {
        finish_txn(cid, txn, in_txn).await?;
        return Ok(resp_ok());
    }

    let new_l = adjust_offset(start, l, r);
    let mut new_r = adjust_offset(stop, l, r);
    if new_l >= new_r || start >= r - l {
        // TODO: remove key and the elements.
        new_r = new_l;
    }

    txn.put(encoded_key, encode_list_meta(new_l, new_r)).await?;

    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_lpos(key: &str, element: &str) -> AsyncResult<RedisValue> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    // TODO: avoid full scan for all elements.
    let start_key = encode_list_elem_key(key, l);
    let end_key = encode_list_elem_key(key, r);
    let range = start_key..end_key;
    let result = txn.scan(range, 10200).await?;
    let pos = result
        .into_iter()
        .position(|p| str::from_utf8(p.value()).unwrap() == element);

    finish_txn(cid, txn, in_txn).await?;

    match pos {
        Some(v) => Ok(resp_int(v as i64)),
        None => Ok(RedisValue::Null),
    }
}
*/