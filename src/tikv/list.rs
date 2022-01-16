use std::str;
use std::str::FromStr;

use crate::{
    tikv::{encoding::*, utils::*},
    utils::{get_client_id, redis_resp, resp_int, resp_ok, tokio_spawn},
};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use tikv_client::Error;

pub async fn do_async_lpush(
    cid: u64,
    key: &str,
    elements: Vec<String>,
) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    for (pos, e) in elements.iter().enumerate() {
        let _ = txn
            .put(encode_list_elem_key(key, l - pos as i64 - 1), e.to_owned())
            .await?;
    }

    let new_l = l - elements.len() as i64;
    txn.put(encoded_key, encode_list_meta(new_l, r)).await?;

    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_int(r - new_l))
}

pub async fn do_async_lrange(
    cid: u64,
    key: &str,
    start: i64,
    stop: i64,
) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    let num = match stop {
        p if p > 0 => i64::min(stop - start, r - l) + 1,
        n if n < 0 => r + n - l + 1,
        _ => 0,
    };

    let start_pos = l + start;
    let end_pos = start_pos + num;

    let start_key = encode_list_elem_key(key, start_pos);
    let end_key = encode_list_elem_key(key, end_pos);
    let range = start_key..end_key;
    let result = txn.scan(range, num as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| Vec::from(Into::<Vec<u8>>::into(p.value().clone())))
        .collect();

    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

fn adjust_offset(offset: i64, l: i64, r: i64) -> i64 {
    if offset < 0 {
        r + offset + 1
    } else {
        l + offset
    }
}

pub async fn do_async_ltrim(
    cid: u64,
    key: &str,
    start: i64,
    stop: i64,
) -> Result<RedisValue, Error> {
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

pub async fn do_async_rpush(
    cid: u64,
    key: &str,
    elements: Vec<String>,
) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    for (pos, e) in elements.iter().enumerate() {
        let _ = txn
            .put(encode_list_elem_key(key, r + pos as i64), e.to_owned())
            .await?;
    }

    let new_r = r + elements.len() as i64;
    txn.put(encoded_key, encode_list_meta(l, new_r)).await?;

    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_int(new_r - l))
}

pub async fn do_async_llen(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_int(r - l))
}

pub async fn do_async_lpop(cid: u64, key: &str, count: i64) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);
    let count = i64::min(count, r - l);
    if count == 0 {
        return Ok(RedisValue::Null);
    }

    let start_pos = l;
    let end_pos = start_pos + count;

    let start_key = encode_list_elem_key(key, start_pos);
    let end_key = encode_list_elem_key(key, end_pos);
    let range = start_key..end_key;
    let result = txn.scan(range, count as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| Vec::from(Into::<Vec<u8>>::into(p.value().clone())))
        .collect();

    let new_l = l + count;
    txn.put(encoded_key, encode_list_meta(new_l, r)).await?;

    // TODO: delete_range for pop-ed elements.
    // TODO: delete meta info for empty list.

    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_lpos(cid: u64, key: &str, element: &str) -> Result<RedisValue, Error> {
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

pub async fn do_async_lindex(cid: u64, key: &str, index: i64) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    let pos = if index < 0 { r + index } else { l + index };
    if pos < l || pos >= r {
        finish_txn(cid, txn, in_txn).await?;
        return Ok(RedisValue::Null);
    }
    let value = txn.get(encode_list_elem_key(key, pos)).await?;

    finish_txn(cid, txn, in_txn).await?;
    Ok(value.into())
}

pub fn tikv_lpush(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    let elements = args.map(|x| x.to_string_lossy()).collect();
    ctx.log_debug(&format!(
        "Handle tikv_lpush commands, key: {}, elements: {:?}",
        key, elements
    ));
    tokio_spawn(async move {
        let res = do_async_lpush(cid, key, elements).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_lrange(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let start = args.next_str()?.parse::<i64>().unwrap();
    let end = args.next_str()?.parse::<i64>().unwrap();
    ctx.log_debug(&format!(
        "Handle tikv_lrange commands, key: {}, start: {}, end: {}",
        key, start, end
    ));
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_lrange(cid, key, start, end).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rpush(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    let elements = args.map(|x| x.to_string_lossy()).collect();
    ctx.log_debug(&format!(
        "Handle tikv_lpush commands, key: {}, elements: {:?}",
        key, elements
    ));
    tokio_spawn(async move {
        let res = do_async_rpush(cid, key, elements).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_llen(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    ctx.log_debug(&format!("Handle tikv_llen commands, key: {}", key));
    tokio_spawn(async move {
        let res = do_async_llen(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_lpop(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let count = match args.next() {
        Some(s) => i64::from_str(s.try_as_str()?)?,
        None => 1,
    };
    if count < 0 {
        return Err(RedisError::Str("value is out of range, must be positive"));
    }
    let blocked_client = ctx.block_client();
    ctx.log_debug(&format!(
        "Handle tikv_lpop commands, key: {}, count: {}",
        key, count
    ));
    tokio_spawn(async move {
        let res = do_async_lpop(cid, key, count).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_ltrim(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let start = args.next_str()?.parse::<i64>().unwrap();
    let end = args.next_str()?.parse::<i64>().unwrap();
    ctx.log_debug(&format!(
        "Handle tikv_ltrim commands, key: {}, start: {}, end: {}",
        key, start, end
    ));
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_ltrim(cid, key, start, end).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_lpos(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    if args.len() > 3 {
        return Err(RedisError::Str("extra arguments are not supported yet."));
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let element = args.next_str()?;
    ctx.log_debug(&format!(
        "Handle tikv_lpos commands, key: {}, element: {}",
        key, element
    ));
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_lpos(cid, key, element).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_lindex(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    if args.len() > 3 {
        return Err(RedisError::Str("extra arguments are not supported yet."));
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let index = args.next_i64()?;
    ctx.log_debug(&format!(
        "Handle tikv_lindex commands, key: {}, index: {}",
        key, index
    ));
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_lindex(cid, key, index).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
