use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString};
use crate::utils::{ redis_resp, tokio_spawn, get_client_id };
use crate::tikv::*;
use tikv_client::{KvPair};
use crate::encoding::{encode_hash_key};

pub fn tikv_hset(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hput(cid, key, field, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hget(cid, key, field).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hget_all(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hkeys(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan_fields(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hmset(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let num_kvs = args.len() - 2;
    if num_kvs % 2 != 0 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut kvs: Vec<KvPair> = Vec::new();
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    for _i in 0..num_kvs/2 {
        let field = args.next_str()?;
        let value = args.next_str()?;
        let kv = KvPair::from((
            encode_hash_key(key, field).to_owned(),
            value.to_owned()
        ));
        kvs.push(kv);
    }
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_put(cid, kvs).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hmget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let fields: Vec<String> = args.map(|s| encode_hash_key(key, &s.to_string())).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_hget(cid, fields).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hvals(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan_values(cid, key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hexists(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let cid = get_client_id(ctx);
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hexists(cid, key, field).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
