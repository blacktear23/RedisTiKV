use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString, ThreadSafeContext };
use crate::utils::{ redis_resp, tokio_spawn };
use crate::tikv::*;
use tikv_client::{KvPair};
use crate::encoding::{DataType, encode_hash_key};
use crate::init::GLOBAL_CLIENT;

pub fn tikv_hset(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hput(key, field, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hget(key, field).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hget_all(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan(key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hkeys(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan_fields(key).await;
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
        let res = do_async_batch_put(kvs).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hmget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let fields: Vec<String> = args.map(|s| encode_hash_key(key, &s.to_string())).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_hget(fields).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hvals(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hscan_values(key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_hexists(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let field = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_hexists(key, field).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
