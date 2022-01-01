use crate::tikv::*;
use crate::utils::{redis_resp, tokio_spawn};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};

pub fn tikv_lpush(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let len = args.len() as i64;
    let blocked_client = ctx.block_client();
    let elements = args.map(|x| x.to_string_lossy()).collect();
    tokio_spawn(async move {
        let res = do_async_lpush(key, elements).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::Integer(len))
}

pub fn tikv_lrange(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let start = args.next_str()?.parse::<i32>().unwrap();
    let end = args.next_str()?.parse::<i32>().unwrap();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_lrange(key, start, end).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}