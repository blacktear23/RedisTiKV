use crate::{
    metrics::*,
    commands::asyncs::set::*,
    utils::async_execute,
};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString};

pub fn tikv_sadd(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["sadd"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let members = args.map(|x| x.to_string_lossy()).collect();
    async_execute(ctx, async move {
        do_async_sadd(key, members).await
    })
}

pub fn tikv_scard(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["scard"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    async_execute(ctx, async move {
        do_async_scard(key).await
    })
}

pub fn tikv_smembers(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["smembers"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    async_execute(ctx, async move {
        do_async_smembers(key).await
    })
}