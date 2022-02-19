use std::str::FromStr;
use crate::{
    metrics::*,
    commands::asyncs::list::*,
    utils::async_execute,
};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString};

pub fn tikv_lpush(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["lpush"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let elements = args.map(|x| x.to_string_lossy()).collect();
    ctx.log_debug(&format!(
        "Handle tikv_lpush commands, key: {}, elements: {:?}",
        key, elements
    ));
    async_execute(ctx, async move {
        do_async_push(key, elements, ListDirection::Left).await
    })
}

pub fn tikv_lrange(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["lrange"]).inc();
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let start = args.next_str()?.parse::<i64>().unwrap();
    let end = args.next_str()?.parse::<i64>().unwrap();
    ctx.log_debug(&format!(
        "Handle tikv_lrange commands, key: {}, start: {}, end: {}",
        key, start, end
    ));
    async_execute(ctx, async move {
        do_async_lrange(key, start, end).await
    })
}

pub fn tikv_rpush(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["lpush"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let elements = args.map(|x| x.to_string_lossy()).collect();
    ctx.log_debug(&format!(
        "Handle tikv_lpush commands, key: {}, elements: {:?}",
        key, elements
    ));
    async_execute(ctx, async move {
        do_async_push(key, elements, ListDirection::Right).await
    })
}

pub fn tikv_llen(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["llen"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    ctx.log_debug(&format!("Handle tikv_llen commands, key: {}", key));
    async_execute(ctx, async move {
        do_async_llen(key).await
    })
}

pub fn tikv_lpop(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["lpop"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let count = match args.next() {
        Some(s) => i64::from_str(s.try_as_str()?)?,
        None => 1,
    };
    if count < 0 {
        return Err(RedisError::Str("value is out of range, must be positive"));
    }
    ctx.log_debug(&format!(
        "Handle tikv_lpop commands, key: {}, count: {}",
        key, count
    ));
    async_execute(ctx, async move {
        do_async_pop(key, count, ListDirection::Left).await
    })
}

pub fn tikv_rpop(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["rpop"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let count = match args.next() {
        Some(s) => i64::from_str(s.try_as_str()?)?,
        None => 1,
    };
    if count < 0 {
        return Err(RedisError::Str("value is out of range, must be positive"));
    }
    ctx.log_debug(&format!(
        "Handle tikv_lpop commands, key: {}, count: {}",
        key, count
    ));
    async_execute(ctx, async move {
        do_async_pop(key, count, ListDirection::Right).await
    })
}

pub fn tikv_lindex(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["lindex"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    if args.len() > 3 {
        return Err(RedisError::Str("extra arguments are not supported yet."));
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let index = args.next_i64()?;
    ctx.log_debug(&format!(
        "Handle tikv_lindex commands, key: {}, index: {}",
        key, index
    ));
    async_execute(ctx, async move {
        do_async_lindex(key, index).await
    })
}

pub fn tikv_ldel(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["ldel"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    async_execute(ctx, async move {
        do_async_ldel(key).await
    })
}

/*
pub fn tikv_ltrim(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }
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
        let res = do_async_ltrim(key, start, end).await;
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
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let element = args.next_str()?;
    ctx.log_debug(&format!(
        "Handle tikv_lpos commands, key: {}, element: {}",
        key, element
    ));
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_lpos(key, element).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
*/
