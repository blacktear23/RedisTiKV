use redis_module::{Context, RedisString, RedisResult, RedisError, NextArg, RedisValue, ThreadSafeContext, KeyType};
use tikv_client::KvPair;
use crate::{
    metrics::*,
    utils::{tokio_spawn, redis_resp, redis_resp_with_ctx, resp_int}, encoding::KeyEncoder,
};
use super::asyncs::string::*;

pub fn tikv_raw_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["get"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_get(key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["set"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_put(key, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_del(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["del"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let keys: Vec<String> = args.into_iter().skip(1).map(|s| s.to_string()).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_batch_del(keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_scan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["scan"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let num_args = args.len();
    let mut args = args.into_iter().skip(1);
    let start_key = args.next_str()?;
    let end_key: &str;
    if num_args > 3 {
        end_key = args.next_str()?;
    } else {
        end_key = "";
    }
    let limit = args.next_u64()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        if num_args == 3 {
            let res = do_async_rawkv_scan(start_key, limit).await;
            redis_resp(blocked_client, res);
        } else {
            let res = do_async_rawkv_scan_range(start_key, end_key, limit).await;
            redis_resp(blocked_client, res);
        }
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_setnx(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["setnx"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_put_not_exists(key, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_cached_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["get"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    // Try to get from redis
    let rkey = ctx.open_key(&key);
    if rkey.key_type() == KeyType::String {
        match rkey.read()? {
            Some(val) => {
                return Ok(val.into());
            }
            _ => {}
        }
    }
    let blocked_client = ctx.block_client();
    let skey = key.to_string_lossy();
    tokio_spawn(async move {
        let tctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let res = do_async_rawkv_cached_get(&tctx, skey).await;
        redis_resp_with_ctx(&tctx, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_cached_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["set"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let value = args.next_str()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let tctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let res = do_async_rawkv_cached_put(&tctx, key.to_string(), value).await;
        redis_resp_with_ctx(&tctx, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_cached_del(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["del"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let args = args.into_iter().skip(1);
    let mut keys: Vec<String> = Vec::new();
    args.for_each(|s| {
        keys.push(s.to_string());
        let rkey = ctx.open_key_writable(&s);
        match rkey.delete() {
            Err(err) => {
                ctx.log_notice(&format!(
                    "Delete Redis Key {} got error: {}",
                    s.to_string(),
                    err.to_string()
                ));
            }
            _ => {}
        }
    });
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_batch_del(keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_incr(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["incr"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_incr(key.to_owned(), true, 1).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_decr(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["decr"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_incr(key.to_owned(), false, 1).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_incrby(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["incr"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let step = args.next_i64()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_incr(key.to_owned(), true, step).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_decrby(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["decr"]).inc();
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let step = args.next_i64()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_incr(key.to_owned(), false, step).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_exists(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["exists"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let keys: Vec<String> = args
        .into_iter()
        .skip(1)
        .map(|s| s.to_string())
        .collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_exists(keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_batch_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["mget"]).inc();
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let keys: Vec<String> = args
        .into_iter()
        .skip(1)
        .map(|s| s.to_string())
        .collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_batch_get(keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_batch_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&["mset"]).inc();
    let num_kvs = args.len() - 1;
    if num_kvs % 2 != 0 {
        return Err(RedisError::WrongArity);
    }
    let mut kvs: Vec<KvPair> = Vec::new();
    let encoder = KeyEncoder::new();
    let mut args = args.into_iter().skip(1);
    for _i in 0..num_kvs / 2 {
        let key = args.next_str()?;
        let value = args.next_str()?;
        let kv = KvPair::from((encoder.encode_string(key), value.to_owned()));
        kvs.push(kv);
    }
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_batch_put(kvs).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_redis_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let value = args.next_str()?;
    let rkey = ctx.open_key_writable(&key);
    let _ = rkey.write(value)?;
    Ok(resp_int(1))
}

pub fn tikv_raw_expire(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let ttl = args.next_u64()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_expire(key.to_owned(), ttl).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_raw_ttl(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_rawkv_get_ttl(key.to_owned()).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}