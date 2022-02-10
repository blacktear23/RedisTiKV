use crate::{
    utils::{redis_resp, resp_sstr, tokio_spawn},
    commands::asyncs::connection::*,
};
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};

use super::PD_ADDRS;

pub fn tikv_connect(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 1 {
        return Err(RedisError::WrongArity);
    }
    let guard = PD_ADDRS.read().unwrap();
    if guard.as_ref().is_some() {
        return Ok(resp_sstr("Already Connected"));
    }
    let mut addrs: Vec<String> = Vec::new();
    if args.len() == 1 {
        addrs.push(String::from("127.0.0.1:2379"));
    } else {
        addrs = args.into_iter().skip(1).map(|s| s.to_string()).collect();
    }

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_connect(addrs).await;
        redis_resp(blocked_client, res);
    });

    Ok(RedisValue::NoReply)
}

pub fn tikv_close(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_close().await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}