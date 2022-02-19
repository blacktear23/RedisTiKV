use crate::{
    utils::{resp_sstr, async_execute},
    commands::asyncs::connection::*,
};
use redis_module::{Context, RedisError, RedisResult, RedisString};

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

    async_execute(ctx, async move {
        do_async_connect(addrs).await
    })
}

pub fn tikv_close(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    async_execute(ctx, async move {
        do_async_close().await
    })
}