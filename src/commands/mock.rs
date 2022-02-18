use redis_module::{Context, RedisString, RedisResult, RedisError, NextArg, RedisValue};
use crate::{
    utils::{tokio_spawn, redis_resp, tokio_block_on},
};
use crate::{
    commands::errors::AsyncResult,
    utils::sleep,
};

use super::asyncs::string::do_async_rawkv_get;

async fn do_async_mock_get(_key: &str) -> AsyncResult<RedisValue> {
    Ok("Mock Value".into())
}

pub fn tikv_mock_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;

    // let blocked_client = ctx.block_client();
    match tokio_block_on(async move {
        // do_async_mock_get(key).await
        do_async_rawkv_get(key).await
    }) {
        Ok(val) => Ok(val),
        Err(err) => Err(RedisError::String(err.to_string())),
    }
}