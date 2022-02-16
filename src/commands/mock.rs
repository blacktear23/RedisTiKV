use redis_module::{Context, RedisString, RedisResult, RedisError, NextArg, RedisValue, ThreadSafeContext, KeyType};
use crate::{
    utils::{tokio_spawn, redis_resp},
};
use crate::{
    commands::errors::AsyncResult,
    utils::sleep,
};

async fn do_async_mock_get(key: &str) -> AsyncResult<RedisValue> {
    sleep(1).await;
    Ok("Mock Value".into())
}

pub fn tikv_mock_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_mock_get(key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}