use redis_module::{RedisError, Context, NextArg, RedisResult, RedisValue, RedisString, ThreadSafeContext, BlockedClient };
use quick_js::{Context as JsContext, ExecutionError};
use crate::{
    utils::{tokio_spawn, redis_resp},
};

fn redis_value_to_string(val: RedisValue) -> String {
    format!("{:?}", val)
}

pub async fn do_async_eval(ctx: ThreadSafeContext<BlockedClient>, script: &str) -> Result<RedisValue, ExecutionError> {
    let jctx = JsContext::new().unwrap();
    jctx.add_callback("redis_call", move |cmd: String, args: String| {
        let pargs = args.split(" ").collect::<Vec<&str>>();
        match ctx.lock().call(&cmd, &pargs) {
            Ok(val) => Ok(redis_value_to_string(val)),
            Err(err) => Err(ExecutionError::Internal(err.to_string())),
        }
    })?;
    let val = jctx.eval(script)?;
    let ret = match val.into_string() {
        None => RedisValue::Null,
        Some(v) => RedisValue::BulkString(v),
    };
    Ok(ret)
}

pub fn js_eval(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let script = args.next_str()?;

    let blocked_client = ctx.block_client();
    let blocked_client2 = ctx.block_client();
    tokio_spawn(async move {
        let tctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let res = do_async_eval(tctx, script).await;
        redis_resp(blocked_client2, res);
    });
    Ok(RedisValue::NoReply)
}