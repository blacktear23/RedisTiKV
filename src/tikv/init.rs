use crate::{
    tikv::{PD_ADDRS, TIKV_TNX_CONN_POOL},
    utils::{redis_resp, resp_ok, resp_sstr, tokio_spawn},
};
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};
use tikv_client::Error;

pub async fn do_async_connect(addrs: Vec<String>) -> Result<RedisValue, Error> {
    PD_ADDRS.write().unwrap().replace(addrs.clone());
    Ok(resp_ok())
}

pub async fn do_async_close() -> Result<RedisValue, Error> {
    *PD_ADDRS.write().unwrap() = None;
    let mut pool = TIKV_TNX_CONN_POOL.lock().unwrap();
    for _i in 0..pool.len() {
        let client = pool.pop_front();
        drop(client);
    }
    Ok(resp_sstr("Closed"))
}

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
