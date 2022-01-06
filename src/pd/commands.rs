use crate::{
    utils::{redis_resp, tokio_spawn},
    pd::utils::*,
};
use redis_module::{Context, NextArg, RedisResult, RedisValue, RedisString };

pub fn pd_members(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut pd_addr: &str = "127.0.0.1:2379";
    if args.len() > 1 {
        pd_addr = args.into_iter().skip(1).next_str()?;
    }
    let url = generate_pd_url(pd_addr, "members");

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_curl(&url).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}