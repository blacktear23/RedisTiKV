use crate::{
    pd::utils::*,
    utils::{redis_resp, tokio_spawn},
};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};

pub fn pd_members(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let pd_addr = get_pd_addr()?;
    let url = generate_pd_url(&pd_addr, "members");

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_get(&url).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn pd_regions(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let pd_addr = get_pd_addr()?;
    let url = generate_pd_url(&pd_addr, "regions");

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_get_members(&url).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn pd_stores(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let pd_addr = get_pd_addr()?;
    let url = generate_pd_url(&pd_addr, "stores");

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_get(&url).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn pd_apiget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let pd_addr = get_pd_addr()?;
    let mut args = args.into_iter().skip(1);
    let sub_path = args.next_str()?;
    let url = generate_pd_url(&pd_addr, sub_path);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_get(&url).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn pd_apidelete(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let pd_addr = get_pd_addr()?;
    let mut args = args.into_iter().skip(1);
    let sub_path = args.next_str()?;
    let url = generate_pd_url(&pd_addr, sub_path);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_delete(&url).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn pd_apipost(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let num_args = args.len();
    let pd_addr = get_pd_addr()?;
    let mut args = args.into_iter().skip(1);
    let sub_path = args.next_str()?;
    let mut body = "";
    if num_args > 2 {
        body = args.next_str()?;
    }
    let url = generate_pd_url(&pd_addr, sub_path);
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_post(&url, body.to_owned()).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
