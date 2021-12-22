#[macro_use]
extern crate redis_module;
extern crate tokio;
extern crate reqwest;
extern crate tikv_client;

mod init;
mod utils;
mod commands;

use std::thread;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString, ThreadSafeContext };
use init::{ tikv_init, tikv_deinit };
use utils::{ redis_resp, tokio_spawn };
use commands::*;

// Commands
fn async_curl(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let url = args.next_str()?;

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_curl(url).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

fn tikv_connect(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 1 {
        return Err(RedisError::WrongArity);
    }
    let mut pd_addr: &str = "";

    if args.len() > 1 {
        let mut args = args.into_iter().skip(1);
        pd_addr = args.next_str()?;
    }

    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_connect(pd_addr).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

fn tikv_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_get(key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

fn tikv_put(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let value = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_put(key, value).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

fn tikv_del(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_del(key).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)

}

fn tikv_load(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let tctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let res = do_async_get_raw(key).await;
        match res {
            Ok(data) => {
                if data.len() > 0 {
                    let data_str = std::str::from_utf8(&data);
                    tctx.lock().call("SET", &[key, data_str.unwrap()]).unwrap();
                }
                tctx.reply(Ok(data.into()));
            },
            Err(err) => {
                let err_msg = format!("error: {}", err);
                tctx.reply(Ok(err_msg.into()));
            },
        };
    });
    Ok(RedisValue::NoReply)
}

fn tikv_scan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key = args.next_str()?;
    let limit = args.next_u64()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_scan(key, limit).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

fn curl_mul(_: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let nums = args
        .into_iter()
        .skip(1)
        .map(|s| s.parse_integer())
        .collect::<Result<Vec<i64>, RedisError>>()?;

    let product = nums.iter().product();

    let mut response = Vec::from(nums);
    response.push(product);

    return Ok(response.into());
}

fn curl_echo(_: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let mut args = args.into_iter().skip(1);

    let response = args.next_str()?;
    return Ok(response.into());
}

fn thread_curl(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let url = args.next_str().unwrap();

    let blocked_client = ctx.block_client();
    thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let client = reqwest::blocking::Client::new();
        let res = client.get(url).send();
        match res {
            Err(err) => {
                let err_msg = format!("error: {}", err);
                thread_ctx.reply(Ok(err_msg.into()));
            },
            Ok(body) => {
                thread_ctx.reply(Ok(body.text().unwrap().into()));
            }
        }
    });

    Ok(RedisValue::NoReply)
}


// register functions
redis_module! {
    name: "tikv",
    version: 1,
    data_types: [],
    init: tikv_init,
    deinit: tikv_deinit,
    commands: [
        ["tikv.mul", curl_mul, "", 0, 0, 0],
        ["tikv.echo", curl_echo, "", 0, 0, 0],
        ["tikv.curl", async_curl, "", 0, 0, 0],
        ["tikv.tcurl", thread_curl, "", 0, 0, 0],
        ["tikv.conn", tikv_connect, "", 0, 0, 0],
        ["tikv.get", tikv_get, "", 0, 0, 0],
        ["tikv.put", tikv_put, "", 0, 0, 0],
        ["tikv.del", tikv_del, "", 0, 0, 0],
        ["tikv.load", tikv_load, "", 0, 0, 0],
        ["tikv.scan", tikv_scan, "", 0, 0, 0],
    ],
}
