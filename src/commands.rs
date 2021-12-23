use std::thread;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString, ThreadSafeContext };
use crate::utils::{ redis_resp, tokio_spawn };
use crate::tikv::*;

pub async fn do_async_curl(url: &str) -> Result<RedisValue, reqwest::Error> {
    let client = reqwest::Client::new();
    let text = client.get(url).send().await?.text().await?;
    Ok(text.into())
}

pub fn async_curl(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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

pub fn tikv_connect(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 1 {
        return Err(RedisError::WrongArity);
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

pub fn tikv_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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

pub fn tikv_put(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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

pub fn tikv_del(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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

pub fn tikv_load(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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

pub fn tikv_scan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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
            let res = do_async_scan(start_key, limit).await;
            redis_resp(blocked_client, res);
        } else {
            let res = do_async_scan_range(start_key, end_key, limit).await;
            redis_resp(blocked_client, res);
        }
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_del_range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let key_start = args.next_str()?;
    let key_end = args.next_str()?;
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_delete_range(key_start, key_end).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_batch_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let keys: Vec<String> = args.into_iter().skip(1).map(|s| s.to_string()).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_batch_get(keys).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

pub fn curl_mul(_: &Context, args: Vec<RedisString>) -> RedisResult {
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

pub fn curl_echo(_: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 2 {
        return Err(RedisError::WrongArity);
    }

    let mut args = args.into_iter().skip(1);

    let response = args.next_str()?;
    return Ok(response.into());
}

pub fn thread_curl(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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
