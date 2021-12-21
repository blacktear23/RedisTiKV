#[macro_use]
extern crate redis_module;
extern crate tokio;
extern crate reqwest;
extern crate tikv_client;

use std::thread;
use tikv_client::RawClient;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::runtime::{ Runtime, Handle };
use tokio::time::{sleep, Duration};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisValue, RedisString, ThreadSafeContext, Status, BlockedClient };

static mut GLOBAL_RT: Option<Handle> = None;
static mut GLOBAL_RUNNING: Option<Arc<Mutex<u32>>> = None;
static mut GLOBAL_CLIENT: Option<RawClient> = None;

// Initial tokio main executor in other thread
fn tikv_init(_ctx: &Context, _args: &Vec<RedisString>) -> Status {
    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let running =  Arc::new(Mutex::new(1));
        unsafe {
            GLOBAL_RT.replace(handle);
            GLOBAL_RUNNING.replace(running);
        }
        println!("Tokio Runtime Created!");
        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                let running = unsafe { *GLOBAL_RUNNING.as_ref().unwrap().lock().unwrap() };
                if running == 0 {
                    return;
                }
            }
        });
        println!("Tokio Runtime Finished");
        runtime.shutdown_timeout(Duration::from_secs(10));
        println!("Tokio Runtime Shutdown");
    });

    Status::Ok
}

fn tikv_deinit(_ctx: &Context) -> Status {
    unsafe {
        let data = GLOBAL_RUNNING.as_ref().unwrap();
        *data.lock().unwrap() = 0;
    };
    println!("Set Runnint to False");
    Status::Ok
}

// Respose for redis blocked client
fn redis_resp<E>(client: BlockedClient, result: Result<RedisValue, E>)
where
    E: std::error::Error
{
    let ctx = ThreadSafeContext::with_blocked_client(client);
    match result {
        Ok(data) => {
            ctx.reply(Ok(data.into()));
        },
        Err(err) => {
            let err_msg = format!("error: {}", err);
            ctx.reply(Ok(err_msg.into()));
        },
    };
}

// Spawn async task from Redis Module main thread
fn tokio_spawn<T>(future: T)
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    let hdl = unsafe { GLOBAL_RT.as_ref().unwrap() };
    hdl.spawn(future);
}

// Async worker helpers
async fn do_async_curl(url: &str) -> Result<RedisValue, reqwest::Error> {
    let client = reqwest::Client::new();
    let text = client.get(url).send().await?.text().await?;
    Ok(text.into())
}

async fn do_async_connect(pd_addr: &str) -> Result<RedisValue, tikv_client::Error> {
    let mut addrs = Vec::new();
    if pd_addr == "" {
        addrs.push("127.0.0.1:2379");
    } else {
        addrs.push(pd_addr);
    }
    let client = RawClient::new(addrs).await?;
    unsafe {
        GLOBAL_CLIENT.replace(client);
    }
    Ok("OK".into())
}

async fn do_async_get(key: &str) -> Result<RedisValue, tikv_client::Error> {
    let client = unsafe { GLOBAL_CLIENT.as_ref().unwrap() };
    let value = client.get(key.to_owned()).await?;
    Ok(value.into())
}

async fn do_async_get_raw(key: &str) -> Result<Vec<u8>, tikv_client::Error> {
    let client = unsafe { GLOBAL_CLIENT.as_ref().unwrap() };
    let value = client.get(key.to_owned()).await?;
    Ok(value.unwrap())
}

async fn do_async_put(key: &str, val: &str) -> Result<RedisValue, tikv_client::Error> {
    let client = unsafe { GLOBAL_CLIENT.as_ref().unwrap() };
    let value = client.put(key.to_owned(), val.to_owned()).await?;
    Ok(value.into())
}

async fn do_async_del(key: &str) -> Result<RedisValue, tikv_client::Error> {
    let client = unsafe { GLOBAL_CLIENT.as_ref().unwrap() };
    let value = client.delete(key.to_owned()).await?;
    Ok(value.into())
}

async fn do_async_scan(prefix: &str, limit: u64) -> Result<RedisValue, tikv_client::Error> {
    let client = unsafe { GLOBAL_CLIENT.as_ref().unwrap() };
    let range = prefix.to_owned()..;
    let result = client.scan(range, limit as u32).await?;
    let values: Vec<_> = result.into_iter().map(|p| Vec::from([Into::<Vec<u8>>::into(p.key().clone()), Into::<Vec<u8>>::into(p.value().clone())])).collect();
    Ok(values.into())
}

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
