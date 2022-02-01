use crate::{
    tikv::TIKV_RAW_CLIENT,
    utils::{redis_resp, tokio_spawn, resp_ok},
};
use redis_module::{
    Context, NextArg, RedisError, RedisResult, RedisString, RedisValue,
};
use tikv_client::{Error, ColumnFamily};

use super::utils::get_txn_client;

pub async fn do_async_rawkv_ascan(
    cf: ColumnFamily,
    prefix: &str, 
    limit: u64,
) -> Result<RedisValue, Error> {
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap()};
    let range = prefix.to_owned()..;
    let result = client.with_cf(cf).scan(range, limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                Into::<Vec<u8>>::into(p.key().to_owned()),
                Into::<Vec<u8>>::into(p.value().clone()),
            ])
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_rawkv_ascan_range(
    cf: ColumnFamily,
    start_key: &str,
    end_key: &str,
    limit: u64,
) -> Result<RedisValue, Error> {
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap()};
    let range = start_key.to_owned()..end_key.to_owned();
    let result = client.with_cf(cf).scan(range, limit as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| {
            Vec::from([
                Into::<Vec<u8>>::into(p.key().to_owned()),
                Into::<Vec<u8>>::into(p.value().to_owned()),
            ])
        })
        .collect();
    Ok(values.into())
}

pub async fn do_async_tikv_gc() -> Result<RedisValue, Error> {
    let conn = get_txn_client().await?;
    let safepoint = conn.current_timestamp().await?;
    let _ = conn.gc(safepoint).await?;
    Ok(resp_ok())
}

pub fn tikv_rawkv_dscan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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
        let cf = ColumnFamily::Default;
        if num_args == 3 {
            let res = do_async_rawkv_ascan(cf, start_key, limit).await;
            redis_resp(blocked_client, res);
        } else {
            let res = do_async_rawkv_ascan_range(cf, start_key, end_key, limit).await;
            redis_resp(blocked_client, res);
        }
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_wscan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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
        let cf = ColumnFamily::Write;
        if num_args == 3 {
            let res = do_async_rawkv_ascan(cf, start_key, limit).await;
            redis_resp(blocked_client, res);
        } else {
            let res = do_async_rawkv_ascan_range(cf, start_key, end_key, limit).await;
            redis_resp(blocked_client, res);
        }
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_rawkv_lscan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
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
        let cf = ColumnFamily::Lock;
        if num_args == 3 {
            let res = do_async_rawkv_ascan(cf, start_key, limit).await;
            redis_resp(blocked_client, res);
        } else {
            let res = do_async_rawkv_ascan_range(cf, start_key, end_key, limit).await;
            redis_resp(blocked_client, res);
        }
    });
    Ok(RedisValue::NoReply)
}

pub fn tikv_gc(ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_tikv_gc().await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}