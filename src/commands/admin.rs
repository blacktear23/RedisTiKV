use crate::{
    commands::asyncs::admin::*,
    utils::{redis_resp, tokio_spawn},
};
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};
use tikv_client::{ColumnFamily, Error};

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

fn convert_to_column_family(cf_str: &str) -> Result<ColumnFamily, Error> {
    let lcf_str = cf_str.to_lowercase();
    if lcf_str.eq("default") || lcf_str.eq("d") {
        Ok(ColumnFamily::Default)
    } else if lcf_str.eq("write") || lcf_str.eq("w") {
        Ok(ColumnFamily::Write)
    } else if lcf_str.eq("lock") || lcf_str.eq("l") {
        Ok(ColumnFamily::Lock)
    } else {
        Err(Error::StringError(String::from("Unknown Column Family")))
    }
}

pub fn tikv_rawkv_cfscan(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if args.len() < 4 {
        return Err(RedisError::WrongArity);
    }

    let num_args = args.len();
    let mut args = args.into_iter().skip(1);
    let cf_str = args.next_str()?;
    let start_key = args.next_str()?;
    let end_key: &str;
    if num_args > 4 {
        end_key = args.next_str()?;
    } else {
        end_key = "";
    }
    let limit = args.next_u64()?;
    let blocked_client = ctx.block_client();
    let cf = convert_to_column_family(cf_str)?;
    tokio_spawn(async move {
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