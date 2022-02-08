use gperftools::profiler::PROFILER;
use redis_module::{RedisResult, NextArg, RedisError, RedisString, Context};
use crate::utils::resp_ok;

static mut PROFILE_STARTED: bool = false;

pub fn tikv_pprof_start(_ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    if unsafe {PROFILE_STARTED} {
        return Err(RedisError::String(String::from("Already started")));
    }
    if args.len() < 1 {
        return Err(RedisError::WrongArity);
    }
    let mut args = args.into_iter().skip(1);
    let fname = args.next_str()?;
    let prof_fname = format!("/tmp/{}.prof", fname); 
    unsafe {PROFILE_STARTED = true};
    PROFILER.lock().unwrap().start(prof_fname).unwrap();
    Ok(resp_ok())
}

pub fn tikv_pprof_finish(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    if ! unsafe {PROFILE_STARTED} {
        return Err(RedisError::String(String::from("Not Started")));
    }
    PROFILER.lock().unwrap().stop().unwrap();
    unsafe {PROFILE_STARTED = false};
    Ok(resp_ok())
}