use redis_module::{Context, RedisString, RedisResult, RedisValue};
use crate::metrics::get_info_string;

pub fn tikv_status(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let info = get_info_string();
    Ok(RedisValue::SimpleString(info))
}