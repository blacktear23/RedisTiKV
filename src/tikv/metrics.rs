use prometheus::*;
use redis_module::{Context, RedisResult, RedisValue, RedisString};
use crate::get_instance_id;

lazy_static! {
    pub static ref REQUEST_COUNTER: IntCounter =
        register_int_counter!("redistikv_requests", "Request counter").unwrap();
    pub static ref REQUEST_CMD_COUNTER: IntCounterVec = 
        register_int_counter_vec!("redistikv_command_requests", "Request command counter",
            &["cmd"]).unwrap();
}

pub fn tikv_status(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let info = format!("instance_id:{}\nrequests:{}\nget:{}\nset:{}\nmget:{}\nmset:{}\ndel:{}\nexists:{}\nscan:{}",
        get_instance_id(),
        REQUEST_COUNTER.get(),
        REQUEST_CMD_COUNTER.with_label_values(&["get"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["set"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["mget"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["mset"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["del"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["exists"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["scan"]).get(),
    );
    Ok(RedisValue::SimpleString(info))
}