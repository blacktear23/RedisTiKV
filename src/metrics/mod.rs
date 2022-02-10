use crate::get_instance_id;
use prometheus::{IntCounter, IntCounterVec, IntGauge};

mod http;

pub use self::http::prometheus_server;

lazy_static! {
    pub static ref INSTANCE_ID_GAUGER: IntGauge =
        register_int_gauge!("redistikv_instance_id", "Instance ID").unwrap();
    pub static ref REQUEST_COUNTER: IntCounter =
        register_int_counter!("redistikv_requests", "Request counter").unwrap();
    pub static ref REQUEST_CMD_COUNTER: IntCounterVec = register_int_counter_vec!(
        "redistikv_command_requests",
        "Request command counter",
        &["cmd"]
    )
    .unwrap();
}

pub fn get_info_string() -> String {
    let info = format!(
        "instance_id:{}\n\
        requests:{}\n\
        get:{}\n\
        set:{}\n\
        incr:{}\n\
        decr:{}\n\
        mget:{}\n\
        mset:{}\n\
        setnx:{}\n\
        del:{}\n\
        exists:{}\n\
        scan:{}\n\
        hget:{}\n\
        hset:{}\n\
        hgetall:{}\n\
        hmset:{}\n\
        hmget:{}\n\
        hkeys:{}\n\
        hvals:{}\n\
        hexists:{}\n\
        hdel:{}",
        get_instance_id(),
        REQUEST_COUNTER.get(),
        REQUEST_CMD_COUNTER.with_label_values(&["get"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["set"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["incr"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["decr"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["mget"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["mset"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["setnx"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["del"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["exists"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["scan"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hget"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hset"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hgetall"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hmset"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hmget"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hkeys"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hvals"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hexists"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["hdel"]).get(),
    );
    return info;
}