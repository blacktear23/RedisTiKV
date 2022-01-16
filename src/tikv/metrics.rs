use prometheus::{IntCounter, Encoder, TextEncoder, IntCounterVec, IntGauge};
use redis_module::{Context, RedisResult, RedisValue, RedisString};
use crate::get_instance_id;
use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};

lazy_static! {
    pub static ref INSTANCE_ID_GAUGER: IntGauge = 
        register_int_gauge!("redistikv_instance_id", "Instance ID").unwrap();
    pub static ref REQUEST_COUNTER: IntCounter =
        register_int_counter!("redistikv_requests", "Request counter").unwrap();
    pub static ref REQUEST_CMD_COUNTER: IntCounterVec = 
        register_int_counter_vec!("redistikv_command_requests", "Request command counter",
            &["cmd"]).unwrap();
}

fn get_info_string() -> String {
    let info = format!(
        "instance_id:{}\n\
        requests:{}\n\
        get:{}\n\
        set:{}\n\
        mget:{}\n\
        mset:{}\n\
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
        REQUEST_CMD_COUNTER.with_label_values(&["mget"]).get(),
        REQUEST_CMD_COUNTER.with_label_values(&["mset"]).get(),
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

pub fn tikv_status(_ctx: &Context, _args: Vec<RedisString>) -> RedisResult {
    let info = get_info_string(); 
    Ok(RedisValue::SimpleString(info))
}

async fn serve_req(_r: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

pub async fn prometheus_server() -> Result<(), hyper::Error> {
    let addr = ([127, 0, 0, 1], 9898).into();
    println!("Listening on http://{}", addr);

    // gather all metrics to hold the data
    let _ = get_info_string(); 
    let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_req))
    }));

    serve_future.await?;
    Ok(())
}