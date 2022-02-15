use crate::{
    commands::{PD_ADDRS, TIKV_RAW_CLIENT, TIKV_TNX_CONN_POOL, errors::AsyncResult, TIKV_RAW_CLIENT_2},
    utils::{resp_ok, resp_sstr},
};
use redis_module::RedisValue;
use tikv_client::RawClient;

pub async fn do_async_txn_connect(addrs: Vec<String>) -> AsyncResult<RedisValue> {
    PD_ADDRS.write().unwrap().replace(addrs.clone());
    Ok(resp_ok())
}

pub async fn do_async_raw_connect(addrs: Vec<String>) -> AsyncResult<RedisValue> {
    let client = RawClient::new(addrs.clone(), None).await?;
    unsafe {
        TIKV_RAW_CLIENT.replace(client);
    };
    let client_2 = RawClient::new(addrs, None).await?;
    unsafe {
        TIKV_RAW_CLIENT_2.replace(client_2);
    }
    Ok(resp_ok())
}

pub async fn do_async_close() -> AsyncResult<RedisValue> {
    *PD_ADDRS.write().unwrap() = None;
    let mut pool = TIKV_TNX_CONN_POOL.lock().unwrap();
    for _i in 0..pool.len() {
        let client = pool.pop_front();
        drop(client);
    }
    Ok(resp_sstr("Closed"))
}

pub async fn do_async_connect(addrs: Vec<String>) -> AsyncResult<RedisValue> {
    do_async_txn_connect(addrs.clone()).await?;
    do_async_raw_connect(addrs).await
}