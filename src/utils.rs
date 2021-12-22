use std::future::Future;
use redis_module::{RedisValue, ThreadSafeContext, BlockedClient };

pub use crate::init::{ GLOBAL_RT };

// Respose for redis blocked client
pub fn redis_resp<E>(client: BlockedClient, result: Result<RedisValue, E>)
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
pub fn tokio_spawn<T>(future: T)
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    let hdl = unsafe { GLOBAL_RT.as_ref().unwrap() };
    hdl.spawn(future);
}
