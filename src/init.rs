use std::thread;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, RwLock, Mutex};
use tikv_client::RawClient;
use redis_module::{Context, RedisString, ThreadSafeContext, Status };
use tokio::runtime::{ Runtime, Handle };
use crate::try_redis_command;
use crate::commands::{tikv_get, tikv_put, tikv_batch_get, tikv_batch_put, tikv_del, tikv_exists};

lazy_static! {
    pub static ref GLOBAL_RT1: Arc<RwLock<Option<Box<Handle>>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_RT2: Arc<RwLock<Option<Box<Handle>>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_COUNTER: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    pub static ref GLOBAL_CLIENT: Arc<RwLock<Option<Box<RawClient>>>> = Arc::new(RwLock::new(None));
    static ref GLOBAL_RUNNING: Arc<RwLock<u32>> = Arc::new(RwLock::new(1));
}

// Initial tokio main executor in other thread
pub fn tikv_init(ctx: &Context, args: &Vec<RedisString>) -> Status {
    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        GLOBAL_RT1.write().unwrap().replace(Box::new(handle));
        *GLOBAL_RUNNING.write().unwrap() = 1;
        let tctx = ThreadSafeContext::new();
        tctx.lock().log_notice("Tokio Runtime 1 Created");
        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                if *GLOBAL_RUNNING.read().unwrap() == 0 {
                    return;
                }
            }
        });
        tctx.lock().log_notice("Tokio Runtime 1 Finished");
        runtime.shutdown_timeout(Duration::from_secs(10));
        tctx.lock().log_notice("Tokio Runtime 1 Shutdown");
    });

    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        GLOBAL_RT2.write().unwrap().replace(Box::new(handle));
        *GLOBAL_RUNNING.write().unwrap() = 1;
        let tctx = ThreadSafeContext::new();
        tctx.lock().log_notice("Tokio Runtime 2 Created");
        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                if *GLOBAL_RUNNING.read().unwrap() == 0 {
                    return;
                }
            }
        });
        tctx.lock().log_notice("Tokio Runtime 2 Finished");
        runtime.shutdown_timeout(Duration::from_secs(10));
        tctx.lock().log_notice("Tokio Runtime 2 Shutdown");
    });

    if args.len() > 0 {
        let replace_system = args.into_iter().any(|s| {
            s.to_string() == "replacesys"
        });

        if replace_system {
            // Try to replace system command automatically
            try_redis_command!(ctx, "get", tikv_get, "", 0, 0, 0);
            try_redis_command!(ctx, "set", tikv_put, "", 0, 0, 0);
            try_redis_command!(ctx, "mget", tikv_batch_get, "", 0, 0, 0);
            try_redis_command!(ctx, "mset", tikv_batch_put, "", 0, 0, 0);
            try_redis_command!(ctx, "del", tikv_del, "", 0, 0, 0);
            try_redis_command!(ctx, "exists", tikv_exists, "", 0, 0, 0);
        }
    }
    Status::Ok
}

pub fn tikv_deinit(ctx: &Context) -> Status {
    *GLOBAL_RUNNING.write().unwrap() = 0;
    ctx.log_notice("Set Running to False");
    Status::Ok
}
