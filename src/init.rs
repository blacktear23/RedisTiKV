use std::thread;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, RwLock, Mutex};
use redis_module::{Context, RedisString, ThreadSafeContext, Status };
use tokio::runtime::{ Runtime, Handle };
use crate::{
    try_redis_command,
    tidb::commands::do_async_mysql_connect,
    tikv::{
        init::do_async_connect,
        tikv_get, tikv_put, tikv_batch_get, tikv_batch_put, tikv_del, tikv_exists,
    },
};

lazy_static! {
    pub static ref GLOBAL_RT1: Arc<RwLock<Option<Box<Handle>>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_RT2: Arc<RwLock<Option<Box<Handle>>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_COUNTER: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    static ref GLOBAL_RUNNING: Arc<RwLock<u32>> = Arc::new(RwLock::new(1));
}

// Initial tokio main executor in other thread
pub fn tikv_init(ctx: &Context, args: &Vec<RedisString>) -> Status {
    let mut replace_system: bool = false;
    let mut auto_connect: bool = false;
    let mut auto_connect_mysql: bool = false;
    let mut pd_addrs: String = String::from("");
    let mut mysql_url: String = String::from("");
    if args.len() > 0 {
        let mut start_pd_addrs = false;
        let mut start_mysql_addrs = false;
        args.into_iter().for_each(|s| {
            let ss = s.to_string();
            if ss == "replacesys" {
                replace_system = true;
                return;
            }
            if ss == "autoconn" {
                auto_connect = true;
                start_pd_addrs = true;
                return;
            }
            if start_pd_addrs {
                pd_addrs = ss.clone();
                start_pd_addrs = false;
            }
            if ss == "autoconnmysql" {
                auto_connect_mysql = true;
                start_mysql_addrs = true;
                return;
            }
            if start_mysql_addrs {
                mysql_url = ss.clone();
                start_mysql_addrs = false;
            }
        });
    }

    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        GLOBAL_RT1.write().unwrap().replace(Box::new(handle));
        *GLOBAL_RUNNING.write().unwrap() = 1;
        let tctx = ThreadSafeContext::new();
        tctx.lock().log_notice("Tokio Runtime 1 Created");

        if auto_connect && pd_addrs != "" {
            tctx.lock().log_notice("Auto connect to PD");
            runtime.block_on(async {
                let mut addrs: Vec<String> = Vec::new();
                pd_addrs.split(",").for_each(|s| {
                    addrs.push(s.to_string());
                });
                let ret = do_async_connect(addrs).await;
                match ret {
                    Ok(_) => {
                        tctx.lock().log_notice(&format!("Connect to PD {} Success", pd_addrs));
                    },
                    Err(err) => {
                        tctx.lock().log_notice(&format!("Connect to PD {} error: {}", pd_addrs, err));
                    },
                }
            });
        }

        if auto_connect_mysql && mysql_url != "" {
            tctx.lock().log_notice("Auto connect to MySQL");
            runtime.block_on(async {
                let ret = do_async_mysql_connect(&mysql_url.clone()).await;
                match ret {
                    Ok(_) => {
                        tctx.lock().log_notice(&format!("Connect to MySQL {} Success", mysql_url));
                    },
                    Err(err) => {
                        tctx.lock().log_notice(&format!("Connect to MySQL {} error: {}", mysql_url, err));
                    },
                }
            });
        }

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

    if replace_system {
        // Try to replace system command automatically
        try_redis_command!(ctx, "get", tikv_get, "", 0, 0, 0);
        try_redis_command!(ctx, "set", tikv_put, "", 0, 0, 0);
        try_redis_command!(ctx, "mget", tikv_batch_get, "", 0, 0, 0);
        try_redis_command!(ctx, "mset", tikv_batch_put, "", 0, 0, 0);
        try_redis_command!(ctx, "del", tikv_del, "", 0, 0, 0);
        try_redis_command!(ctx, "exists", tikv_exists, "", 0, 0, 0);
    }

    Status::Ok
}

pub fn tikv_deinit(ctx: &Context) -> Status {
    *GLOBAL_RUNNING.write().unwrap() = 0;
    ctx.log_notice("Set Running to False");
    Status::Ok
}
