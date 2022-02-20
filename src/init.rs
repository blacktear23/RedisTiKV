use crate::commands::asyncs::connection::{do_async_txn_connect, do_async_raw_connect, do_async_close};
use crate::metrics::prometheus_server;
use crate::{
    commands::*,
    metrics::INSTANCE_ID_GAUGER,
    try_redis_command,
};
use redis_module::{Context, RedisString, Status, ThreadSafeContext};
use std::sync::{Arc, RwLock};
use std::thread;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::time::{sleep, Duration};

lazy_static! {
    static ref GLOBAL_RUNNING: Arc<RwLock<u32>> = Arc::new(RwLock::new(1));
}

pub static mut GLOBAL_RT_FAST: Option<Box<Handle>> = None;
pub static mut ASYNC_EXECUTE_MODE: bool = true;

// Initial tokio main executor in other thread
pub fn tikv_init(ctx: &Context, args: &Vec<RedisString>) -> Status {
    let mut replace_system: bool = false;
    let mut replace_system_mode: String = String::from("nocache");
    let mut auto_connect: bool = false;
    let mut pd_addrs: String = String::from("");
    let mut enable_prometheus_http: bool = false;
    let mut threads: usize = 32;
    if args.len() > 0 {
        let mut start_pd_addrs = false;
        let mut start_instance_id = false;
        let mut start_threads = false;
        let mut start_replace_system = false;
        let mut start_execute_mode = false;
        args.into_iter().for_each(|s| {
            let ss = s.to_string();
            if ss == "replacesys" {
                replace_system = true;
                start_replace_system = true;
                return;
            }
            if start_replace_system {
                replace_system_mode = ss.clone();
                start_replace_system = false;
                return;
            }
            if ss == "pdaddrs" {
                auto_connect = true;
                start_pd_addrs = true;
                return;
            }
            if start_pd_addrs {
                pd_addrs = ss.clone();
                start_pd_addrs = false;
                return;
            }
            if ss == "instanceid" {
                start_instance_id = true;
                return;
            }
            if start_instance_id {
                let instance_id_str = ss.clone();
                match instance_id_str.parse::<u64>() {
                    Ok(val) => set_instance_id(val),
                    Err(_) => set_instance_id(0),
                };
                INSTANCE_ID_GAUGER.set(get_instance_id() as i64);
                start_instance_id = false;
                return;
            }
            if ss == "enablepromhttp" {
                enable_prometheus_http = true;
                return;
            }
            if ss == "threads" {
                start_threads = true;
                return;
            }
            if start_threads {
                let threads_str = ss.clone();
                match threads_str.parse::<u64>() {
                    Ok(val) => {
                        threads = val as usize;
                    }
                    Err(_) => {}
                };
                start_threads = false;
            }
            if ss == "execmode" {
                start_execute_mode = true;
                return
            }
            if start_execute_mode {
                start_execute_mode = false;
                let exec_mode = ss.clone();
                if exec_mode.eq("sync") {
                    unsafe {
                        ASYNC_EXECUTE_MODE = false;
                    }
                } else {
                    unsafe {
                        ASYNC_EXECUTE_MODE = true;
                    }
                }
            }
        });
    }

    thread::Builder::new()
        .name("tokio-worker-1".into())
        .spawn(move || {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(threads)
            .build()
            .unwrap();
        // let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        unsafe {
            GLOBAL_RT_FAST.replace(Box::new(handle));
        }
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
                match do_async_txn_connect(addrs.clone()).await {
                    Ok(_) => {
                        tctx.lock()
                            .log_notice(&format!("Connect to PD {} Success", pd_addrs));
                    }
                    Err(err) => {
                        tctx.lock()
                            .log_notice(&format!("Connect to PD {} error: {}", pd_addrs, err));
                    }
                }
                match do_async_raw_connect(addrs).await {
                    Ok(_) => {
                        tctx.lock()
                            .log_notice(&format!("Raw Client Connect to PD {} Success", pd_addrs));
                    }
                    Err(err) => {
                        tctx.lock().log_notice(&format!(
                            "Raw Client Connect to PD {} error: {}",
                            pd_addrs, err
                        ));
                    }
                }
            });
        }

        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                if *GLOBAL_RUNNING.read().unwrap() == 0 {
                    // Close connections
                    let _ = do_async_close().await;
                    println!("TiKV Connection Closed");
                    return;
                }
            }
        });
        println!("Tokio Runtime 1 Finished");
        runtime.shutdown_timeout(Duration::from_secs(10));
        println!("Tokio Runtime 1 Shutdown");
    }).unwrap();

    if enable_prometheus_http {
        thread::spawn(move || {
            let runtime = Runtime::new().unwrap();
            let tctx = ThreadSafeContext::new();
            tctx.lock().log_notice("Tokio Runtime Prometheus Created");
            runtime.block_on(async {
                match prometheus_server().await {
                    Ok(()) => {
                        tctx.lock().log_notice("Prometheus Server Stopped");
                    }
                    Err(err) => {
                        tctx.lock()
                            .log_notice(&format!("Prometheus Server Stopped with Error: {:}", err));
                    }
                };
            });
        });
    }

    if replace_system {
        let replace_default: bool;
        if replace_system_mode.eq("nocache") {
            replace_default = true;
            try_redis_command!(ctx, "get", tikv_raw_get, "", 0, 0, 0);
            try_redis_command!(ctx, "set", tikv_raw_set, "", 0, 0, 0);
            try_redis_command!(ctx, "del", tikv_raw_del, "", 0, 0, 0);
        } else if replace_system_mode.eq("cache") {
            replace_default = true;
            try_redis_command!(ctx, "get", tikv_raw_cached_get, "", 0, 0, 0);
            try_redis_command!(ctx, "set", tikv_raw_cached_set, "", 0, 0, 0);
            try_redis_command!(ctx, "del", tikv_raw_cached_del, "", 0, 0, 0);
        } else if replace_system_mode.eq("mock") {
            replace_default = true;
            try_redis_command!(ctx, "get", tikv_mock_get, "", 0, 0, 0);
            try_redis_command!(ctx, "set", tikv_raw_set, "", 0, 0, 0);
            try_redis_command!(ctx, "del", tikv_raw_del, "", 0, 0, 0);
        } else {
            replace_default = false;
            ctx.log_notice(&format!("Unknown Replace System Mode"))
        }
        if replace_default {
            try_redis_command!(ctx, "exists", tikv_raw_exists, "", 0, 0, 0);
            try_redis_command!(ctx, "mget", tikv_raw_batch_get, "", 0, 0, 0);
            try_redis_command!(ctx, "mset", tikv_raw_batch_set, "", 0, 0, 0);
            try_redis_command!(ctx, "incr", tikv_raw_incr, "", 0, 0, 0);
            try_redis_command!(ctx, "decr", tikv_raw_decr, "", 0, 0, 0);
        }
    }
    Status::Ok
}

pub fn tikv_deinit(ctx: &Context) -> Status {
    *GLOBAL_RUNNING.write().unwrap() = 0;
    ctx.log_notice("Set Running to False");
    Status::Ok
}
