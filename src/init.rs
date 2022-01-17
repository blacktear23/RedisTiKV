use crate::{
    pd::pd_ctl,
    tidb::commands::{do_async_mysql_close, do_async_mysql_connect},
    tikv::{
        get_instance_id,
        init::{do_async_close, do_async_connect, do_async_rawkv_connect},
        metrics::INSTANCE_ID_GAUGER,
        prometheus_server, set_instance_id, tikv_batch_get, tikv_batch_put, tikv_cached_del,
        tikv_cached_get, tikv_cached_put, tikv_ctl, tikv_del, tikv_exists, tikv_get, tikv_put,
        tikv_rawkv_del, tikv_rawkv_get, tikv_rawkv_put,
    },
    try_redis_command,
};
use redis_module::{Context, RedisString, Status, ThreadSafeContext};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use tokio::runtime::{Handle, Runtime};
use tokio::time::{sleep, Duration};

lazy_static! {
    pub static ref GLOBAL_RT: Arc<RwLock<Option<Box<Handle>>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_COUNTER: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    static ref GLOBAL_RUNNING: Arc<RwLock<u32>> = Arc::new(RwLock::new(1));
    pub static ref BIN_PATH: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
}

// Initial tokio main executor in other thread
pub fn tikv_init(ctx: &Context, args: &Vec<RedisString>) -> Status {
    let mut replace_system: bool = false;
    let mut replace_system_with_cached_api: bool = false;
    let mut replace_system_with_rawkv: bool = false;
    let mut auto_connect: bool = false;
    let mut auto_connect_mysql: bool = false;
    let mut pd_addrs: String = String::from("");
    let mut mysql_url: String = String::from("");
    let mut support_binary: bool = false;
    let mut bin_path: String = String::from("");
    let mut enable_prometheus_http: bool = false;
    if args.len() > 0 {
        let mut start_pd_addrs = false;
        let mut start_mysql_addrs = false;
        let mut start_bin_path = false;
        let mut start_instance_id = false;
        args.into_iter().for_each(|s| {
            let ss = s.to_string();
            if ss == "replacesys" {
                replace_system = true;
                replace_system_with_rawkv = false;
                replace_system_with_cached_api = false;
                return;
            }
            if ss == "replacesyscache" {
                replace_system = true;
                replace_system_with_rawkv = false;
                replace_system_with_cached_api = true;
                return;
            }
            if ss == "replacesysrawkv" {
                replace_system = true;
                replace_system_with_rawkv = true;
                replace_system_with_cached_api = false;
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
                return;
            }
            if ss == "autoconnmysql" {
                auto_connect_mysql = true;
                start_mysql_addrs = true;
                return;
            }
            if start_mysql_addrs {
                mysql_url = ss.clone();
                start_mysql_addrs = false;
                return;
            }
            if ss == "binpath" {
                start_bin_path = true;
                support_binary = true;
                return;
            }
            if start_bin_path {
                bin_path = ss.clone();
                start_bin_path = false;
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
        });
    }

    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        GLOBAL_RT.write().unwrap().replace(Box::new(handle));
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
                let ret = do_async_connect(addrs.clone()).await;
                match ret {
                    Ok(_) => {
                        tctx.lock()
                            .log_notice(&format!("Connect to PD {} Success", pd_addrs));
                    }
                    Err(err) => {
                        tctx.lock()
                            .log_notice(&format!("Connect to PD {} error: {}", pd_addrs, err));
                    }
                }
                let ret2 = do_async_rawkv_connect(addrs).await;
                match ret2 {
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

        if auto_connect_mysql && mysql_url != "" {
            tctx.lock().log_notice("Auto connect to MySQL");
            runtime.block_on(async {
                let ret = do_async_mysql_connect(&mysql_url.clone()).await;
                match ret {
                    Ok(_) => {
                        tctx.lock()
                            .log_notice(&format!("Connect to MySQL {} Success", mysql_url));
                    }
                    Err(err) => {
                        tctx.lock()
                            .log_notice(&format!("Connect to MySQL {} error: {}", mysql_url, err));
                    }
                }
            });
        }

        if bin_path != "" {
            BIN_PATH.write().unwrap().replace(bin_path);
        }

        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                if *GLOBAL_RUNNING.read().unwrap() == 0 {
                    // Close connections
                    let _ = do_async_mysql_close().await;
                    println!("MySQL Connection Closed");
                    let _ = do_async_close().await;
                    println!("TiKV Connection Closed");
                    return;
                }
            }
        });
        println!("Tokio Runtime 1 Finished");
        runtime.shutdown_timeout(Duration::from_secs(10));
        println!("Tokio Runtime 1 Shutdown");
    });

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
        // Try to replace system command automatically
        if replace_system_with_cached_api {
            try_redis_command!(ctx, "get", tikv_cached_get, "", 0, 0, 0);
            try_redis_command!(ctx, "set", tikv_cached_put, "", 0, 0, 0);
            try_redis_command!(ctx, "del", tikv_cached_del, "", 0, 0, 0);
        } else if replace_system_with_rawkv {
            try_redis_command!(ctx, "get", tikv_rawkv_get, "", 0, 0, 0);
            try_redis_command!(ctx, "set", tikv_rawkv_put, "", 0, 0, 0);
            try_redis_command!(ctx, "del", tikv_rawkv_del, "", 0, 0, 0);
        } else {
            try_redis_command!(ctx, "get", tikv_get, "", 0, 0, 0);
            try_redis_command!(ctx, "set", tikv_put, "", 0, 0, 0);
            try_redis_command!(ctx, "del", tikv_del, "", 0, 0, 0);
        }
        try_redis_command!(ctx, "mget", tikv_batch_get, "", 0, 0, 0);
        try_redis_command!(ctx, "mset", tikv_batch_put, "", 0, 0, 0);
        try_redis_command!(ctx, "exists", tikv_exists, "", 0, 0, 0);
    }

    if support_binary {
        try_redis_command!(ctx, "tikv.ctl", tikv_ctl, "", 0, 0, 0);
        try_redis_command!(ctx, "pd.ctl", pd_ctl, "", 0, 0, 0);
    }

    Status::Ok
}

pub fn tikv_deinit(ctx: &Context) -> Status {
    *GLOBAL_RUNNING.write().unwrap() = 0;
    ctx.log_notice("Set Running to False");
    Status::Ok
}
