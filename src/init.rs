use std::thread;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, RwLock, Mutex};
use tikv_client::RawClient;
use redis_module::{Context, RedisString, Status };
use tokio::runtime::{ Runtime, Handle };

lazy_static! {
    pub static ref GLOBAL_RT1: Arc<RwLock<Option<Box<Handle>>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_RT2: Arc<RwLock<Option<Box<Handle>>>> = Arc::new(RwLock::new(None));
    pub static ref GLOBAL_COUNTER: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    pub static ref GLOBAL_CLIENT: Arc<RwLock<Option<Box<RawClient>>>> = Arc::new(RwLock::new(None));
    static ref GLOBAL_RUNNING: Arc<RwLock<u32>> = Arc::new(RwLock::new(1));
}

// Initial tokio main executor in other thread
pub fn tikv_init(_ctx: &Context, _args: &Vec<RedisString>) -> Status {
    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        GLOBAL_RT1.write().unwrap().replace(Box::new(handle));
        *GLOBAL_RUNNING.write().unwrap() = 1;
        println!("Tokio Runtime 1 Created!");
        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                if *GLOBAL_RUNNING.read().unwrap() == 0 {
                    return;
                }
            }
        });
        println!("Tokio Runtime 1 Finished");
        runtime.shutdown_timeout(Duration::from_secs(10));
        println!("Tokio Runtime 1 Shutdown");
    });

    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        GLOBAL_RT2.write().unwrap().replace(Box::new(handle));
        *GLOBAL_RUNNING.write().unwrap() = 1;
        println!("Tokio Runtime 2 Created!");
        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                if *GLOBAL_RUNNING.read().unwrap() == 0 {
                    return;
                }
            }
        });
        println!("Tokio Runtime 2 Finished");
        runtime.shutdown_timeout(Duration::from_secs(10));
        println!("Tokio Runtime 2 Shutdown");
    });

    Status::Ok
}

pub fn tikv_deinit(_ctx: &Context) -> Status {
    *GLOBAL_RUNNING.write().unwrap() = 0;
    println!("Set Runnint to False");
    Status::Ok
}
