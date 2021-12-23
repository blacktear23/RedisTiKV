use std::thread;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, Mutex};
use tikv_client::RawClient;
use redis_module::{Context, RedisString, Status };
use tokio::runtime::{ Runtime, Handle };

lazy_static! {
    pub static ref GLOBAL_RT: Arc<Mutex<Option<Box<Handle>>>> = Arc::new(Mutex::new(None));
    pub static ref GLOBAL_CLIENT: Arc<Mutex<Option<Box<RawClient>>>> = Arc::new(Mutex::new(None));
    static ref GLOBAL_RUNNING: Arc<Mutex<u32>> = Arc::new(Mutex::new(1));
}

// Initial tokio main executor in other thread
pub fn tikv_init(_ctx: &Context, _args: &Vec<RedisString>) -> Status {
    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        GLOBAL_RT.lock().unwrap().replace(Box::new(handle));
        *GLOBAL_RUNNING.lock().unwrap() = 1;
        println!("Tokio Runtime Created!");
        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                if *GLOBAL_RUNNING.lock().unwrap() == 0 {
                    return;
                }
            }
        });
        println!("Tokio Runtime Finished");
        runtime.shutdown_timeout(Duration::from_secs(10));
        println!("Tokio Runtime Shutdown");
    });

    Status::Ok
}

pub fn tikv_deinit(_ctx: &Context) -> Status {
    *GLOBAL_RUNNING.lock().unwrap() = 0;
    println!("Set Runnint to False");
    Status::Ok
}
