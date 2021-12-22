
use std::thread;
use tokio::time::{sleep, Duration};
use std::sync::{Arc, Mutex};
use tikv_client::RawClient;
use redis_module::{Context, RedisString, Status };
use tokio::runtime::{ Runtime, Handle };

pub static mut GLOBAL_RUNNING: Option<Arc<Mutex<u32>>> = None;
pub static mut GLOBAL_RT: Option<Handle> = None;
pub static mut GLOBAL_CLIENT: Option<RawClient> = None;

// Initial tokio main executor in other thread
pub fn tikv_init(_ctx: &Context, _args: &Vec<RedisString>) -> Status {
    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let running =  Arc::new(Mutex::new(1));
        unsafe {
            GLOBAL_RT.replace(handle);
            GLOBAL_RUNNING.replace(running);
        }
        println!("Tokio Runtime Created!");
        runtime.block_on(async {
            loop {
                sleep(Duration::from_secs(1)).await;
                let running = unsafe { *GLOBAL_RUNNING.as_ref().unwrap().lock().unwrap() };
                if running == 0 {
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
    unsafe {
        let data = GLOBAL_RUNNING.as_ref().unwrap();
        *data.lock().unwrap() = 0;
    };
    println!("Set Runnint to False");
    Status::Ok
}
