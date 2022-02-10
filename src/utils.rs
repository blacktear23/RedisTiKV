pub use crate::init::GLOBAL_RT;
use redis_module::{
    BlockedClient, Context, RedisValue, ThreadSafeContext,
    redisraw::bindings::RedisModule_GetClientId,
};
use std::future::Future;
use tokio::{
    io::{Error, ErrorKind},
    process::Command,
    time::Duration,
};

pub fn resp_ok() -> RedisValue {
    RedisValue::SimpleStringStatic("OK")
}

pub fn resp_sstr(val: &'static str) -> RedisValue {
    RedisValue::SimpleStringStatic(val)
}

pub fn resp_int(val: i64) -> RedisValue {
    RedisValue::Integer(val)
}

pub async fn sleep(ms: u32) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

// Respose for redis blocked client
pub fn redis_resp<E>(client: BlockedClient, result: Result<RedisValue, E>)
where
    E: std::error::Error,
{
    let ctx = ThreadSafeContext::with_blocked_client(client);
    match result {
        Ok(data) => {
            ctx.lock().reply(Ok(data.into()));
        }
        Err(err) => {
            let err_msg = format!("{}", err);
            ctx.lock().reply_error_string(&err_msg);
        }
    };
}

// Respose for redis blocked client
pub fn redis_resp_with_ctx<E>(ctx: &ThreadSafeContext<BlockedClient>, result: Result<RedisValue, E>)
where
    E: std::error::Error,
{
    match result {
        Ok(data) => {
            ctx.lock().reply(Ok(data.into()));
        }
        Err(err) => {
            let err_msg = format!("{}", err);
            ctx.lock().reply_error_string(&err_msg);
        }
    };
}

// Spawn async task from Redis Module main thread
pub fn tokio_spawn<T>(future: T)
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    let tmp = GLOBAL_RT.read().unwrap();
    let hdl = tmp.as_ref().unwrap();
    hdl.spawn(future);
}

pub fn get_client_id(ctx: &Context) -> u64 {
    unsafe { RedisModule_GetClientId.unwrap()(ctx.get_raw()) }
}

pub async fn proc_exec(command: String, args: Vec<String>) -> Result<String, Error> {
    let output = Command::new(command).args(&args).output().await?;
    match String::from_utf8(output.stdout) {
        Ok(stdout) => Ok(stdout),
        Err(err) => Err(Error::new(ErrorKind::Other, err.to_string())),
    }
}

// Try to register a redis command, if got error, just log a warning.
#[macro_export]
macro_rules! try_redis_command {
    ($ctx:expr,
     $command_name:expr,
     $command_handler:expr,
     $command_flags:expr,
     $firstkey:expr,
     $lastkey:expr,
     $keystep:expr) => {{
        let name = std::ffi::CString::new($command_name).unwrap();
        let flags = std::ffi::CString::new($command_flags).unwrap();

        /////////////////////
        extern "C" fn __do_command(
            ctx: *mut redis_module::RedisModuleCtx,
            argv: *mut *mut redis_module::RedisModuleString,
            argc: std::os::raw::c_int,
        ) -> std::os::raw::c_int {
            let context = Context::new(ctx);

            let args = redis_module::decode_args(ctx, argv, argc);
            let response = $command_handler(&context, args);
            context.reply(response) as std::os::raw::c_int
        }
        /////////////////////

        if unsafe {
            redis_module::RedisModule_CreateCommand.unwrap()(
                $ctx.ctx,
                name.as_ptr(),
                Some(__do_command),
                flags.as_ptr(),
                $firstkey,
                $lastkey,
                $keystep,
            )
        } == redis_module::raw::Status::Err as std::os::raw::c_int
        {
            $ctx.log_warning(&format!("Unable define command: {}", $command_name));
        }
    }};
}
