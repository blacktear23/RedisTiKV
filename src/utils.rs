use crate::{init::{GLOBAL_RT_FAST, ASYNC_EXECUTE_MODE}, commands::errors::AsyncResult};
use redis_module::{
    BlockedClient, Context, RedisValue, ThreadSafeContext,
    redisraw::bindings::RedisModule_GetClientId, RedisError, RedisResult, RedisModule_GetContextFlags, REDISMODULE_CTX_FLAGS_LUA,
};
use std::{future::Future, sync::Arc};
use tokio::{
    time::Duration, task::JoinHandle, sync::RwLock,
};

lazy_static! {
    static ref LUA_SCRIPT_LOCK: Arc<RwLock<()>> = Arc::new(RwLock::new(()));
}

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

// Response for redis blocked client
#[inline]
pub fn redis_resp(client: BlockedClient, result: AsyncResult<RedisValue>) {
    let ctx = ThreadSafeContext::with_blocked_client(client);
    match result {
        Ok(data) => {
            ctx.reply(Ok(data.into()));
        }
        Err(err) => {
            let err_msg = format!("{}", err);
            ctx.reply(Err(RedisError::String(err_msg)));
        }
    };
}

// Response for redis blocked client
pub fn redis_resp_with_ctx(ctx: &ThreadSafeContext<BlockedClient>, result: AsyncResult<RedisValue>) {
    match result {
        Ok(data) => {
            ctx.reply(Ok(data.into()));
        }
        Err(err) => {
            let err_msg = format!("{}", err);
            ctx.reply(Err(RedisError::String(err_msg)));
        }
    };
}

// Spawn async task from Redis Module main thread
pub fn tokio_spawn<T>(future: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    let hdl = unsafe { GLOBAL_RT_FAST.as_ref().unwrap() };
    hdl.spawn(future)
}

pub fn tokio_block_on<F: Future>(future: F) -> F::Output {
    let hdl = unsafe { GLOBAL_RT_FAST.as_ref().unwrap() };
    hdl.block_on(future)
}

#[inline]
fn is_block(ctx: &Context) -> (bool, bool) {
    let block = !unsafe { ASYNC_EXECUTE_MODE };
    let flags = get_context_flags(ctx);
    if (flags & REDISMODULE_CTX_FLAGS_LUA) == 1 {
        return (true, true);
    }
    (block, false)
}

pub fn async_execute<F>(ctx: &Context, future: F) -> RedisResult 
where
    F: Future<Output = AsyncResult<RedisValue>> + Send + 'static,
{
    let (sync_mode, in_script) = is_block(ctx);
    if sync_mode {
        let ret: AsyncResult<RedisValue>;
        if in_script {
            ret = tokio_block_on(async move {
                let _guard = LUA_SCRIPT_LOCK.write();
                future.await
            })
        } else {
            ret = tokio_block_on(async move {
                let _guard = LUA_SCRIPT_LOCK.read();
                future.await
            })
        }
        return match ret {
            Ok(val) => Ok(val),
            Err(err) => {
                let err_msg = format!("{}", err);
                Err(RedisError::String(err_msg))
            }
        }
    }
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let _guard = LUA_SCRIPT_LOCK.read();
        let res = future.await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}

#[inline]
pub fn get_client_id(ctx: &Context) -> u64 {
    unsafe { RedisModule_GetClientId.unwrap()(ctx.get_raw()) }
}

#[inline]
pub fn get_context_flags(ctx: &Context) -> u32 {
    unsafe { RedisModule_GetContextFlags.unwrap()(ctx.get_raw()) as u32 }
}

/* 
pub async fn proc_exec(command: String, args: Vec<String>) -> Result<String, Error> {
    let output = Command::new(command).args(&args).output().await?;
    match String::from_utf8(output.stdout) {
        Ok(stdout) => Ok(stdout),
        Err(err) => Err(Error::new(ErrorKind::Other, err.to_string())),
    }
}
*/

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
