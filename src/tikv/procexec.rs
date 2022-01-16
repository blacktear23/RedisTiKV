use crate::utils::{get_binary_path, proc_exec, redis_resp, tokio_spawn};
use redis_module::{Context, RedisResult, RedisString, RedisValue};
use tokio::io::{Error, ErrorKind};

fn has_bin_path() -> Result<(), Error> {
    if get_binary_path() == "" {
        return Err(Error::new(ErrorKind::Other, "Binary path not set"));
    }
    Ok(())
}

fn format_output(output: String) -> RedisValue {
    output.split("\n").collect::<Vec<&str>>().into()
}

pub async fn do_async_tikv_ctl(args: Vec<String>) -> Result<RedisValue, Error> {
    let path = get_binary_path();
    let cmd = format!("{}/{}", path, "tikv-ctl");
    let output = proc_exec(cmd, args).await?;
    Ok(format_output(output))
}

pub fn tikv_ctl(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    has_bin_path()?;
    let args = args.into_iter().skip(1);
    let cmd_args: Vec<String> = args.map(|i| i.to_string_lossy()).collect();
    let blocked_client = ctx.block_client();
    tokio_spawn(async move {
        let res = do_async_tikv_ctl(cmd_args).await;
        redis_resp(blocked_client, res);
    });
    Ok(RedisValue::NoReply)
}
