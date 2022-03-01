use super::{frame::Frame, parser::Parse, connection::Connection};
use crate::{server::Result, commands::asyncs::get_client, encoding::KeyEncoder};

pub async fn process_command(cmd: String, parse: &mut Parse, conn: &mut Connection) -> Result<()> {
    let frame = match &cmd[..] {
        "get" => {
            cmd_get(parse, conn).await
        }
        "set" => {
            cmd_set(parse, conn).await
        }
        _ => {
            cmd_unknown(cmd, parse, conn).await
        }
    };
    match frame {
        Ok(frame) => {
            conn.write_frame(&frame).await?;
        }
        Err(err) => {
            let err_frame = Frame::Error(format!("ERR {}", err.to_string()));
            conn.write_frame(&err_frame).await?;
        }
    }
    Ok(())
}

async fn cmd_get(parse: &mut Parse, conn: &mut Connection) -> Result<Frame> {
    let key = parse.next_string()?;
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(&key);
    let val = client.get(ekey).await?;
    match val {
        Some(val) => Ok(Frame::Bulk(val.into())),
        None => Ok(Frame::Null),
    }
}

async fn cmd_set(parse: &mut Parse, conn: &mut Connection) -> Result<Frame> {
    let key = parse.next_string()?;
    let value = parse.next_bytes()?;
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(&key);
    let (_, swapped) = client.compare_and_swap(ekey, None.into(), value.to_vec()).await?;
    if swapped {
        Ok(Frame::Integer(1))
    } else {
        Ok(Frame::Integer(0))
    } 
}

async fn cmd_unknown(cmd: String, _parse: &mut Parse, conn: &mut Connection) -> Result<Frame> {
    Err(format!("Unknown Command {}", cmd).into())
}