use super::{frame::Frame, parser::{Parse, ParseError}, connection::Connection};
use crate::{server::Result, commands::asyncs::get_client, encoding::KeyEncoder, metrics::{REQUEST_CMD_COUNTER, REQUEST_COUNTER, REQUEST_CMD_FINISH_COUNTER}};

pub async fn process_command(cmd: String, parse: &mut Parse, conn: &mut Connection) -> Result<()> {
    let mut check_finish = true;
    let frame = match &cmd[..] {
        "get" => {
            cmd_get(parse, conn).await
        }
        "set" => {
            cmd_set(parse, conn).await
        }
        "ping" => {
            cmd_ping(parse, conn).await
        }
        _ => {
            check_finish = false;
            cmd_unknown(cmd.clone(), parse, conn).await
        }
    };
    REQUEST_COUNTER.inc();
    REQUEST_CMD_COUNTER.with_label_values(&[&cmd]).inc();

    if check_finish {
        match parse.finish() {
            Err(err) => {
                let err_frame = Frame::Error(format!("ERR {}", err.to_string()));
                conn.write_frame_buf(&err_frame).await?;
                return Ok(());
            }
            Ok(()) => {}
        }
    }

    match frame {
        Ok(frame) => {
            conn.write_frame_buf(&frame).await?;
        }
        Err(err) => {
            let err_frame = Frame::Error(format!("ERR {}", err.to_string()));
            conn.write_frame_buf(&err_frame).await?;
        }
    }
    REQUEST_CMD_FINISH_COUNTER.with_label_values(&[&cmd]).inc();

    Ok(())
}

async fn cmd_get(parse: &mut Parse, _conn: &mut Connection) -> Result<Frame> {
    let key = parse.next_string()?;
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(&key);
    let val = client.get(ekey).await?;
    match val {
        Some(val) => Ok(Frame::Bulk(val.into())),
        None => Ok(Frame::Null),
    }
}

async fn cmd_set(parse: &mut Parse, _conn: &mut Connection) -> Result<Frame> {
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

async fn cmd_unknown(cmd: String, _parse: &mut Parse, _conn: &mut Connection) -> Result<Frame> {
    println!("Err Unknown Command {}", &cmd);
    Err(format!("Unknown Command {}", cmd).into())
}

async fn cmd_ping(parse: &mut Parse, _conn: &mut Connection) -> Result<Frame> {
    match parse.next_string() {
        Ok(msg) => {
            Ok(Frame::Bulk(msg.into()))
        }
        Err(ParseError::EndOfStream) => {
            Ok(Frame::Simple("PONG".to_string()))
        }
        Err(e) => {
            Err(e.into())
        }
    }
}