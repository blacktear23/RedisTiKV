use std::time::Duration;

use async_std::net::{TcpListener, TcpStream};
use tokio::time;
use crate::metrics::CURRENT_CONNECTION_COUNTER;
use crate::utils::tokio_spawn;

use super::commands::process_command;
use super::connection::Connection;
use super::frame::Frame;
use super::parser::Parse;
use super::Result;

pub async fn self_server(port: u16) -> Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    println!("Listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    loop {
        let sock = do_accept(&listener).await?;
        CURRENT_CONNECTION_COUNTER.inc();
        tokio_spawn(async move {
            if let Err(e) = handle_connection(sock).await {
                println!("Handle connection got error: {}", e.to_string());
            }
            CURRENT_CONNECTION_COUNTER.dec();
        });
    }
}

async fn do_accept(listener: &TcpListener) -> Result<TcpStream> {
    let mut backoff = 1;
    loop {
        match listener.accept().await {
            Ok((socket, _)) => return Ok(socket),
            Err(err) => {
                println!("Accept Error! {:?}", &err);
                if backoff > 32 {
                    return Err(err.into())
                }
            }
        }

        // Pause execution until the back off period elapses.
        time::sleep(Duration::from_secs(backoff)).await;

        // Double the back off
        backoff *= 2;
    }
}

async fn handle_connection(sock: TcpStream) -> Result<()> {
    let mut conn = Connection::new(sock);
    loop {
        let maybe_frame = tokio::select! {
            res = conn.read_request() => res?,
        };
        /* 
        let maybe_frame = tokio::select! {
            res = conn.read_frame() => res?,
        };
        */
        let frame = match maybe_frame {
            Some(frame) => frame,
            None => {
                println!("Alert Got None Command");
                return Ok(());
            }
        };
        // println!("{:?}", &frame);
        handle_frame(frame, &mut conn).await?;
    }
}

async fn handle_frame(frame: Frame, conn: &mut Connection) -> Result<()>  {
    let mut parse = Parse::new(frame)?;
    let cmd_name = parse.next_string()?.to_lowercase();
    process_command(cmd_name, &mut parse, conn).await?;
    Ok(())
}