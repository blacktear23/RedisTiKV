use tokio::net::{TcpListener, TcpStream};
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
        let (sock, _) = listener.accept().await?;
        tokio_spawn(async move {
            match handle_connection(sock).await {
                Ok(_) => {}
                Err(e) => {
                    println!("Handle connection got error: {}", e.to_string());
                }
            }
        });
    }
}

async fn handle_connection(mut sock: TcpStream) -> Result<()> {
    let mut conn = Connection::new(sock);
    loop {
        let maybe_frame = conn.read_frame().await?;
        let frame = match maybe_frame {
            Some(frame) => frame,
            None => return Ok(()),
        };

        handle_frame(frame, &mut conn).await?;
    }
}

async fn handle_frame(frame: Frame, conn: &mut Connection) -> Result<()>  {
    let mut parse = Parse::new(frame)?;
    let cmd_name = parse.next_string()?.to_lowercase();
    process_command(cmd_name, &mut parse, conn).await?;
    Ok(())
}