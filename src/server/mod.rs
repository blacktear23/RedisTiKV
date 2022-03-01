pub mod server;
pub mod connection;
pub mod frame;
pub mod parser;
pub mod commands;

pub use self::server::self_server;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;