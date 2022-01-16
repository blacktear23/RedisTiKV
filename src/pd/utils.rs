use crate::tikv::PD_ADDRS;
use redis_module::RedisValue;
use reqwest::{Client, Error as ReqError};
use serde_json::{to_string_pretty, Value};
use std::{error, fmt, fmt::Display};

#[derive(Debug)]
pub enum Error {
    String(StringError),
    Reqwest(ReqError),
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error::String(StringError::new(&msg))
    }
}

impl From<ReqError> for Error {
    fn from(err: ReqError) -> Self {
        Error::Reqwest(err)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            // Both underlying errors already impl `Display`, so we defer to
            // their implementations.
            Error::String(ref err) => write!(f, "{}", err),
            Error::Reqwest(ref err) => write!(f, "{}", err),
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            // N.B. Both of these implicitly cast `err` from their concrete
            // types (either `&io::Error` or `&num::ParseIntError`)
            // to a trait object `&Error`. This works because both error types
            // implement `Error`.
            Error::String(ref err) => Some(err),
            Error::Reqwest(ref err) => Some(err),
        }
    }
}

#[derive(Debug)]
pub struct StringError {
    message: String,
}

impl StringError {
    pub fn new(message: &str) -> StringError {
        StringError {
            message: String::from(message),
        }
    }
}

impl<'a> Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Store error: {}", self.message)
    }
}

impl<'a> error::Error for StringError {
    fn description(&self) -> &str {
        self.message.as_str()
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        None
    }
}

pub fn generate_pd_url(pd_addr: &str, func: &str) -> String {
    format!("http://{}/pd/api/v1/{}", pd_addr, func)
}

pub fn format_json(json_data: String) -> Result<RedisValue, Error> {
    let json_val: Value = match serde_json::from_str(&json_data) {
        Ok(val) => val,
        Err(err) => {
            return Err(Error::from(err.to_string()));
        }
    };
    match to_string_pretty(&json_val) {
        Ok(val) => Ok(val
            .replace("\"", "'")
            .split("\n")
            .collect::<Vec<&str>>()
            .into()),
        Err(err) => Err(Error::from(err.to_string())),
    }
}

pub fn get_pd_addr() -> Result<String, Error> {
    let guard = PD_ADDRS.read().unwrap();
    if guard.is_none() {
        return Err(Error::from(String::from("PD addresses not set!")));
    }
    match guard.as_ref().unwrap().first() {
        Some(addr) => Ok(addr.to_string()),
        None => Err(Error::from(String::from("PD addresses not set!"))),
    }
}

pub async fn do_async_get(url: &str) -> Result<RedisValue, Error> {
    let client = Client::new();
    let text = client.get(url).send().await?.text().await?;
    let ntext = text.replace("\"", "'");
    let lines = ntext.split("\n").collect::<Vec<&str>>();
    Ok(lines.into())
}

pub async fn do_async_delete(url: &str) -> Result<RedisValue, Error> {
    let client = Client::new();
    let text = client.delete(url).send().await?.text().await?;
    let ntext = text.replace("\"", "'");
    let lines = ntext.split("\n").collect::<Vec<&str>>();
    Ok(lines.into())
}

pub async fn do_async_post(url: &str, body: String) -> Result<RedisValue, Error> {
    let client = Client::new();
    let text = client.post(url).body(body).send().await?.text().await?;
    let ntext = text.replace("\"", "'");
    let lines = ntext.split("\n").collect::<Vec<&str>>();
    Ok(lines.into())
}

pub async fn do_async_get_members(url: &str) -> Result<RedisValue, Error> {
    let client = Client::new();
    let text = client.get(url).send().await?.text().await?;
    let output = format_json(text)?;
    Ok(output)
}
