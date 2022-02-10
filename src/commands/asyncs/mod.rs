use crate::client::RawClientWrapper;
use crate::commands::errors::RTError;
use super::TIKV_RAW_CLIENT;

pub mod string;
pub mod connection;
pub mod hash;
pub mod list;
pub mod admin;

pub fn get_client() -> Result<RawClientWrapper, RTError> {
    if unsafe {TIKV_RAW_CLIENT.is_none() } {
        return Err(RTError::StringError(String::from("Not Connected")))
    }
    let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap() };
    let ret = RawClientWrapper::new(client);
    Ok(ret)
}