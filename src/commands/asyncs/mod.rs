use crate::client::RawClientWrapper;
use crate::commands::errors::RTError;
use super::{TIKV_RAW_CLIENT, TIKV_RAW_CLIENT_2};

pub mod string;
pub mod connection;
pub mod hash;
pub mod list;
pub mod admin;
pub mod set;

static mut CLIENT_COUNTER: u64 = 0;

pub fn get_client() -> Result<RawClientWrapper, RTError> {
    if unsafe {TIKV_RAW_CLIENT.is_none() } {
        return Err(RTError::StringError(String::from("Not Connected")))
    }
    let idx: u64;
    let ret: RawClientWrapper;
    unsafe {
        CLIENT_COUNTER += 1;
        idx = CLIENT_COUNTER
    }
    if idx % 2 == 0 {
        let client = unsafe {TIKV_RAW_CLIENT.as_ref().unwrap() };
        ret = RawClientWrapper::new(client);
    } else {
        let client = unsafe {TIKV_RAW_CLIENT_2.as_ref().unwrap() };
        ret = RawClientWrapper::new(client);
    }
    Ok(ret)
}