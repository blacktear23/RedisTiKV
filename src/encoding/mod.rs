pub mod encode;
pub mod decode;

pub const EMPTY_VALUE: Vec<u8> = vec![];

pub enum DataType {
    String,
    Hash,
    List,
    Set,
}

pub use {
    encode::KeyEncoder,
    decode::KeyDecoder,
};