use tikv_client::Key;


pub struct KeyDecoder {}

impl KeyDecoder {
    pub fn new() -> Self {
        KeyDecoder{}
    }

    pub fn decode_string(&self, key: Key) -> Vec<u8> {
        let mut bytes: Vec<u8> = key.clone().into();
        bytes.drain(15..).collect()
    }

    pub fn decode_hash_field(&self, rkey: Key, key: &str) -> Vec<u8> {
        let mut bytes: Vec<u8> = rkey.clone().into();
        bytes.drain(17 + key.len() + 1..).collect()
    }
}