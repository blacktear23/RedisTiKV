pub enum DataType {
    Raw,
    Hash,
}

fn get_prefix(tp: DataType) -> String {
    let dt_prefix = match tp {
        DataType::Raw => "R",
        DataType::Hash => "H",
    };
    format!("$R_{}", dt_prefix)
}

pub fn encode_key(tp: DataType, key: &str) -> String {
    let prefix = get_prefix(tp);
    format!("{}_{}", prefix, key)
}

pub fn encode_keys(tp: DataType, keys: Vec<String>) -> Vec<String> {
    let prefix = get_prefix(tp);
    keys.into_iter().map(|val| format!("{}_{}", prefix, val)).collect()
}

pub fn encode_endkey(tp: DataType) -> String {
    let prefix = get_prefix(tp);
    format!("{}`", prefix)
}

pub fn decode_key(key: Vec<u8>) -> Vec<u8> {
    key.clone().drain(5..).collect()
}

pub fn encode_hash_key(key: &str, field: &str) -> String {
    let prefix = get_prefix(DataType::Hash);
    format!("{}_D_{}_{}", prefix, key, field)
}

pub fn encode_hash_prefix(key: &str) -> String {
    let prefix = get_prefix(DataType::Hash);
    format!("{}_D_{}_", prefix, key)
}

pub fn encode_hash_prefix_end(key: &str) -> String {
    let prefix = get_prefix(DataType::Hash);
    format!("{}_D_{}`", prefix, key)
}

pub fn decode_hash_field(rkey: Vec<u8>, key: &str) -> Vec<u8> {
    rkey.clone().drain(7+key.len()+1..).collect()
}
