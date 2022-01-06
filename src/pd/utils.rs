use reqwest::{ Client, Error };
use redis_module::RedisValue;

pub async fn do_async_curl(url: &str) -> Result<RedisValue, Error> {
    let client = Client::new();
    let text = client.get(url).send().await?.text().await?;
    let ntext = text.replace("\"", "'");
    let lines = ntext.split("\n").collect::<Vec<&str>>();
    Ok(lines.into())
}

pub fn generate_pd_url(pd_addr: &str, func: &str) -> String {
    format!("http://{}/pd/api/v1/{}", pd_addr, func)
}