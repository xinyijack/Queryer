
use async_trait::async_trait;
use anyhow::{anyhow, Result};
use tokio::fs;

// rust 的 async trait 还没有稳定，可以用async_trait 宏
#[async_trait]
pub trait Fetch {
    type Error;
    async fn fetch(&self) -> Result<String, Self::Error>;
}

/// 从文件源或者 http 源中获取数据，组成data_frame
pub async fn retrieve_data(source: impl AsRef<str>) -> Result<String> {
    let name = source.as_ref();
    match &name[..4] {
        // 包括 http / https
        "http" => {UrlFetcher(name).fetch().await},
        "file" => {FileFetcher(name).fetch().await},
        _ => return Err(anyhow!("We only support http/https/file at the moment"))
    }
}

struct UrlFetcher<'a>(pub(crate) &'a str);

#[async_trait]
impl<'a> Fetch for UrlFetcher<'a> {
    type Error = anyhow::Error;

    async fn fetch(&self) -> Result<String, Self::Error> {
        Ok(reqwest::get(self.0).await?.text().await?)
    }
}

struct FileFetcher<'a>(pub(crate) &'a str);
#[async_trait]
impl<'a> Fetch for FileFetcher<'a> {
    type Error = anyhow::Error;

    async fn fetch(&self) -> Result<String, Self::Error> {
        let path = &self.0[7..];
        Ok(fs::read_to_string(path).await?)
    }
}

