use std::ops::{Deref, DerefMut};
use polars::frame::DataFrame;
use anyhow::{anyhow, Result};
use polars::io::SerWriter;
use polars::prelude::{CsvWriter, IntoLazy};
use sqlparser::parser::Parser;
use tracing::info;
use crate::convert::Sql;
use crate::dialect::TryDialect;
use crate::fetcher::retrieve_data;
use crate::load::detect_contend;

mod dialect;
mod convert;
mod fetcher;
mod load;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[derive(Debug)]
pub struct DataSet(DataFrame);

impl Deref for DataSet {
    type Target = DataFrame;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DataSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// DataSet 内部方法
impl DataSet {
    /// DataSet 转换为 CSV
    pub fn to_csv(&self) -> Result<String> {
        let mut buf = Vec::new();
        let writer = CsvWriter::new(&mut buf);
        writer.finish(self)?;
        Ok(String::from_utf8(buf)?)
    }
}

/// 从 from 中获取数据，从 where 中过滤，最后选取需要返回的列
pub async fn query<T: AsRef<str>>(sql: T) -> Result<DataSet>{
    let ast = Parser::parse_sql(&TryDialect::default(), sql.as_ref())?;

    if ast.len() != 1 {
        return Err(anyhow!("Only support single sql at the moment"));
    }

    let sql = &ast[0];

    let Sql{
        selection, condition, source, order_by, offset, limit,
    } = sql.try_into()?;

    info!("retrieving data from source: {}", source);

    let ds = detect_contend(retrieve_data(source).await?).load()?;

    let mut filtered = match condition {
        None => {ds.0.lazy()}
        Some(expr) => {ds.0.lazy().filter(expr)}
    };

    filtered = order_by
        .into_iter()
        .fold(filtered, |acc, (col, desc)| acc.sort(&col, desc));

    if offset.is_some() || limit.is_some() {
        filtered = filtered.slice(offset.unwrap_or(0), limit.unwrap_or(usize::MAX));
    }

    Ok(DataSet(filtered.select(selection).collect()?))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
