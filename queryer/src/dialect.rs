use sqlparser::dialect::Dialect;

#[derive(Debug, Default)]
pub struct TryDialect;

impl Dialect for TryDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch) || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ('a'..='z').contains(&ch) || ('A'..='Z').contains(&ch)
        || ('0'..='9').contains(&ch)
        || [':', '/', '?', '&', '=', '_', '-', '.'].contains(&ch)
    }
}

/// 辅助测试函数
pub fn example_sql() -> String {
    let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";

    let sql = format!(
        "SELECT location name, total_cases, new_cases, total_deaths, new_deaths \
        FROM {} where new_deaths >= 500 ORDER BY new_cases DESC LIMIT 6 OFFSET 5",
        url
    );

    sql
}

#[cfg(test)]
mod tests {
    use sqlparser::parser::Parser;
    use crate::dialect::{example_sql, TryDialect};

    #[test]
    fn it_works() {
        assert!(Parser::parse_sql(&TryDialect::default(), &example_sql()).is_ok());
    }
}