use anyhow::anyhow;
use anyhow::Result;
use polars::prelude::*;
use polars::prelude::Expr::Column;
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Offset as SqlOffset, OrderByExpr, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
};

/// 解析出来的 SQL
pub struct Sql<'a> {
    pub(crate) selection: Vec<Expr>,
    pub(crate) condition: Option<Expr>,
    pub(crate) source: &'a str,
    pub(crate) order_by: Vec<(String, bool)>,
    pub(crate) offset: Option<i64>,
    pub(crate) limit: Option<usize>,
}

// 因为 Rust trait 的孤儿规则，我们如果要想对已有的类型实现已有的 trait，
// 需要简单包装一下

pub struct Expression(pub(crate) Box<SqlExpr>);
pub struct Operation(pub(crate) SqlBinaryOperator);
pub struct Projection<'a>(pub(crate) &'a SelectItem);
pub struct Source<'a>(pub(crate) &'a [TableWithJoins]);
pub struct Order<'a>(pub(crate) &'a OrderByExpr);
pub struct Offset<'a>(pub(crate) &'a SqlOffset);
pub struct Limit<'a>(pub(crate) &'a SqlExpr);
pub struct Value(pub(crate) SqlValue);

impl<'a> TryFrom<&'a Statement> for Sql<'a> {
    type Error = anyhow::Error;

    fn try_from(sql: &'a Statement) -> Result<Self, Self::Error> {
        match sql {
            Statement::Query(q) => {
                let Select {
                    from: table_with_joins,
                    selection: where_clause,
                    ..
                } = match &q.body {
                    SetExpr::Select(statement) => statement.as_ref(),
                    _ => return Err(anyhow!("We only surport Select Query at the moment")),
                };

                let source = Source(table_with_joins).try_into()?;

            }
            _ => Err(anyhow!("We only support Query at the moment")),
        }
    }
}

impl TryFrom<Expression> for Expr {
    type Error = anyhow::Error;

    fn try_from(value: Expression) -> Result<Self, Self::Error> {
        match *value.0 {
            SqlExpr::BinaryOp{left, op, right} => {
                Self::BinaryExpr {
                    left: Box::new(Expression(left).try_into()?),
                    op: Operation(op).try_into()?,
                    right: Box::new(Expression(right).try_into()?),
                }
            },
            SqlExpr::Wildcard => {
                Ok(Self::Wildcard)
            },
            SqlExpr::IsNull(expr) => {
                Ok(Self::IsNull(Expression(expr).try_into()?))
            },
            SqlExpr::IsNotNull(expr) => {
                Ok(Self::IsNotNull(Box::new(Expression(expr).try_into()?)))
            },
            SqlExpr::Identifier(id) => {
                Ok(Column(Arc::new(id.value)))
            },
            SqlExpr::Value(v) => Ok(Self::Literal(Value(v).try_into()?)),
            // ... continue with remaining SqlExpr patterns
            _ => Err(anyhow!("Unhandled expression case")),
        }
    }
}