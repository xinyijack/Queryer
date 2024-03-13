use anyhow::anyhow;
use anyhow::Result;
use polars::prelude::*;
use polars::prelude::Expr::Column;
use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Offset as SqlOffset, OrderByExpr, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue};

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
                let offset = q.offset.as_ref();
                let limit = q.limit.as_ref();
                let orders = &q.order_by;
                let Select {
                    from: table_with_joins,
                    selection: where_clause,
                    projection,
                    group_by: _,
                    ..
                } = match &q.body {
                    SetExpr::Select(statement) => statement.as_ref(),
                    _ => return Err(anyhow!("We only support Select Query at the moment")),
                };

                let source = Source(table_with_joins).try_into()?;

                let condition = match where_clause {
                    None => None,
                    Some(expr) => Some(Expression(Box::new(expr.to_owned())).try_into()?),
                };

                let mut selection = Vec::with_capacity(8);
                for p in projection {
                    let expr = Projection(p).try_into()?;
                    selection.push(expr);
                }

                let mut order_by = Vec::new();
                for expr in orders {
                    order_by.push(Order(expr).try_into()?);
                }
                let offset = offset.map(|v| Offset(v).into());
                let limit = limit.map(|v| Limit(v).into());

                Ok(Sql{
                    selection,
                    condition,
                    source,
                    order_by,
                    offset,
                    limit,
                })
            }
            _ => Err(anyhow!("We only support Query at the moment")),
        }
    }
}

impl TryFrom<Expression> for Expr {
    type Error = anyhow::Error;

    fn try_from(value: Expression) -> Result<Self, Self::Error> {
        match *value.0 {
            SqlExpr::BinaryOp{left, op, right} => Ok({
                Self::BinaryExpr {
                    left: Box::new(Expression(left).try_into()?),
                    op: Operation(op).try_into()?,
                    right: Box::new(Expression(right).try_into()?),
                }
            }),
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
            v => Err(anyhow!("expr {:#?} is not supported", v)),
        }
    }
}

impl TryFrom<Operation> for Operator {
    type Error = anyhow::Error;

    fn try_from(op: Operation) -> std::result::Result<Self, Self::Error> {
        match op.0 {
            SqlBinaryOperator::Plus => Ok(Self::Plus),
            SqlBinaryOperator::Minus => Ok(Self::Minus),
            SqlBinaryOperator::Multiply => Ok(Self::Multiply),
            SqlBinaryOperator::Divide => Ok(Self::Divide),
            SqlBinaryOperator::Modulo => Ok(Self::Modulo),
            SqlBinaryOperator::Gt => Ok(Self::Gt),
            SqlBinaryOperator::Lt => Ok(Self::Lt),
            SqlBinaryOperator::GtEq => Ok(Self::GtEq),
            SqlBinaryOperator::LtEq => Ok(Self::LtEq),
            SqlBinaryOperator::Eq => Ok(Self::Eq),
            SqlBinaryOperator::NotEq => Ok(Self::NotEq),
            SqlBinaryOperator::And => Ok(Self::And),
            SqlBinaryOperator::Or => Ok(Self::Or),
            v => Err(anyhow!("Operator {:#?} is not supported", v)),
        }
    }
}

impl<'a> TryFrom<Projection<'a>> for Expr {
    type Error = anyhow::Error;

    fn try_from(p: Projection<'a>) -> std::result::Result<Self, Self::Error> {
        match p.0 {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(id)) => Ok(col(&id.to_string())),
            SelectItem::ExprWithAlias { expr: SqlExpr::Identifier(id), alias } =>
                Ok(Expr::Alias(Box::new(Column(Arc::new(id.to_string()))), Arc::new(alias.to_string()))),
            SelectItem::QualifiedWildcard(v) =>
                Ok(col(&v.to_string())),
            SelectItem::Wildcard =>
                Ok(col("*")),
            item => Err(anyhow!("Projection {} not supported", item)),
        }
    }
}

impl<'a> TryFrom<Source<'a>> for &'a str {
    type Error = anyhow::Error;

    fn try_from(s: Source<'a>) -> Result<Self, Self::Error> {
         if s.0.len() != 1 {
            return Err(anyhow!("We only support single data source at the moment"));
         }

        let table = &s.0[0];
        if !table.joins.is_empty() {
            return Err(anyhow!("We do not support joint data source at the moment"));
        }

        match &table.relation {
            TableFactor::Table { name, .. } => {
                Ok(&name.0.first().unwrap().value)
            },
            _ => Err(anyhow!("we only support table")),
        }
    }
}

impl<'a> TryFrom<Order<'a>> for (String, bool) {
    type Error = anyhow::Error;

    fn try_from(o: Order<'a>) -> Result<Self, Self::Error> {
        let name = match &o.0.expr {
            SqlExpr::Identifier(id) => {
                id.to_string()
            },
            expr => {
              return Err(anyhow!(
                  "We only support identifier for order by, got {}",
                  expr
              ))
            }
        };

        Ok((name, !o.0.asc.unwrap_or(true)))
    }
}

/// 转换offset expr 为 i64
impl<'a> From<Offset<'a>> for i64 {
    fn from(offset: Offset<'a>) -> Self {
        match offset.0 {
            SqlOffset {
                value: SqlExpr::Value(SqlValue::Number(v, _b)),
                ..
            } => v.parse().unwrap_or(0),
            _ => 0,
        }
    }
}

/// 把limit转换为usize
impl<'a> From<Limit<'a>> for usize {
    fn from(l: Limit<'a>) -> Self {
        match l.0 {
            SqlExpr::Value(SqlValue::Number(v, _b)) => v.parse.unwarap_or(usize::MAX),
            _ => usize::MAX,
        }
    }
}

/// 把sqlParse 的 value转换为 DataFrame支持的 LiteralValue
impl TryFrom<Value> for LiteralValue {
    type Error = anyhow::Error;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v.0 {
            Value::Number(v, _) => Ok(LiteralValue::Float64(v.parse.unwrap())),
            Value::Boolean(v) => Ok(LiteralValue::Boolean(v)),
            Value::Null => Ok(LiteralValue::Null),
            v => Err(anyhow!("Value {} is not supported", v)),
        }
    }
}