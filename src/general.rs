use chrono::NaiveDateTime;
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};

pub enum BaseQuery<'a> {
    Sql(&'a str),
    QueryBuilder(QueryBuilder<'a, Postgres>),
}

pub enum ColumnType {
    OptPrimitive(Option<Value>),
    OptDateTime(Option<NaiveDateTime>),
}
