use sqlx::{QueryBuilder, Postgres};
use serde_json::Value;
use chrono::NaiveDateTime;

pub enum BaseQuery<'a> {
    Sql(&'a str),
    QueryBuilder(QueryBuilder<'a, Postgres>)
}


pub enum ColumnType {
    OptPrimitive(Option<Value>),
    OptDateTime(Option<NaiveDateTime>)
}
