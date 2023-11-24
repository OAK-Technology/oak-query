use sqlx::{Postgres, QueryBuilder};

pub enum BaseQuery<'a> {
    Sql(&'a str),
    QueryBuilder(QueryBuilder<'a, Postgres>),
}
