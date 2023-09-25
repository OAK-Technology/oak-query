use sqlx::{QueryBuilder, Postgres};

pub enum BaseQuery<'a> {
    Sql(&'a str),
    QueryBuilder(QueryBuilder<'a, Postgres>)
}
