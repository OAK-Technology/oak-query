use sqlx::{Postgres, QueryBuilder};
use serde::{Serialize, Deserialize};
use serde_json::{Value, Number};
use chrono::{NaiveDate, NaiveDateTime};
use std::convert::From;

pub enum BaseQuery<'a> {
    Sql(&'a str),
    QueryBuilder(QueryBuilder<'a, Postgres>),
}

#[derive(Serialize, Deserialize)]
pub enum NaiveChrono {
    NaiveDate(NaiveDate),
    NaiveDateTime(NaiveDateTime)
}

#[derive(Serialize, Deserialize)]
pub enum SqlValue {
    GenericValue(Value),
    NaiveChrono(NaiveChrono)
}

impl From<Value> for SqlValue {
    fn from(value: Value) -> Self {
        Self::GenericValue(value)
    }
}

impl From<&str> for SqlValue {
    fn from(value: &str) -> Self {
        Self::GenericValue(Value::String(value.to_string()))
    }
}

impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        Self::GenericValue(Value::String(value))
    }
}

impl From<bool> for SqlValue {
    fn from(value: bool) -> Self {
        Self::GenericValue(Value::Bool(value))
    }
}

impl From<i8> for SqlValue {
    fn from(value: i8) -> Self {
        Self::GenericValue(Value::Number(Number::from(value)))
    }
}

impl From<i16> for SqlValue {
    fn from(value: i16) -> Self {
        Self::GenericValue(Value::Number(Number::from(value)))
    }
}

impl From<i32> for SqlValue {
    fn from(value: i32) -> Self {
        Self::GenericValue(Value::Number(Number::from(value)))
    }
}

impl From<i64> for SqlValue {
    fn from(value: i64) -> Self {
        Self::GenericValue(Value::Number(Number::from(value)))
    }
}

impl From<isize> for SqlValue {
    fn from(value: isize) -> Self {
        Self::GenericValue(Value::Number(Number::from(value)))
    }
}

impl From<NaiveDate> for SqlValue {
    fn from(value: NaiveDate) -> Self {
        Self::NaiveChrono(NaiveChrono::NaiveDate(value))
    }
}

impl From<NaiveDateTime> for SqlValue {
    fn from(value: NaiveDateTime) -> Self {
        Self::NaiveChrono(NaiveChrono::NaiveDateTime(value))
    }
}


pub fn push_jsonvalue(value: Value, mut query_builder: QueryBuilder<'_, Postgres>) -> QueryBuilder<'_, Postgres> {
    match value {
        Value::Null => {},
        Value::Bool(v) => { query_builder.push_bind(v); },
        Value::Number(v) => { 
            if v.is_f64() {
                query_builder.push_bind(v.as_f64());
            }

            if v.is_i64() || v.is_u64() {
                query_builder.push_bind(v.as_i64());
            }
         },
        Value::String(v) => { query_builder.push_bind(v); },
        Value::Array(v) => { query_builder.push_bind(v); },
        Value::Object(_) => { query_builder.push_bind(value); },
    }

    query_builder
}
