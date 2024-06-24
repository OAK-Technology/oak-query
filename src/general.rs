use sqlx::{Postgres, QueryBuilder};
use serde::{Serialize, Deserialize};
use serde_json::{Value, Number};
use chrono::{NaiveDate, NaiveDateTime};
use std::convert::From;

pub enum BaseQuery<'a> {
    Sql(&'a str),
    QueryBuilder(QueryBuilder<'a, Postgres>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NaiveChrono {
    NaiveDate(NaiveDate),
    NaiveDateTime(NaiveDateTime)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlValue {
    GenericValue(Value),
    NaiveChrono(NaiveChrono)
}

impl From<&Value> for SqlValue {
    fn from(value: &Value) -> Self {
        Self::GenericValue(value.clone())
    }
}

impl From<Value> for SqlValue {
    fn from(value: Value) -> Self {
        Self::GenericValue(value)
    }
}

impl From<Vec<Value>> for SqlValue {
    fn from(value: Vec<Value>) -> Self {
        Self::GenericValue(value.into())
    }
}

impl From<&str> for SqlValue {
    fn from(value: &str) -> Self {
        Self::GenericValue(Value::String(value.to_string()))
    }
}

impl From<&String> for SqlValue {
    fn from(value: &String) -> Self {
        Self::GenericValue(Value::String(value.to_string()))
    }
}

impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        Self::GenericValue(Value::String(value))
    }
}

impl From<Vec<&str>> for SqlValue {
    fn from(value: Vec<&str>) -> Self {
        Self::GenericValue(value.into())
    }
}

impl From<Vec<String>> for SqlValue {
    fn from(value: Vec<String>) -> Self {
        Self::GenericValue(value.into())
    }
}

impl From<Vec<&String>> for SqlValue {
    fn from(value: Vec<&String>) -> Self {
        Self::GenericValue(Value::from(value.iter().cloned().map(Into::into).collect::<Vec<String>>()))
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

impl From<u64> for SqlValue {
    fn from(value: u64) -> Self {
        Self::GenericValue(Value::Number(Number::from(value as i64)))
    }
}

impl From<Vec<i8>> for SqlValue {
    fn from(value: Vec<i8>) -> Self {
        Self::GenericValue(value.into())
    }
}

impl From<Vec<i16>> for SqlValue {
    fn from(value: Vec<i16>) -> Self {
        Self::GenericValue(value.into())
    }
}

impl From<Vec<i32>> for SqlValue {
    fn from(value: Vec<i32>) -> Self {
        Self::GenericValue(value.into())
    }
}

impl From<Vec<i64>> for SqlValue {
    fn from(value: Vec<i64>) -> Self {
        Self::GenericValue(value.into())
    }
}

impl From<Vec<isize>> for SqlValue {
    fn from(value: Vec<isize>) -> Self {
        Self::GenericValue(value.into())
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

pub fn push_sqlvalue(value: SqlValue, mut query_builder: QueryBuilder<'_, Postgres>) -> QueryBuilder<'_, Postgres> {
    match value {
        SqlValue::GenericValue(v) => push_jsonvalue(v, query_builder),
        SqlValue::NaiveChrono(naive_chrono) => {
            match naive_chrono {
                NaiveChrono::NaiveDate(nd) => { query_builder.push_bind(nd); },
                NaiveChrono::NaiveDateTime(ndt) => { query_builder.push_bind(ndt); },
            }
            
            return query_builder
        },
    }
}
