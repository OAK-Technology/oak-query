use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};

use crate::{BaseQuery, SqlValue, push_sqlvalue, push_jsonvalue};

#[derive(Debug, Clone)]
pub struct Condition<'a> {
    pub chain_opr: Option<&'a str>,
    pub column: &'a str,
    pub eq_opr: &'a str,
    pub value_l: SqlValue,
    pub value_r: Option<SqlValue>,
}

impl<'a> Condition<'a> {
    /// chain_opr: AND, OR, (may etc.)
    /// column: column that condition belongs for
    /// eq_opr: =, >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, BETWEEN, NOT BETWEEN etc.
    /// value_l and value_r: is used for BETWEN operator ex.: `WHERE sample_col BETWEEN value_l and value_r`
    /// value for other operators is value_l
    pub fn new(
        chain_opr: Option<&'a str>,
        column: &'a str,
        eq_opr: &'a str,
        value_l: SqlValue,
        value_r: Option<SqlValue>,
    ) -> Self {
        Self {
            chain_opr,
            column,
            eq_opr,
            value_l,
            value_r,
        }
    }
}

/// if only one condition provided, then chain operator ignored for that condition
pub struct ConditionBuilder<'a> {
    pub base_query: BaseQuery<'a>,
    pub conditions: &'a Vec<Condition<'a>>,
    pub middle: Option<&'a str>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub end: Option<&'a str>,
}

impl<'a> ConditionBuilder<'a> {
    pub fn new(
        base_query: BaseQuery<'a>,
        conditions: &'a Vec<Condition<'a>>,
        middle: Option<&'a str>,
        limit: Option<i64>,
        offset: Option<i64>,
        end: Option<&'a str>,
    ) -> Self {
        Self {
            base_query,
            conditions,
            middle,
            limit,
            offset,
            end,
        }
    }

    pub fn build(self) -> QueryBuilder<'a, Postgres> {
        let mut query: QueryBuilder<'_, Postgres>;

        match self.base_query {
            BaseQuery::Sql(base_sql) => query = QueryBuilder::new(base_sql),
            BaseQuery::QueryBuilder(query_builder) => query = query_builder,
        }

        for (index, cond) in self.conditions.iter().enumerate() {
            match cond.eq_opr.to_uppercase().as_str() {
                "BETWEEN" => {
                    if let Some(value_r) = &cond.value_r {
                        if index == 0 {
                            query.push("\nWHERE");
                            query.push(format!("\n    {0} {1} ", cond.column, cond.eq_opr));
                            
                            query = push_sqlvalue(cond.value_l.clone(), query);
                            query.push(" AND ");
                            query = push_sqlvalue(value_r.clone(), query);
                        } else if let Some(chain_opr) = cond.chain_opr {
                            query.push(format!(
                                "\n    {0} {1} {2} ",
                                chain_opr, cond.column, cond.eq_opr
                            ));

                            query = push_sqlvalue(cond.value_l.clone(), query);
                            query.push(" AND ");
                            query = push_sqlvalue(value_r.clone(), query);
                        }
                    }
                },

                "IN" => {
                    if index == 0 {
                        if let SqlValue::GenericValue(Value::Array(item_list)) = cond.value_l.clone() {
                            query.push("\nWHERE");
                            query.push(format!("\n    {0} {1} ", cond.column, cond.eq_opr));

                            query = Self::push_as_sql_tuple(item_list, query);
                        }
                    } else if let Some(chain_opr) = cond.chain_opr {
                        if let SqlValue::GenericValue(Value::Array(item_list)) = cond.value_l.clone() {
                            query.push(format!(
                                "\n    {0} {1} {2} ",
                                chain_opr, cond.column, cond.eq_opr
                            ));

                            query = Self::push_as_sql_tuple(item_list, query);
                        }
                    }
                },

                operator if operator.contains("LIKE") => {
                    if index == 0 {
                        query.push("\nWHERE");
                        query.push(format!("\n    {0} {1} ", cond.column, cond.eq_opr));
                        
                        let like_value: String;

                        if let SqlValue::GenericValue(Value::String(value)) = cond.value_l.clone() {
                            like_value = format!("%{value}%");
                        } else {
                            like_value = String::new();
                        }

                        query = push_sqlvalue(like_value.into(), query);
                    } else if let Some(chain_opr) = cond.chain_opr {
                        query.push(format!(
                            "\n    {0} {1} {2} ",
                            chain_opr, cond.column, cond.eq_opr
                        ));
                        query = push_sqlvalue(cond.value_l.clone(), query);
                    }
                },

                _ => {
                    if index == 0 {
                        query.push("\nWHERE");
                        query.push(format!("\n    {0} {1} ", cond.column, cond.eq_opr));
                        query = push_sqlvalue(cond.value_l.clone(), query);
                    } else if let Some(chain_opr) = cond.chain_opr {
                        query.push(format!(
                            "\n    {0} {1} {2} ",
                            chain_opr, cond.column, cond.eq_opr
                        ));
                        query = push_sqlvalue(cond.value_l.clone(), query);
                    } 
                }
            }
        }

        if let Some(middle_sql) = self.middle {
            query.push(format!("\n{}", middle_sql));
        }

        if let Some(limit) = self.limit {
            query.push("\nLIMIT ");
            query.push_bind(limit);
        }

        if let Some(offset) = self.offset {
            query.push("\nOFFSET ");
            query.push_bind(offset);
        }

        if let Some(ending) = self.end {
            query.push(format!("\n{}", ending));
        }

        query
    }

    fn push_as_sql_tuple(item_list: Vec<Value>, mut query: QueryBuilder<'a, Postgres>) -> QueryBuilder<'a, Postgres> {
        query.push("(");
        
        for (index, item) in item_list.iter().enumerate() {
            query = push_jsonvalue(item.clone(), query);

            if index < item_list.len() - 1 {
                query.push(", ");
            }
        }

        query.push(")");

        query
    }
}

#[cfg(test)]
mod tests {
    use crate::condition::{Condition, ConditionBuilder};
    use crate::general::BaseQuery;

    #[test]
    fn between_with_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(
            None,
            "test_col",
            "BETWEEN",
            5.into(),
            Some(24.into()),
        ));
        let test_query =
            ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);

        let result = "\nWHERE\n    test_col BETWEEN $1 AND $2";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn single_condition_with_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(
            None,
            "test_col",
            "LIKE",
            "sample".into(),
            None,
        ));
        let test_query =
            ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);

        let result = "\nWHERE\n    test_col LIKE $1";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn single_condition_with_in_operator() {
        let mut conditions: Vec<Condition> = Vec::new();
        
        let list: Vec<&str> = vec!["ab", "cd", "ef"];

        conditions.push(Condition::new(
            None,
            "test_col",
            "IN",
            list.into(),
            None,
        ));
        let test_query =
            ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);

        let result = "\nWHERE\n    test_col IN ($1, $2, $3)";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn single_condition_without_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(
            Some(""),
            "test_col",
            "LIKE",
            "sample".into(),
            None,
        ));
        let test_query =
            ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);

        let result = "\nWHERE\n    test_col LIKE $1";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn single_condition_with_chain_operand() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(
            Some("AND"),
            "test_col",
            "LIKE",
            "sample".into(),
            None,
        ));
        let test_query =
            ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);

        let result = "\nWHERE\n    test_col LIKE $1";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn empty_condition_builder() {
        let conditions: Vec<Condition> = Vec::new();
        let test_query =
            ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);

        assert_eq!(test_query.build().into_sql(), "");
    }

    #[test]
    fn multiple_conditions() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(
            Some("AND"),
            "test_col",
            "LIKE",
            "sample".into(),
            None,
        ));
        conditions.push(Condition::new(
            Some("OR"),
            "test_col2",
            "=",
            5.into(),
            None,
        ));

        // This condition will be ignored because there is no chain operator
        conditions.push(Condition::new(
            None,
            "other_col",
            "=",
            7.into(),
            None
        ));

        let order_by = "ORDER BY\n    id DESC";

        let test_query = ConditionBuilder::new(
            BaseQuery::Sql(""),
            &conditions,
            Some(order_by),
            Some(10),
            Some(0),
            None,
        );

        let result = r#"
WHERE
    test_col LIKE $1
    OR test_col2 = $2
ORDER BY
    id DESC
LIMIT $3
OFFSET $4"#;

        assert_eq!(test_query.build().into_sql(), result);
    }
}
