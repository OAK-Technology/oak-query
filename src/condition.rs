use serde_json::Value;
use sqlx::{QueryBuilder, Postgres};

use crate::BaseQuery;


#[derive(Debug, Clone)]
pub struct Condition<'a> {
    pub chain_opr: Option<&'a str>,
    pub column: &'a str,
    pub eq_opr: &'a str,
    pub value_l: Option<Value>,
    pub value_r: Option<Value>
}

impl <'a > Condition<'a> {
    /// chain_opr: AND, OR, (may etc)
    /// column: column that condition belongs for
    /// eq_opr: >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, BETWEEN, NOT BETWEEN and etc
    /// value_l and value_r: is used for BETWEN operator ex.: `WHERE sample_col BETWEEN value_l and value_r`
    /// value for other operators is value_l
    pub fn new(chain_opr: Option<&'a str>, column: &'a str, eq_opr: &'a str, value_l: Option<Value>, value_r: Option<Value>) -> Self {
        Self {
            chain_opr,
            column,
            eq_opr,
            value_l,
            value_r
        }
    }
}

pub struct ConditionBuilder<'a> {
    pub base_query: BaseQuery<'a>,
    pub conditions: &'a Vec<Condition<'a>>,
    pub middle: Option<&'a str>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub end: Option<&'a str>
}

impl <'a> ConditionBuilder <'a> {
    pub fn new(
        base_query: BaseQuery<'a>,
        conditions: &'a Vec<Condition<'a>>,
        middle: Option<&'a str>,
        limit: Option<i64>,
        offset: Option<i64>,
        end: Option<&'a str>
    ) -> Self {
        Self {
            base_query,
            conditions,
            middle,
            limit,
            offset,
            end
        }
    }

    pub fn build(self,) -> QueryBuilder<'a, Postgres> {
        let mut query: QueryBuilder<'_, Postgres>;
    
        match self.base_query {
            BaseQuery::Sql(base_sql) => {query = QueryBuilder::new(base_sql)},
            BaseQuery::QueryBuilder(query_builder) => {query = query_builder},
        }
        
        for (index, cond) in self.conditions.iter().enumerate() {
            if cond.eq_opr.to_uppercase().contains("BETWEEN") {
                if let (Some(value_l), Some(value_r)) = (&cond.value_l, &cond.value_r) {
                    if let Some(chain_opr) = cond.chain_opr {
                        query.push(format!("\n    {0} {1} {2} ", chain_opr, cond.column, cond.eq_opr));
                        query.push_bind(value_l);
                        query.push(" AND ");
                        query.push_bind(value_r);
                    } else {
                        if index == 0 {
                            query.push(format!("\nWHERE"));
                            query.push(format!("\n    {0} {1} ", cond.column, cond.eq_opr));
                            query.push_bind(value_l);
                            query.push(" AND ");
                            query.push_bind(value_r);
                        }
                    }
                }
            } else {
                if let Some(value) = &cond.value_l {
                    if let Some(chain_opr) = cond.chain_opr {
                        query.push(format!("\n    {0} {1} {2} ", chain_opr, cond.column, cond.eq_opr));
                        query.push_bind(value);
                    } else {
                        if index == 0 {
                            query.push(format!("\nWHERE"));
                            query.push(format!("\n    {0} {1} ", cond.column, cond.eq_opr));
                            query.push_bind(value);
                        }
                    }
                }
            }
        }
    
        if let Some(middle_sql) = self.middle {
            query.push(middle_sql);
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
}

#[cfg(test)]
mod tests {
    use crate::condition::{Condition, ConditionBuilder};
    use crate::general::BaseQuery;

    #[test]
    fn in_without_where() {
        let mut conditions: Vec<Condition> = Vec::new();
        let num_list = vec![3, 5, 7];

        conditions.push(Condition::new(Some(""), "test_col", "IN", Some(num_list.into()), None));
        let test_query = ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);

        let result = "\n     test_col IN $1";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn between_with_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(None, "test_col", "BETWEEN", Some(5.into()), Some(24.into())));
        let test_query = ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);
        
        let result = "\nWHERE\n    test_col BETWEEN $1 AND $2";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn single_condition_with_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(None, "test_col", "LIKE", Some("sample".into()), None));
        let test_query = ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);
        
        let result = "\nWHERE\n    test_col LIKE $1";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn single_condition_without_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(Some(""), "test_col", "LIKE", Some("sample".into()), None));
        let test_query = ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);
        
        let result = "\n     test_col LIKE $1";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn single_condition_with_chain_operand() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(Some("AND"), "test_col", "LIKE", Some("sample".into()), None));
        let test_query = ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);
        
        let result = "\n    AND test_col LIKE $1";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn empty_condition_builder() {
        let conditions: Vec<Condition> = Vec::new();
        let test_query = ConditionBuilder::new(BaseQuery::Sql(""), &conditions, None, None, None, None);
        
        assert_eq!(test_query.build().into_sql(), "");
    }

    #[test]
    fn multiple_conditions() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(Some("AND"), "test_col", "LIKE", Some("sample".into()), None));
        conditions.push(Condition::new(Some("OR"), "test_col2", "=", Some(5.into()), None));

        // This condition will be ignored because there is no chain operator
        conditions.push(Condition::new(None, "other_col", "=", Some(7.into()), None));

        let order_by = "\nORDER BY\n    id DESC";

        let test_query = ConditionBuilder::new(BaseQuery::Sql(""), &conditions, Some(order_by), Some(10), Some(0), None);
        
        let result = r#"
    AND test_col LIKE $1
    OR test_col2 = $2
ORDER BY
    id DESC
LIMIT $3
OFFSET $4"#;

        assert_eq!(test_query.build().into_sql(), result);
    }
}
