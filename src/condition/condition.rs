use serde_json::Value;
use sqlx::{QueryBuilder, Postgres};


#[derive(Debug)]
pub struct Condition<'a> {
    pub chain_operator: Option<&'a str>,
    pub column: &'a str,
    pub equality: &'a str,
    pub value_l: Option<Value>,
    pub value_r: Option<Value>
}

impl <'a > Condition<'a> {
    /// chain_operator: AND, OR, (may etc)
    /// column: column that condition belongs for
    /// equality: >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, BETWEEN, NOT BETWEEN and etc
    /// value_l and value_r: is used for BETWEN operator ex.: `WHERE sample_col BETWEEN value_l and value_r`
    /// value for other operators is value_l
    pub fn new(chain_operator: Option<&'a str>, column: &'a str, equality: &'a str, value_l: Option<Value>, value_r: Option<Value>) -> Self {
        Self {
            chain_operator,
            column,
            equality,
            value_l,
            value_r
        }
    }
}

pub fn condition_builder<'a>(
    base_query: &'a str,
    conditions: &'a Vec<Condition<'a>>,
    middle: &'a str,
    limit: Option<i64>,
    offset: Option<i64>,
    end: &'a str
) -> QueryBuilder<'a, Postgres> {
    let mut query: QueryBuilder<'_, Postgres> = QueryBuilder::new(base_query);
    
    for (index, cond) in conditions.iter().enumerate() {
        if cond.equality.to_uppercase().contains("BETWEEN") {
            if let (Some(value_l), Some(value_r)) = (&cond.value_l, &cond.value_r) {
                if let Some(chain_operator) = cond.chain_operator {
                    query.push(format!("\n    {0} {1} {2} ", chain_operator, cond.column, cond.equality));
                    query.push_bind(value_l);
                    query.push(" AND ");
                    query.push_bind(value_r);
                } else {
                    if index == 0 {
                        query.push(format!("\nWHERE"));
                        query.push(format!("\n    {0} {1} ", cond.column, cond.equality));
                        query.push_bind(value_l);
                        query.push(" AND ");
                        query.push_bind(value_r);
                    }
                }
            }
        } else {
            if let Some(value) = &cond.value_l {
                if let Some(chain_operator) = cond.chain_operator {
                    query.push(format!("\n    {0} {1} {2} ", chain_operator, cond.column, cond.equality));
                    query.push_bind(value);
                } else {
                    if index == 0 {
                        query.push(format!("\nWHERE"));
                        query.push(format!("\n    {0} {1} ", cond.column, cond.equality));
                        query.push_bind(value);
                    }
                }
            }
        }
    }

    query.push(middle);

    if let Some(limit) = limit {
        query.push("\nLIMIT ");
        query.push_bind(limit);
    }

    if let Some(offset) = offset {
        query.push("\nOFFSET ");
        query.push_bind(offset);
    }

    query.push(end);

    query
}

#[cfg(test)]
mod tests {
    use crate::condition::condition::{Condition, condition_builder};

    #[test]
    fn in_without_where() {
        let mut conditions: Vec<Condition> = Vec::new();
        let num_list = vec![3, 5, 7];

        conditions.push(Condition::new(Some(""), "test_col", "IN", Some(num_list.into()), None));
        let test_query = condition_builder("", &conditions, "", None, None, "");

        let result = "\n     test_col IN $1";

        assert_eq!(test_query.into_sql(), result);
    }

    #[test]
    fn between_with_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(None, "test_col", "BETWEEN", Some(5.into()), Some(24.into())));
        let test_query = condition_builder("", &conditions, "", None, None, "");
        
        let result = "\nWHERE\n    test_col BETWEEN $1 AND $2";

        assert_eq!(test_query.into_sql(), result);
    }

    #[test]
    fn single_condition_with_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(None, "test_col", "LIKE", Some("sample".into()), None));
        let test_query = condition_builder("", &conditions, "", None, None, "");
        
        let result = "\nWHERE\n    test_col LIKE $1";

        assert_eq!(test_query.into_sql(), result);
    }

    #[test]
    fn single_condition_without_where() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(Some(""), "test_col", "LIKE", Some("sample".into()), None));
        let test_query = condition_builder("", &conditions, "", None, None, "");
        
        let result = "\n     test_col LIKE $1";

        assert_eq!(test_query.into_sql(), result);
    }

    #[test]
    fn single_condition_with_chain_operand() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(Some("AND"), "test_col", "LIKE", Some("sample".into()), None));
        let test_query = condition_builder("", &conditions, "", None, None, "");
        
        let result = "\n    AND test_col LIKE $1";

        assert_eq!(test_query.into_sql(), result);
    }

    #[test]
    fn empty_condition_builder() {
        let conditions: Vec<Condition> = Vec::new();
        let test_query = condition_builder("", &conditions, "", None, None, "");
        
        assert_eq!(test_query.into_sql(), "");
    }

    #[test]
    fn multiple_conditions() {
        let mut conditions: Vec<Condition> = Vec::new();

        conditions.push(Condition::new(Some("AND"), "test_col", "LIKE", Some("sample".into()), None));
        conditions.push(Condition::new(Some("OR"), "test_col2", "=", Some(5.into()), None));

        // This condition will be ignored because there is no chain operator
        conditions.push(Condition::new(None, "other_col", "=", Some(7.into()), None));

        let order_by = "\nORDER BY\n    id DESC";

        let test_query = condition_builder("", &conditions, order_by, Some(10), Some(0), "");
        
        let result = r#"
    AND test_col LIKE $1
    OR test_col2 = $2
ORDER BY
    id DESC
LIMIT $3
OFFSET $4"#;

        assert_eq!(test_query.into_sql(), result);
    }
}
