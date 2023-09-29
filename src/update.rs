use serde_json::Value;
use sqlx::{QueryBuilder, Postgres};

use crate::{Condition, ConditionBuilder, BaseQuery};


#[derive(Debug)]
pub struct UpdateBuilder<'a> {
    pub table: &'a str,
    pub columns: Vec<(&'a str, Value)>,
    pub conditions: Vec<Condition<'a>>,
    pub end: Option<&'a str>
}

impl <'a > UpdateBuilder<'a> {
    /// table: table name
    /// columns: will be updated
    /// conditions: for restricting modified rows
    /// end: additional query part goes to end of update query ex.: `RETURNING id`
    pub fn new(table: &'a str, columns: Vec<(&'a str, Value)>, conditions: Vec<Condition<'a>>, end: Option<&'a str>) -> Self {
        Self {
            table,
            columns,
            conditions,
            end
        }
    }

    pub fn build(&self) -> QueryBuilder<'_, Postgres> {
        let mut query: QueryBuilder<'_, Postgres> = QueryBuilder::new("");

        if self.columns.len() > 0 {
            let base_query = format!("UPDATE {}", self.table);
            query.push(base_query);
            
            for (index, column) in self.columns.iter().enumerate() {
                if index == 0 {
                    query.push(format!("\n    SET {0} = ", column.0));
                    query.push_bind(&column.1);
                    
                    if index < self.columns.len() - 1 {
                        query.push(",");
                    }
                } else {
                    query.push(format!("\n    {0} = ", column.0));
                    query.push_bind(&column.1);
                    
                    if index < self.columns.len() - 1 {
                        query.push(",");
                    }
                }
            }
        }

        query
    }

    pub fn build_all(&mut self) -> QueryBuilder<'_, Postgres>{
        let query: QueryBuilder<'_, Postgres> = self.build();

        let query_new = ConditionBuilder::new(
            BaseQuery::QueryBuilder(query), 
            &self.conditions, 
            None, 
            None, 
            None,
            self.end
        ).build();

        query_new
    }
}



#[cfg(test)]
mod tests {
    use crate::{UpdateBuilder, Condition};
    use serde_json::Value;

    #[test]
    fn update_only() {
        let conditions: Vec<Condition> = Vec::new();
        let columns: Vec<(&str, Value)> = vec![("col1", 5.into()), ("col2", 3.into()), ("col3", 7.into())];
        let test_query = UpdateBuilder::new("sample_table", columns, conditions, None);
        let result = "UPDATE sample_table\n    SET col1 = $1,\n    col2 = $2,\n    col3 = $3";

        assert_eq!(test_query.build().into_sql(), result);
    }

    #[test]
    fn update_with_empty_conditions() {
        let conditions: Vec<Condition> = Vec::new();
        let columns: Vec<(&str, Value)> = vec![("col1", 5.into()), ("col2", 3.into()), ("col3", 7.into())];
        let mut test_query = UpdateBuilder::new("sample_table", columns, conditions, None);
        let result = "UPDATE sample_table\n    SET col1 = $1,\n    col2 = $2,\n    col3 = $3";

        assert_eq!(test_query.build_all().into_sql(), result);
    }

    #[test]
    fn update_with_conditions() {
        let mut conditions: Vec<Condition> = Vec::new();
        conditions.push(Condition::new(None, "id", "=", Some(5.into()), None));

        let columns: Vec<(&str, Value)> = vec![("col1", 5.into()), ("col2", 3.into()), ("col3", 7.into())];
        let mut test_query = UpdateBuilder::new("sample_table", columns, conditions, None);
        let result = "UPDATE sample_table\n    SET col1 = $1,\n    col2 = $2,\n    col3 = $3\nWHERE\n    id = $4";

        assert_eq!(test_query.build_all().into_sql(), result);
    }

    #[test]
    fn update_with_conditions_with_end() {
        let mut conditions: Vec<Condition> = Vec::new();
        conditions.push(Condition::new(None, "id", "=", Some(5.into()), None));

        let columns: Vec<(&str, Value)> = vec![("col1", 5.into()), ("col2", 3.into()), ("col3", 7.into())];
        let mut test_query = UpdateBuilder::new("sample_table", columns, conditions, Some("RETURNING id"));
        let result = "UPDATE sample_table\n    SET col1 = $1,\n    col2 = $2,\n    col3 = $3\nWHERE\n    id = $4\nRETURNING id";

        assert_eq!(test_query.build_all().into_sql(), result);
    }
}
