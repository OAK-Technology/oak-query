use serde_json::Value;
use sqlx::{QueryBuilder, Postgres};


pub type InsertValues = Vec<Option<Value>>;

pub struct InsertBuilder<'a> {
    pub table: &'a str,
    pub columns: &'a Vec<&'a str>,
    pub rows: &'a Vec<&'a InsertValues>,
    pub last_part: Option<&'a str>
}

impl <'a> InsertBuilder<'a> {
    pub fn new(table: &'a str, columns: &'a Vec<&'a str>, rows: &'a Vec<&'a InsertValues>, last_part: Option<&'a str>) -> Self {
        Self {
            table,
            columns,
            rows,
            last_part
        }
    }

    pub fn build(self) -> QueryBuilder<'a, Postgres> {
        let mut query: QueryBuilder<'_, Postgres> = QueryBuilder::new("");

        if self.rows.len() == 0 {
            return query;
        }

        query.push(format!("INSERT INTO {0}(", self.table));

        for (index, column) in self.columns.iter().enumerate() {
            if index < self.columns.len() - 1 {
                query.push(format!("{0}, ", *column));
            } else {
                query.push(format!("{0})\n", *column));
            }
        }

        query.push("VALUES\n");

        for (row_index, row) in self.rows.iter().enumerate() {
            if self.columns.len() == (*row).len()  {
                query.push("       (");

                for (col_index, value) in (*row).iter().enumerate() {
                    query.push_bind(value);

                    if col_index < (*row).len() - 1 {
                        query.push(", ");
                    }
                }

                if row_index < self.rows.len() - 1 {
                    query.push("),\n");
                } else {
                    query.push(")\n");
                }
            }
        }

        if let Some(last_part) = self.last_part {
            query.push(format!("{0}\n", last_part));
        }

        query
    }
}


#[cfg(test)]
mod tests {
    use crate::{InsertBuilder, InsertValues};
    
    #[test]
    fn insert_one_column_one_row<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: InsertValues = Vec::new();
        let mut rows: Vec<&InsertValues> = Vec::new();

        columns.push("column1");
        row1.push(Some("title1".into()));
        rows.push(&row1);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1)\nVALUES\n       ($1)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
    
    #[test]
    fn insert_one_column_two_rows<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: InsertValues = Vec::new();
        let mut row2: InsertValues = Vec::new();
        let mut rows: Vec<&InsertValues> = Vec::new();

        columns.push("column1");
        row1.push(Some("title1".into()));
        row2.push(Some("title2".into()));
        rows.push(&row1);
        rows.push(&row2);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1)\nVALUES\n       ($1),\n       ($2)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
    
    #[test]
    fn insert_two_column_one_row<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: InsertValues = Vec::new();
        let mut rows: Vec<&InsertValues> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        row1.push(Some("title1".into()));
        row1.push(Some(32.into()));
        rows.push(&row1);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1, column2)\nVALUES\n       ($1, $2)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
    
    #[test]
    fn insert_three_column_multi_rows<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: InsertValues = Vec::new();
        let mut row2: InsertValues = Vec::new();
        let mut row3: InsertValues = Vec::new();
        let mut row4: InsertValues = Vec::new();

        let mut rows: Vec<&InsertValues> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        columns.push("column3");

        row1.push(Some("title1".into()));
        row1.push(Some(32.into()));
        row1.push(Some("2023-01-01".into()));

        row2.push(Some("title2".into()));
        row2.push(Some(64.into()));
        row2.push(Some("2023-02-02".into()));

        row3.push(Some("title3".into()));
        row3.push(Some(18.into()));
        row3.push(Some("2023-03-03".into()));

        row4.push(Some("title4".into()));
        row4.push(Some(64.into()));
        row4.push(Some("2023-04-04".into()));

        rows.push(&row1);
        rows.push(&row2);
        rows.push(&row3);
        rows.push(&row4);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1, column2, column3)\nVALUES\n       ($1, $2, $3),\n       ($4, $5, $6),\n       ($7, $8, $9),\n       ($10, $11, $12)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
    
    #[test]
    fn insert_three_column_multi_rows_with_wrong_rows<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: InsertValues = Vec::new();
        let mut row2: InsertValues = Vec::new();
        let mut row3: InsertValues = Vec::new();
        let mut row4: InsertValues = Vec::new();

        let mut rows: Vec<&InsertValues> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        columns.push("column3");

        row1.push(Some("title1".into()));
        row1.push(Some(32.into()));
        row1.push(Some("2023-01-01".into()));

        row2.push(Some("title2".into()));
        row2.push(Some("2023-02-02".into()));

        row3.push(Some("title3".into()));
        row3.push(Some(18.into()));
        row4.push(Some("2023-03-03".into()));

        row4.push(Some("title4".into()));
        row4.push(Some(64.into()));

        rows.push(&row1);
        rows.push(&row2);
        rows.push(&row3);
        rows.push(&row4);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1, column2, column3)\nVALUES\n       ($1, $2, $3),\n       ($4, $5, $6)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }

    
    #[test]
    fn insert_three_column_two_rows_with_last_part<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: InsertValues = Vec::new();
        let mut row2: InsertValues = Vec::new();
        let mut rows: Vec<&InsertValues> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        columns.push("column3");

        row1.push(Some("title1".into()));
        row1.push(Some(32.into()));
        row1.push(Some("2023-01-01".into()));

        row2.push(Some("title2".into()));
        row2.push(Some(64.into()));
        row2.push(Some("2023-02-02".into()));

        rows.push(&row1);
        rows.push(&row2);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, Some("RETURNING id"));
        let result = "INSERT INTO sample_table(column1, column2, column3)\nVALUES\n       ($1, $2, $3),\n       ($4, $5, $6)\nRETURNING id\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
}