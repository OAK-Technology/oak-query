use sqlx::{QueryBuilder, Postgres};
use crate::ColumnType;


pub type Row = Vec<ColumnType>;

pub struct InsertBuilder<'a> {
    pub table: &'a str,
    pub columns: &'a Vec<&'a str>,
    pub rows: &'a Vec<&'a Row>,
    pub last_part: Option<&'a str>
}

impl <'a> InsertBuilder<'a> {
    pub fn new(table: &'a str, columns: &'a Vec<&'a str>, rows: &'a Vec<&'a Row>, last_part: Option<&'a str>) -> Self {
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
                    match value {
                        ColumnType::OptPrimitive(opt) => {
                            match opt {
                                Some(v) => { query.push_bind(v); },
                                None => { query.push("default"); }
                            }
                        },
                        ColumnType::OptDateTime(opt) => {
                            match opt {
                                Some(v) => { query.push_bind(v); },
                                None => { query.push("default"); }
                            }
                        },
                    }

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
    use crate::{InsertBuilder, Row, ColumnType};
    
    #[test]
    fn insert_one_column_one_row<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: Row = Vec::new();
        let mut rows: Vec<&Row> = Vec::new();

        columns.push("column1");
        row1.push(ColumnType::OptPrimitive(Some("title1".into())));
        rows.push(&row1);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1)\nVALUES\n       ($1)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
    
    #[test]
    fn insert_one_column_two_rows<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: Row = Vec::new();
        let mut row2: Row = Vec::new();
        let mut rows: Vec<&Row> = Vec::new();

        columns.push("column1");
        row1.push(ColumnType::OptPrimitive(Some("title1".into())));
        row2.push(ColumnType::OptPrimitive(Some("title2".into())));
        rows.push(&row1);
        rows.push(&row2);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1)\nVALUES\n       ($1),\n       ($2)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
    
    #[test]
    fn insert_two_column_one_row<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: Row = Vec::new();
        let mut rows: Vec<&Row> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        row1.push(ColumnType::OptPrimitive(Some("title1".into())));
        row1.push(ColumnType::OptPrimitive(Some(32.into())));
        rows.push(&row1);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1, column2)\nVALUES\n       ($1, $2)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
    
    #[test]
    fn insert_three_column_multi_rows<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: Row = Vec::new();
        let mut row2: Row = Vec::new();
        let mut row3: Row = Vec::new();
        let mut row4: Row = Vec::new();

        let mut rows: Vec<&Row> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        columns.push("column3");

        row1.push(ColumnType::OptPrimitive(Some("title1".into())));
        row1.push(ColumnType::OptPrimitive(Some(32.into())));
        row1.push(ColumnType::OptPrimitive(Some("2023-01-01".into())));

        row2.push(ColumnType::OptPrimitive(Some("title2".into())));
        row2.push(ColumnType::OptPrimitive(Some(64.into())));
        row2.push(ColumnType::OptPrimitive(Some("2023-02-02".into())));

        row3.push(ColumnType::OptPrimitive(Some("title3".into())));
        row3.push(ColumnType::OptPrimitive(Some(18.into())));
        row3.push(ColumnType::OptPrimitive(Some("2023-03-03".into())));

        row4.push(ColumnType::OptPrimitive(Some("title4".into())));
        row4.push(ColumnType::OptPrimitive(Some(64.into())));
        row4.push(ColumnType::OptPrimitive(Some("2023-04-04".into())));

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
        let mut row1: Row = Vec::new();
        let mut row2: Row = Vec::new();
        let mut row3: Row = Vec::new();
        let mut row4: Row = Vec::new();

        let mut rows: Vec<&Row> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        columns.push("column3");

        row1.push(ColumnType::OptPrimitive(Some("title1".into())));
        row1.push(ColumnType::OptPrimitive(Some(32.into())));
        row1.push(ColumnType::OptPrimitive(Some("2023-01-01".into())));

        row2.push(ColumnType::OptPrimitive(Some("title2".into())));
        row2.push(ColumnType::OptPrimitive(Some("2023-02-02".into())));

        row3.push(ColumnType::OptPrimitive(Some("title3".into())));
        row3.push(ColumnType::OptPrimitive(Some(18.into())));
        row4.push(ColumnType::OptPrimitive(Some("2023-03-03".into())));

        row4.push(ColumnType::OptPrimitive(Some("title4".into())));
        row4.push(ColumnType::OptPrimitive(Some(64.into())));

        rows.push(&row1);
        rows.push(&row2);
        rows.push(&row3);
        rows.push(&row4);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1, column2, column3)\nVALUES\n       ($1, $2, $3),\n       ($4, $5, $6)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
    
    #[test]
    fn insert_three_column_multi_rows_with_none_values<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: Row = Vec::new();
        let mut row2: Row = Vec::new();
        let mut row3: Row = Vec::new();
        let mut row4: Row = Vec::new();

        let mut rows: Vec<&Row> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        columns.push("column3");

        row1.push(ColumnType::OptPrimitive(Some("title1".into())));
        row1.push(ColumnType::OptPrimitive(Some(32.into())));
        row1.push(ColumnType::OptPrimitive(Some("2023-01-01".into())));

        row2.push(ColumnType::OptPrimitive(Some("title2".into())));
        row2.push(ColumnType::OptPrimitive(None));
        row2.push(ColumnType::OptPrimitive(Some("2023-02-02".into())));

        row3.push(ColumnType::OptPrimitive(Some("title3".into())));
        row3.push(ColumnType::OptPrimitive(Some(18.into())));
        row3.push(ColumnType::OptPrimitive(Some("2023-03-03".into())));

        row4.push(ColumnType::OptPrimitive(Some("title4".into())));
        row4.push(ColumnType::OptPrimitive(Some(64.into())));
        row4.push(ColumnType::OptPrimitive(None));

        rows.push(&row1);
        rows.push(&row2);
        rows.push(&row3);
        rows.push(&row4);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, None);
        let result = "INSERT INTO sample_table(column1, column2, column3)\nVALUES\n       ($1, $2, $3),\n       ($4, default, $5),\n       ($6, $7, $8),\n       ($9, $10, default)\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }

    
    #[test]
    fn insert_three_column_two_rows_with_last_part<'a>() {
        let mut columns: Vec<&'a str> = Vec::new();
        let mut row1: Row = Vec::new();
        let mut row2: Row = Vec::new();
        let mut rows: Vec<&Row> = Vec::new();

        columns.push("column1");
        columns.push("column2");
        columns.push("column3");

        row1.push(ColumnType::OptPrimitive(Some("title1".into())));
        row1.push(ColumnType::OptPrimitive(Some(32.into())));
        row1.push(ColumnType::OptPrimitive(Some("2023-01-01".into())));

        row2.push(ColumnType::OptPrimitive(Some("title2".into())));
        row2.push(ColumnType::OptPrimitive(Some(64.into())));
        row2.push(ColumnType::OptPrimitive(Some("2023-02-02".into())));

        rows.push(&row1);
        rows.push(&row2);

        let insert_query = InsertBuilder::new("sample_table", &columns, &rows, Some("RETURNING id"));
        let result = "INSERT INTO sample_table(column1, column2, column3)\nVALUES\n       ($1, $2, $3),\n       ($4, $5, $6)\nRETURNING id\n";

        assert_eq!(insert_query.build().into_sql(), result);
    }
}
