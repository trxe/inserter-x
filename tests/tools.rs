#[cfg(test)]
pub mod tests {
    use inserter_x::{clickhouse::ClickhouseInserter, common::PlDtype};
    use polars::{
        df,
        frame::DataFrame,
        io::SerReader,
        prelude::{IntoLazy, JsonReader, LazyFrame, col},
        sql::SQLContext,
    };

    pub fn get_sample_df_numerical() -> DataFrame {
        df!(
            "int8" => [-1, -2, 4, 100],
            "uint8" => [1, 2, 4, 100],
            "int16" => [-1, -2, 4, 100],
            "uint16" => [1, 2, 4, 100],
            "int32" => [-1, -2, 4, 100],
            "uint32" => [1, 2, 4, 100],
            "int64" => [-1, -2, 4, 100],
            "uint64" => [1, 2, 4, 100],
            "float" => [1.1, 2.2, 4.4, 100.001],
            "double" => [1.1, 2.2, 4.4, 100.001],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("int8").cast(PlDtype::Int8),
            col("uint8").cast(PlDtype::UInt8),
            col("int16").cast(PlDtype::Int16),
            col("uint16").cast(PlDtype::UInt16),
            col("int32").cast(PlDtype::Int32),
            col("uint32").cast(PlDtype::UInt32),
            col("int64").cast(PlDtype::Int64),
            col("uint64").cast(PlDtype::UInt64),
            col("float").cast(PlDtype::Float32),
            col("double").cast(PlDtype::Float64),
            col("double")
                .alias("decimal128")
                .cast(PlDtype::Decimal(Some(38), None)),
            col("double")
                .alias("decimal256")
                .cast(PlDtype::Decimal(None, Some(1))),
        ])
        .collect()
        .unwrap()
    }

    pub fn run_sql(query: &str, frames: &[(&str, LazyFrame)]) -> LazyFrame {
        let mut context = SQLContext::new();
        for (name, frame) in frames {
            context.register(name.to_owned(), frame.clone());
        }
        context.execute(query).unwrap()
    }

    pub fn send_db_from_inserter(host: &str, ch: &ClickhouseInserter, frame: &DataFrame) {
        let client = reqwest::blocking::Client::new();
        let body = ch.get_arrow_body(frame).expect("body");
        let reqbuilders = [
            client
                .post(host)
                .query(&[("query", ch.get_create_query().expect("insert"))])
                .header("Content-Length", 0),
            client
                .post(host)
                .query(&[("query", ch.get_insert_query().expect("insert"))])
                .header("Content-Length", body.len())
                .body(body),
        ];
        for req in reqbuilders {
            match req.send() {
                Ok(x) => {
                    println!("Response status [{}]: {:?}", x.status(), x.text());
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn clickhouse_converter(
        dfname: &str,
        dataframe: &DataFrame,
        db: Option<&str>,
        engine: Option<&str>,
        order_by_str: Option<&str>,
        primary_key_str: Option<&str>,
        not_nullable_str: Option<&str>,
        creator: Option<&str>,
    ) -> ClickhouseInserter {
        let mut ins = ClickhouseInserter::default(dfname);
        ins = if let Some(x) = db {
            ins.with_dbname(x)
        } else {
            ins
        };
        ins = if let Some(x) = engine {
            ins.with_engine(x)
        } else {
            ins
        };
        ins = if let Some(x) = creator {
            ins.with_create_method(x)
        } else {
            ins
        };
        ins = if let Some(x) = order_by_str {
            ins.with_order_by(x.split(",").map(|x| x.trim().to_owned()).collect())
        } else {
            ins
        };
        ins = if let Some(x) = primary_key_str {
            ins.with_primary_key(x.split(",").map(|x| x.trim().to_owned()).collect())
        } else {
            ins
        };
        ins = if let Some(x) = not_nullable_str {
            ins.with_not_null(x.split(",").map(|x| x.trim().to_owned()).collect())
        } else {
            ins
        };
        ins.with_schema_from_cols(dataframe.get_columns())
            .expect("bad columns")
            .build_queries()
            .unwrap()
    }

    pub fn parse_json_from_url(table_name: &str, url: &str) -> (String, DataFrame) {
        let response = reqwest::blocking::get(url).unwrap();
        if let Err(e) = response.error_for_status_ref() {
            println!("unsuccessful: {}", e);
            panic!("json not obtained");
        }
        let text = response.text().expect("bad text");
        let reader = JsonReader::new(std::io::Cursor::new(text));
        (
            table_name.to_string(),
            reader.finish().expect("failed to read df"),
        )
    }
}
