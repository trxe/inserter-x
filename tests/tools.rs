#[cfg(test)]
pub mod tests {
    use inserter_x::clickhouse::ClickhouseInserter;
    use polars::{frame::DataFrame, io::SerReader, prelude::{CsvParseOptions, CsvReadOptions, JsonReader, LazyFrame}, sql::SQLContext};

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
                .body(body)
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
        ins = if let Some(x) = db { ins.with_dbname(x) } else { ins };
        ins = if let Some(x) = engine { ins.with_engine(x) } else { ins };
        ins = if let Some(x) = creator { ins.with_create_method(x) } else { ins };
        ins = if let Some(x) = order_by_str { ins.with_order_by(x.split(",").map(|x| x.trim().to_owned()).collect()) } else { ins };
        ins = if let Some(x) = primary_key_str { ins.with_primary_key(x.split(",").map(|x| x.trim().to_owned()).collect()) } else { ins };
        ins = if let Some(x) = not_nullable_str { ins.with_not_null(x.split(",").map(|x| x.trim().to_owned()).collect()) } else { ins };
        ins.with_schema_from_cols(dataframe.get_columns()).expect("bad columns").build_queries().unwrap()
    }

    pub fn parse_json_from_url(table_name: &str, url: &str) -> (String, DataFrame) {
        let response = reqwest::blocking::get(url).unwrap();
        if let Err(e) = response.error_for_status_ref() {
            println!("unsuccessful: {}", e);
            panic!("json not obtained");
        }
        let text = response.text().expect("bad text");
        let reader = JsonReader::new(std::io::Cursor::new(text));
        (table_name.to_string(), reader.finish().expect("failed to read df"))
    }
}
