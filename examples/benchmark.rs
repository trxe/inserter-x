use std::time::Instant;

use argh::{from_env, FromArgs};
use inserter_x::clickhouse::ClickhouseInserter;
use polars::{io::SerReader, prelude::{CsvParseOptions, CsvReadOptions}};

#[derive(FromArgs, Clone)]
#[argh(description = "Benchmark")]
struct Args {
    #[argh(option, short = 'f', description = "csv filepath")]
    pub filepath: String,
    #[argh(option, short = 'h', description = "database url")]
    pub host: String,
    #[argh(option, short = 'o', description = "order_by")]
    pub order_by: Option<String>,
    #[argh(option, short = 'p', description = "primary_keys")]
    pub primary_keys: Option<String>,
    #[argh(option, description = "list of not null")]
    pub not_null: Option<String>,
}

pub struct Timer {
    pub name: String,
    pub start: Instant,
}

impl Timer {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            start: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        println!("{}: {:?}", self.name, self.start.elapsed());
    }
}

#[macro_export]
macro_rules! timer {
    ($name:expr, $res:expr) => {{
        let _timer = Timer::new($name);
        $res
    }};
}

fn split_to_keys(orig: &str) -> Vec<String> {
    orig.split(",").map(|x| x.trim().to_owned()).filter(|x| !x.is_empty()).collect()
}

fn main() {
    let args: Args = from_env();
    let path = std::path::Path::new(&args.filepath);
    let dfname = path.file_stem().unwrap().to_str().unwrap().to_owned();
    let parse_options = CsvParseOptions::default().with_try_parse_dates(true);
    let reader = CsvReadOptions::default()
        .with_parse_options(parse_options)
        .try_into_reader_with_file_path(Some(args.filepath.into())).expect("csv reader");
    let frame = timer!("import", reader.finish().expect("failed to read df"));
    let order_by = split_to_keys(args.order_by.as_deref().unwrap_or_default());
    let primary_keys = split_to_keys(args.primary_keys.as_deref().unwrap_or_default());
    let extra_nullable = split_to_keys(args.not_null.as_deref().unwrap_or_default());
    println!("ORDER BY: {:?}\nPRIMARY KEYS: {:?}\nNULL: {:?}", order_by, primary_keys, extra_nullable);
    let ch = ClickhouseInserter::default(&dfname)
        .with_engine("MergeTree")
        .with_order_by(order_by)
        .with_primary_key(primary_keys)
        .with_not_null(extra_nullable)
        .with_create_method("CREATE OR REPLACE TABLE")
        .with_schema_from_cols(frame.get_columns())
        .expect("bad columns").build_queries().unwrap();
    let client = reqwest::blocking::Client::new();
    let body = timer!("creating arrow transport", ch.get_arrow_body(&frame).expect("body"));
    println!("CREATE: {}", ch.get_create_query().expect("create"));
    let reqbuilders = [
        ("create", client
            .post(&args.host)
            .query(&[("query", ch.get_create_query().expect("create"))])
            .header("Content-Length", 0)),
        ("insert", client
            .post(&args.host)
            .query(&[("query", ch.get_insert_query().expect("insert"))])
            .header("Content-Length", body.len())
            .body(body))
    ];
    for (label, req) in reqbuilders {
        match timer!(label, req.send()) {
            Ok(x) => {
                println!("Response status [{}]: {:?}", x.status(), x.text());
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }

}
