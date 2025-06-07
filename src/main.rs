use std::{collections::{HashMap, HashSet}, time::Instant};

use argh::FromArgs;
use arrow::{
    array::{downcast_array, ArrayData, ArrayDataBuilder, AsArray, BinaryBuilder, RecordBatch},
    datatypes::Field,
    ffi::from_ffi,
};
use arrow_array::Array;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use polars::prelude::*;
use polars_arrow::ffi::{export_array_to_c, export_field_to_c};
use reqwest::header::HeaderMap;

struct Timer {
    pub name: String,
    pub start: Instant
}

impl Timer {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_owned(), start: Instant::now() }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        println!("{}: {:?}", self.name, self.start.elapsed());
    }
}

macro_rules! timer {
    ($name:expr, $res:expr) => {
        {
            let _timer = Timer::new($name);
            $res
        }
    };
}

#[derive(FromArgs)]
struct Args {
    #[argh(option, short = 'f')]
    pub filepath: String,
    #[argh(option, short = 'h')]
    pub host: String,
    #[argh(option, short = 'k')]
    pub table_details: Option<String>,
}

fn map_polars_sqltype_arrow(pl: &ArrowDataType) -> &'static str {
    match pl {
        ArrowDataType::Null => "Null",
        ArrowDataType::Boolean => "Bool",
        ArrowDataType::Int8 => "Int8",
        ArrowDataType::Int16 => "Int16",
        ArrowDataType::Int32 => "Int32",
        ArrowDataType::Int64 => "Int64",
        ArrowDataType::UInt8 => "UInt8",
        ArrowDataType::UInt16 => "UInt16",
        ArrowDataType::UInt32 => "UInt32",
        ArrowDataType::UInt64 => "UInt64",
        ArrowDataType::Float32 => "Float32",
        ArrowDataType::Float64 => "Float64",
        ArrowDataType::Utf8 => "String",
        ArrowDataType::Utf8View => "String",
        ArrowDataType::Date32 => "Date32",
        ArrowDataType::Date64 => "DateTime",
        ArrowDataType::Timestamp(_, _) => "DateTime64",
        ArrowDataType::Time32(_) => "Timestamp",
        ArrowDataType::Time64(_) => "Timestamp",
        ArrowDataType::List(_) => "Array",
        ArrowDataType::Struct(_) => "Tuple",
        x => { 
            println!("unrecognized type {:?}", x);
            panic!("not impl");
        },
    }
}

fn map_polars_dtype_arrow(pl: &ArrowDataType) -> arrow::datatypes::DataType {
    match pl {
        ArrowDataType::Null => arrow::datatypes::DataType::Null,
        ArrowDataType::Boolean => arrow::datatypes::DataType::Boolean,
        ArrowDataType::Int8 => arrow::datatypes::DataType::Int8,
        ArrowDataType::Int16 => arrow::datatypes::DataType::Int16,
        ArrowDataType::Int32 => arrow::datatypes::DataType::Int32,
        ArrowDataType::Int64 => arrow::datatypes::DataType::Int64,
        ArrowDataType::Float32 => arrow::datatypes::DataType::Float32,
        ArrowDataType::Float64 => arrow::datatypes::DataType::Float64,
        ArrowDataType::Binary => arrow::datatypes::DataType::Binary,
        ArrowDataType::Utf8 => arrow::datatypes::DataType::Binary,
        ArrowDataType::Utf8View => arrow::datatypes::DataType::Binary,
        x => { 
            println!("unrecognized type {:?}", x);
            panic!("not impl");
        },
    }
}

fn test_phy(frame: DataFrame) {
    frame.iter_chunks_physical().for_each(|chunk| {
        let (sschema, arrays) = chunk.into_schema_and_arrays();
        let mut revarr = arrays.into_iter().rev().collect::<Vec<_>>();

        for (_, field) in sschema.iter() {
            let arr = revarr.pop().expect("mismatched schema and arr");
            println!("{:?}\n{:?}", field, arr)
        }
    });
}

fn convert(frame: DataFrame, args: Args, table_name: &str) {
    // let columns = frame.take_columns();
    // let chclient = clickhouse_rs::Pool::new(args.url);
    let mut bytes: Vec<u8> = vec![];
    let empty_str = PlSmallStr::from_str("");
    let mut coldesc: Vec<String> = vec![];

    let string_cols = frame
        .get_columns()
        .iter()
        .filter(|c| *c.dtype() == polars::prelude::DataType::String)
        .map(|c| c.name().to_string())
        .collect::<HashSet<_>>();
    
    println!("{:?}", frame.schema());

    let mut sch = arrow::datatypes::SchemaBuilder::new();
    for column in frame.get_columns() {
        let pfield = column
            .dtype()
            .to_arrow_field(empty_str.clone(), CompatLevel::newest());
        coldesc.push(format!(
            "{} {}",
            column.name(),
            map_polars_sqltype_arrow(pfield.dtype())
        ));
        let afield = Field::new(
            column.name().as_str(),
            map_polars_dtype_arrow(pfield.dtype()),
            pfield.is_nullable,
        );
        sch.push(afield);
    }
    let fframe = frame.lazy().cast(string_cols.iter().map(|name| (
                name.as_str(), polars::prelude::DataType::Binary
    )).collect(), true).collect().expect("cast failed");
    println!("{}\n{:?}", &args.filepath, fframe);
    let schema = sch.finish();
    let mut streamer =
        StreamWriter::try_new(&mut bytes, &schema).expect("failed to build streamer");
    let aschema = Arc::new(schema);

    timer!("casting arrow", {
        fframe
            // .iter_chunks(CompatLevel::newest(), false)
            .iter_chunks_physical()
            .for_each(|chunk| {
                let (sschema, arrays) = chunk.into_schema_and_arrays();
                let mut revarr = arrays.into_iter().rev().collect::<Vec<_>>();
                let mut batch = vec![];

                for (_, field) in sschema.iter() {
                    let cfield = unsafe {
                        std::mem::transmute::<
                            polars_arrow::ffi::ArrowSchema,
                            arrow::array::ffi::FFI_ArrowSchema,
                        >(export_field_to_c(field))
                    };
                    let arr = revarr.pop().expect("mismatched schema and arr");
                    let carr = unsafe {
                        std::mem::transmute::<
                            polars_arrow::ffi::ArrowArray,
                            arrow::array::ffi::FFI_ArrowArray,
                        >(export_array_to_c(arr))
                    };
                    let array_data = unsafe { from_ffi(carr, &cfield) }.expect("arrow column");
                    let arr = arrow_array::array::make_array(array_data);
                    if *arr.data_type() == arrow::datatypes::DataType::BinaryView {
                        let barray = arr.as_binary_view();
                        let mut builder = BinaryBuilder::with_capacity(arr.len(), 8 * 1024);
                        for value in barray.iter() {
                            builder.append_option(value);
                        }
                        let gba = builder.finish();
                        let farr = arrow_array::array::make_array(gba.to_data());
                        batch.push(farr);
                    } else if arr.data_type().is_nested() {

                    } else {
                        batch.push(arr);
                    }
                }
                let arrow_batch = RecordBatch::try_new(aschema.clone(), batch).expect("batched");
                streamer
                    .write(&arrow_batch)
                    .expect("failed writing to stream");
            });
    });

    let content_len = bytes.len();

    let client = reqwest::blocking::Client::new();
    // build table
    let columns = coldesc.join(",\n\t");
    let create_cmd = format!(
        "CREATE TABLE IF NOT EXISTS {} (\n\t{}\n) {}", table_name, columns, args.table_details.unwrap_or_default()
    );
    let trunc_cmd = format!("TRUNCATE {}", table_name);
    let insert_cmd = format!("INSERT INTO {} FORMAT ArrowStream", table_name);
    println!(
        "Url: {}\nCreate: {}\nInsert: {}",
        &args.host, create_cmd, insert_cmd
    );
    let reqbuilders = [
        client
            .post(&args.host)
            .query(&[("query", create_cmd.as_str())])
            .header("Content-Length", 0),
        client
            .post(&args.host)
            .query(&[("query", trunc_cmd.as_str())])
            .header("Content-Length", 0),
        client
            .post(&args.host)
            .query(&[("query", insert_cmd.as_str())])
            .body(bytes)
            .header("Content-Length", content_len),
    ];
    for req in reqbuilders {
        timer!("req", {
            match req.send() {
                Ok(x) => {
                    println!("Response status [{}]: {:?}", x.status(), x.text());
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        });
    }
}

fn main() {
    let args: Args = argh::from_env();
    let path = std::path::Path::new(&args.filepath);
    let dfname = path.file_stem().unwrap().to_str().unwrap().to_owned();
    let parse_options = CsvParseOptions::default().with_try_parse_dates(true);
    let reader = CsvReadOptions::default()
        .with_parse_options(parse_options)
        .try_into_reader_with_file_path(Some(args.filepath.as_str().into())).expect("reader");
    let df = timer!("collect df", reader.finish().expect("failed to read df"));
    test_phy(df);
    // timer!("full conversion", convert(df, args, &dfname));
}
