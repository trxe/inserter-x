use std::{collections::{HashMap, HashSet}, time::Instant};

use argh::FromArgs;
use arrow::{
    array::{downcast_array, ArrayBuilder, ArrayData, ArrayDataBuilder, AsArray, BinaryBuilder, ListBuilder, RecordBatch},
    datatypes::Field,
    ffi::from_ffi,
};
use arrow_array::{Array, GenericListArray};
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

#[derive(FromArgs, Clone)]
struct Args {
    #[argh(option, short = 'f')]
    pub filepath: String,
    #[argh(option, short = 'h')]
    pub host: String,
    #[argh(option, short = 'k')]
    pub table_details: Option<String>,
    #[argh(option)]
    pub not_null: Option<String>,
}

fn map_polars_sqltype_arrow(pl: &ArrowDataType) -> String {
    match pl {
        ArrowDataType::Null => "Null".to_owned(),
        ArrowDataType::Boolean => "Bool".to_owned(),
        ArrowDataType::Int8 => "Int8".to_owned(),
        ArrowDataType::Int16 => "Int16".to_owned(),
        ArrowDataType::Int32 => "Int32".to_owned(),
        ArrowDataType::Int64 => "Int64".to_owned(),
        ArrowDataType::UInt8 => "UInt8".to_owned(),
        ArrowDataType::UInt16 => "UInt16".to_owned(),
        ArrowDataType::UInt32 => "UInt32".to_owned(),
        ArrowDataType::UInt64 => "UInt64".to_owned(),
        ArrowDataType::Float32 => "Float32".to_owned(),
        ArrowDataType::Float64 => "Float64".to_owned(),
        ArrowDataType::Utf8 => "String".to_owned(),
        ArrowDataType::Utf8View => "String".to_owned(),
        ArrowDataType::Date32 => "Date32".to_owned(),
        ArrowDataType::Date64 => "DateTime".to_owned(),
        ArrowDataType::Timestamp(..) => "DateTime64".to_owned(),
        ArrowDataType::Time32(_) => "Timestamp".to_owned(),
        ArrowDataType::Time64(_) => "Timestamp".to_owned(),
        ArrowDataType::List(ll) => { 
            if ll.is_nullable{
                format!("Array(Nullable({}))", map_polars_sqltype_arrow(ll.dtype()))
            } else {
                format!("Array({})", map_polars_sqltype_arrow(ll.dtype()))
            }
        },
        ArrowDataType::LargeList(ll) => { 
            if ll.is_nullable{
                format!("Array(Nullable({}))", map_polars_sqltype_arrow(ll.dtype()))
            } else {
                format!("Array({})", map_polars_sqltype_arrow(ll.dtype()))
            }
        },
        ArrowDataType::FixedSizeList(ll, _) => { 
            if ll.is_nullable{
                format!("Array(Nullable({}))", map_polars_sqltype_arrow(ll.dtype()))
            } else {
                format!("Array({})", map_polars_sqltype_arrow(ll.dtype()))
            }
        },
        ArrowDataType::Struct(ls) => {
            let types = ls.iter().map(|ll| {
                if ll.is_nullable{
                    format!("Nullable({})", map_polars_sqltype_arrow(ll.dtype()))
                } else {
                    map_polars_sqltype_arrow(ll.dtype())
                }
            }).collect::<Vec<_>>().join(",");
            format!("Tuple({})", types)
        }
        x => { 
            println!("unrecognized type {:?}", x);
            panic!("not impl");
        },
    }
}

fn map_arrow_time_unit(tu: &ArrowTimeUnit) -> arrow::datatypes::TimeUnit {
    match tu {
        ArrowTimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
        ArrowTimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
        ArrowTimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
        ArrowTimeUnit::Second => arrow::datatypes::TimeUnit::Second,
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
        ArrowDataType::LargeUtf8 => arrow::datatypes::DataType::Binary,
        ArrowDataType::LargeBinary => arrow::datatypes::DataType::Binary,
        ArrowDataType::FixedSizeBinary(sz) => arrow::datatypes::DataType::FixedSizeBinary(*sz as i32),
        ArrowDataType::Date32 => arrow::datatypes::DataType::Date32,
        ArrowDataType::Date64 => arrow::datatypes::DataType::Date64,
        ArrowDataType::Timestamp(tu, tz) => {
            arrow::datatypes::DataType::Timestamp(map_arrow_time_unit(tu), tz.clone().map(|x| Arc::from(x.to_string())))
        }
        ArrowDataType::Time32(tu) => {
            arrow::datatypes::DataType::Time32(map_arrow_time_unit(tu))
        }
        ArrowDataType::Time64(tu) => {
            arrow::datatypes::DataType::Time64(map_arrow_time_unit(tu))
        }
        ArrowDataType::List(f) => {
            let rf = map_polars_dtype_arrow(f.dtype());
            arrow::datatypes::DataType::List(Arc::from(arrow::datatypes::Field::new(f.name.as_str(), rf, f.is_nullable)))
        }
        ArrowDataType::LargeList(f) => {
            let rf = map_polars_dtype_arrow(f.dtype());
            arrow::datatypes::DataType::LargeList(Arc::from(arrow::datatypes::Field::new(f.name.as_str(), rf, f.is_nullable)))
        }
        ArrowDataType::FixedSizeList(f, sz) => {
            let rf = map_polars_dtype_arrow(f.dtype());
            arrow::datatypes::DataType::FixedSizeList(Arc::from(arrow::datatypes::Field::new(f.name.as_str(), rf, f.is_nullable)), *sz as i32)
        }
        ArrowDataType::Struct(fs) => {
            let fields = fs.iter().map(|f| arrow::datatypes::Field::new(f.name.as_str(), map_polars_dtype_arrow(f.dtype()), f.is_nullable));
            arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::from_iter(fields))
        }
        ArrowDataType::Map(f, v) => {
            let rf = map_polars_dtype_arrow(f.dtype());
            arrow::datatypes::DataType::Map(Arc::from(arrow::datatypes::Field::new(f.name.as_str(), rf, f.is_nullable)), *v)
        }
        x => { 
            println!("unrecognized type {:?}", x);
            panic!("not impl");
        },
    }
}

fn convert_field(afield: Arc<Field>) -> Arc<Field> {
    match afield.data_type() {
        arrow::datatypes::DataType::List(l) => {
            Arc::new(arrow::datatypes::Field::new(afield.name().as_str(), arrow::datatypes::DataType::List(convert_field(l.clone())), afield.is_nullable()))
        }
        arrow::datatypes::DataType::LargeList(l) => {
            Arc::new(arrow::datatypes::Field::new(afield.name().as_str(), arrow::datatypes::DataType::LargeList(convert_field(l.clone())), afield.is_nullable()))
        }
        arrow::datatypes::DataType::FixedSizeList(l, sz) => {
            Arc::new(arrow::datatypes::Field::new(afield.name().as_str(), arrow::datatypes::DataType::FixedSizeList(convert_field(l.clone()), *sz), afield.is_nullable()))
        }
        arrow::datatypes::DataType::Struct(fs) => {
            let fields = arrow::datatypes::Fields::from_iter(fs.iter().map(|x| convert_field(x.clone())));
            Arc::new(arrow::datatypes::Field::new(afield.name().as_str(), arrow::datatypes::DataType::Struct(fields), afield.is_nullable()))
        }
        arrow::datatypes::DataType::Utf8View => {
            Arc::new(arrow::datatypes::Field::new(afield.name().as_str(), arrow::datatypes::DataType::Binary, afield.is_nullable()))
        }
        _ => afield
    }
}

fn convert_column(arr: Arc<dyn Array>) -> Arc<dyn Array> {
    match arr.data_type() { 
        arrow::datatypes::DataType::Utf8View => {
            let barray = arr.as_string_view();
            let mut builder = BinaryBuilder::with_capacity(arr.len(), 8 * 1024);
            for value in barray.iter() {
                builder.append_option(value);
            }
            let gba = builder.finish();
            arrow_array::array::make_array(gba.into_data())
        }
        arrow::datatypes::DataType::LargeList(f) => {
            if f.data_type().is_primitive() || f.data_type().is_temporal() || f.data_type().is_null() {
                return arr;
            }
            let larray = arr.as_list::<i64>();
            let (field, offsets, a, nulls) = larray.to_owned().into_parts();
            let gba = GenericListArray::new(convert_field(field), offsets, convert_column(a), nulls);
            arrow_array::array::make_array(gba.into_data())
        }
        arrow::datatypes::DataType::Struct(ls) => {
            let non_primitives = ls.iter().filter(|f| !(f.data_type().is_primitive() || f.data_type().is_temporal() || f.data_type().is_null())).collect::<Vec<_>>();
            if non_primitives.is_empty() {
                return arr;
            }
            let larray = arr.as_struct();
            let (fields, a, nulls) = larray.to_owned().into_parts();
            let mut new_fields = vec![];
            let mut new_arrays = vec![];
            for (idx, f) in fields.into_iter().enumerate() {
                new_fields.push(convert_field(f.clone()));
                new_arrays.push(convert_column(a[idx].clone()));
            }
            let structtype = arrow::datatypes::Fields::from_iter(new_fields);
            let gba = arrow::array::StructArray::new(structtype, new_arrays, nulls);
            arrow_array::array::make_array(gba.into_data())
        }
        _ => arr
    }
}

fn test_phy(frame: DataFrame) {
    frame.iter_chunks_physical().enumerate().for_each(|(idx, chunk)| {
        if idx > 0 {
            return;
        }
        let (sschema, arrays) = chunk.into_schema_and_arrays();
        let mut revarr = arrays.into_iter().rev().collect::<Vec<_>>();

        for (_, field) in sschema.iter() {
            let a = revarr.pop().unwrap();
            println!("{:?}\n{:?}", field, a);
        }
    });
}

fn convert(frame: DataFrame, args: Args, table_name: &str) {
    // let columns = frame.take_columns();
    // let chclient = clickhouse_rs::Pool::new(args.url);
    let mut bytes: Vec<u8> = vec![];
    let empty_str = PlSmallStr::from_str("");
    let mut coldesc: Vec<String> = vec![];
    let nullable_overrides = args.not_null.unwrap_or_default().split(",").map(|x| x.trim().to_owned()).collect::<HashSet<String>>();
    
    println!("{:?}", frame.schema());

    let mut sch = arrow::datatypes::SchemaBuilder::new();
    for column in frame.get_columns() {
        let pfield = column
            .dtype()
            .to_arrow_field(empty_str.clone(), CompatLevel::newest());
        coldesc.push(format!(
            "{} {} {} NULL",
            column.name(),
            map_polars_sqltype_arrow(pfield.dtype()).as_str(),
            if pfield.is_nullable && !pfield.dtype().is_nested() && !nullable_overrides.contains(column.name().as_str()) { "" } else { "NOT" }
        ));
        let afield = Field::new(
            column.name().as_str(),
            map_polars_dtype_arrow(pfield.dtype()),
            pfield.is_nullable,
        );
        sch.push(afield);
    }
    println!("{}\n{:?}", &args.filepath, frame);
    let schema = sch.finish();
    let mut streamer =
        StreamWriter::try_new(&mut bytes, &schema).expect("failed to build streamer");
    let aschema = Arc::new(schema);

    timer!("casting arrow", {
        frame
            .iter_chunks(CompatLevel::newest(), false)
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
                    let farr = convert_column(arr);
                    batch.push(farr);
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

pub fn meta_info() -> String {
        // sample response from https://api-web.nhle.com/v1/meta?players=8478401,8478402&teams=EDM,TOR
        r#"[
    {
      "playerId": 8478402,
      "playerSlug": "connor-mcdavid-8478402",
      "actionShot": "https://assets.nhle.com/mugs/actionshots/1296x729/8478402.jpg",
      "num": [92],
      "name": {
        "default": "Connor McDavid"
      },
      "positions": ["LW", "RW", "C"]
    },
    {
      "playerId": 8478401,
      "playerSlug": "pavel-zacha-8478401",
      "actionShot": "https://assets.nhle.com/mugs/actionshots/1296x729/8478401.jpg",
      "num": [37, 18],
      "name": {
        "default": "Pavel Zacha",
        "cs": "Eric Černák"
      },
      "positions": ["C"]
    }
]"#
        .to_string()
}

pub fn get_sample_df() -> DataFrame {
    let cur = std::io::Cursor::new(meta_info());
    let reader = JsonReader::new(cur);
    reader.finish().unwrap()
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
    // test_phy(df);
    let new_args = Args { filepath: "".to_owned(), host: args.host.clone(), table_details: Some("PRIMARY KEY playerId".to_owned()), not_null: Some("playerId".to_owned()) };
    timer!("full conversion", convert(df, args, &dfname));
    {
        let df = get_sample_df();
        timer!("full conversion", convert(df, new_args, "testnest"));
    }
}
