use std::path::Path;

use argh::FromArgs;
use polars::prelude::*;
use polars_arrow::{array::{Array, NullArray, PrimitiveArray, Utf8ViewArray}, ffi::{export_array_to_c, export_field_to_c, import_field_from_c}};


#[derive(FromArgs)]
struct Args {
    #[argh(option, short = 'f')]
    pub filepath: String,
    #[argh(option, short = 'h')]
    pub url: String,
}

struct CArrowColumn {
    //  pub array: Vec<polars_arrow::ffi::ArrowArray>,
    //  pub schema: polars_arrow::ffi::ArrowSchema,
    pub array: Vec<Box<dyn Array>>,
    pub schema: ArrowField,
}

impl CArrowColumn {
    pub fn new(c: polars::prelude::Column) -> Self {
        let series = c.take_materialized_series();
        let field = series.field().to_arrow(CompatLevel::newest());
        let arrow_chunks = series.into_chunks();
        //  let c_field = export_field_to_c(&field);
        //  let c_array = arrow_chunks.into_iter().map(export_array_to_c).collect();
        Self { array: arrow_chunks, schema: field }
    }
}

fn convert(frame: DataFrame, args: Args) {
    // let columns = frame.take_columns();
    // let chclient = clickhouse_rs::Pool::new(args.url);
    let bytes: Vec<u8> = vec![];

    frame.iter_chunks(CompatLevel::newest(), false).for_each(|chunk| {
        println!("{:?}", chunk);
        let res = chunk.schema().clone().into_iter().map(|(_, field)| { 
            let data = 
        });
    });
    // let carr = columns.into_iter().map(CArrowColumn::new).collect::<Vec<_>>();
}

fn main() {
    let args: Args = argh::from_env();
    let reader = LazyCsvReader::new(std::path::Path::new(&args.filepath));
    let lf = reader.finish().expect("invalid frame filepath");
    let df = lf.collect().unwrap();
    println!("{}\n{:?}", &args.filepath, df);
    convert(df, args);
}
