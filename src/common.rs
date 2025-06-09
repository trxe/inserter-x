use std::sync::Arc;

use arrow::{array::AsArray, ffi::from_ffi};
use arrow_array::Array;
use arrow_ipc::writer::StreamWriter;
use polars::prelude::CompatLevel;
use polars_arrow::ffi::{export_array_to_c, export_field_to_c};
use thiserror::Error;

pub type PlColumn = polars::prelude::Column;
pub type PlArrowDtype = polars::prelude::ArrowDataType;
pub type ArDtype = arrow::datatypes::DataType;
pub type ArField = arrow::datatypes::Field;
pub type ArFields = arrow::datatypes::Fields;

#[derive(Error, Debug)]
pub enum InsError {
    #[error("Failed to build {0}: {1}")]
    BuildError(&'static str, String),
    #[error("Failed to convert ({0}): {1}")]
    ConvertError(&'static str, String),
    #[error("No conversion of {0}")]
    ConversionNotImplementedError(String),
}

pub type InsResult<T> = Result<T, InsError>;

pub trait CreateCmd {
    fn field(name: &str, is_nullable: bool, dtype: Option<&str>, constraint: Option<&str>) -> String {
        if let Some(d) = dtype {
            format!(
                "{} {} {} NULL",
                name,
                d,
                (if is_nullable { "" } else { "NOT" }).to_owned(),
            )
        } else {
            format!("{} {}", name, constraint.unwrap_or_default())
        }
    }
    fn table(
        table_name: &str,
        columns: &[String],
        table_config: &str,
        override_creation: Option<&str>,
    ) -> String {
        let col_spec = columns.join(",\n\t");
        let creator = override_creation.unwrap_or("CREATE TABLE IF NOT EXISTS");
        format!(
            "{} {} (\n\t{}\n) {}",
            creator, table_name, col_spec, table_config
        )
    }

    fn insert(table_name: &str) -> String {
        format!("INSERT INTO {} FORMAT ArrowStream", table_name)
    }
}

pub fn polars_to_arrow_time_unit(
    tu: &polars::prelude::ArrowTimeUnit,
) -> arrow::datatypes::TimeUnit {
    match tu {
        polars::prelude::ArrowTimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
        polars::prelude::ArrowTimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
        polars::prelude::ArrowTimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
        polars::prelude::ArrowTimeUnit::Second => arrow::datatypes::TimeUnit::Second,
    }
}

pub fn polars_to_arrow_datatype(pl: &PlArrowDtype) -> InsResult<ArDtype> {
    Ok(match pl {
        PlArrowDtype::Null => ArDtype::Null,
        PlArrowDtype::Boolean => ArDtype::Boolean,
        PlArrowDtype::Int8 => ArDtype::Int8,
        PlArrowDtype::Int16 => ArDtype::Int16,
        PlArrowDtype::Int32 => ArDtype::Int32,
        PlArrowDtype::Int64 => ArDtype::Int64,
        PlArrowDtype::Float32 => ArDtype::Float32,
        PlArrowDtype::Float64 => ArDtype::Float64,
        PlArrowDtype::Binary => ArDtype::Binary,
        PlArrowDtype::Utf8 => ArDtype::Binary,
        PlArrowDtype::Utf8View => ArDtype::Binary,
        PlArrowDtype::LargeUtf8 => ArDtype::Binary,
        PlArrowDtype::LargeBinary => ArDtype::Binary,
        PlArrowDtype::FixedSizeBinary(sz) => ArDtype::FixedSizeBinary(*sz as i32),
        PlArrowDtype::Date32 => ArDtype::Date32,
        PlArrowDtype::Date64 => ArDtype::Date64,
        PlArrowDtype::Timestamp(tu, tz) => ArDtype::Timestamp(
            polars_to_arrow_time_unit(tu),
            tz.clone().map(|x| Arc::from(x.to_string())),
        ),
        PlArrowDtype::Time32(tu) => ArDtype::Time32(polars_to_arrow_time_unit(tu)),
        PlArrowDtype::Time64(tu) => ArDtype::Time64(polars_to_arrow_time_unit(tu)),
        PlArrowDtype::List(f) => {
            let rf = polars_to_arrow_datatype(f.dtype())?;
            ArDtype::List(Arc::from(ArField::new(f.name.as_str(), rf, f.is_nullable)))
        }
        PlArrowDtype::LargeList(f) => {
            let rf = polars_to_arrow_datatype(f.dtype())?;
            ArDtype::LargeList(Arc::from(ArField::new(f.name.as_str(), rf, f.is_nullable)))
        }
        PlArrowDtype::FixedSizeList(f, sz) => {
            let rf = polars_to_arrow_datatype(f.dtype())?;
            ArDtype::FixedSizeList(
                Arc::from(ArField::new(f.name.as_str(), rf, f.is_nullable)),
                *sz as i32,
            )
        }
        PlArrowDtype::Struct(fs) => {
            let mut new_fields = vec![];
            for f in fs {
                new_fields.push(ArField::new(
                    f.name.as_str(),
                    polars_to_arrow_datatype(f.dtype())?,
                    f.is_nullable,
                ));
            }
            ArDtype::Struct(ArFields::from_iter(new_fields))
        }
        PlArrowDtype::Map(f, v) => {
            let rf = polars_to_arrow_datatype(f.dtype())?;
            ArDtype::Map(
                Arc::from(ArField::new(f.name.as_str(), rf, f.is_nullable)),
                *v,
            )
        }
        x => {
            return Err(InsError::ConversionNotImplementedError(format!(
                "polars_to_arrow: {:?}",
                x
            )));
        }
    })
}

fn convert_field(afield: Arc<ArField>) -> Arc<ArField> {
    match afield.data_type() {
        ArDtype::List(l) => Arc::new(ArField::new(
            afield.name().as_str(),
            ArDtype::List(convert_field(l.clone())),
            afield.is_nullable(),
        )),
        ArDtype::LargeList(l) => Arc::new(ArField::new(
            afield.name().as_str(),
            ArDtype::LargeList(convert_field(l.clone())),
            afield.is_nullable(),
        )),
        ArDtype::FixedSizeList(l, sz) => Arc::new(ArField::new(
            afield.name().as_str(),
            ArDtype::FixedSizeList(convert_field(l.clone()), *sz),
            afield.is_nullable(),
        )),
        ArDtype::Struct(fs) => {
            let fields = ArFields::from_iter(fs.iter().map(|x| convert_field(x.clone())));
            Arc::new(ArField::new(
                afield.name().as_str(),
                ArDtype::Struct(fields),
                afield.is_nullable(),
            ))
        }
        ArDtype::Utf8View => Arc::new(ArField::new(
            afield.name().as_str(),
            ArDtype::Binary,
            afield.is_nullable(),
        )),
        _ => afield,
    }
}

fn no_flatten_required(f: &ArField) -> bool {
    f.data_type().is_primitive() || f.data_type().is_temporal() || f.data_type().is_null()
}

fn convert_column(arr: Arc<dyn arrow::array::Array>) -> Arc<dyn arrow::array::Array> {
    match arr.data_type() {
        arrow::datatypes::DataType::Utf8View => {
            let barray = arr.as_string_view();
            let mut builder = arrow::array::BinaryBuilder::with_capacity(arr.len(), 8 * 1024);
            for value in barray.iter() {
                builder.append_option(value);
            }
            let gba = builder.finish();
            arrow_array::array::make_array(gba.into_data())
        }
        arrow::datatypes::DataType::LargeList(f) => {
            if no_flatten_required(f) {
                return arr;
            }
            let larray = arr.as_list::<i64>();
            let (field, offsets, a, nulls) = larray.to_owned().into_parts();
            let gba = arrow::array::GenericListArray::new(
                convert_field(field),
                offsets,
                convert_column(a),
                nulls,
            );
            arrow_array::array::make_array(gba.into_data())
        }
        arrow::datatypes::DataType::Struct(ls) => {
            let non_primitives = ls
                .iter()
                .filter(|f| !(no_flatten_required(f)))
                .collect::<Vec<_>>();
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
        _ => arr,
    }
}

pub fn arrow_to_bytes(
    schema: Arc<arrow::datatypes::Schema>,
    frame: &polars::prelude::DataFrame,
) -> InsResult<Vec<u8>> {
    let mut bytes = vec![];
    let mut streamer = match StreamWriter::try_new(&mut bytes, &schema) {
        Ok(x) => x,
        Err(e) => {
            return Err(InsError::BuildError("arrow StreamWriter", e.to_string()));
        }
    };
    let sschema = frame.schema().iter().map(|(name, field)| field.to_arrow_field(name.clone(), CompatLevel::newest())).collect::<Vec<_>>();
    for chunk in frame.iter_chunks(CompatLevel::newest(), false) {
        let arrays = chunk.arrays();
        let mut revarr = arrays.iter().rev().collect::<Vec<_>>();
        let mut batch = vec![];

        for field in sschema.iter() {
            let cfield = unsafe {
                std::mem::transmute::<
                    polars_arrow::ffi::ArrowSchema,
                    arrow::array::ffi::FFI_ArrowSchema,
                >(export_field_to_c(field))
            };
            let arr = revarr.pop().unwrap();
            let carr = unsafe {
                std::mem::transmute::<
                    polars_arrow::ffi::ArrowArray,
                    arrow::array::ffi::FFI_ArrowArray,
                >(export_array_to_c(arr.to_owned()))
            };
            let array_data = match unsafe { from_ffi(carr, &cfield) } {
                Ok(x) => x,
                Err(e) => {
                    return Err(InsError::ConvertError(
                        "bad conversion from polars to arrow",
                        format!("({:?})\n{}", field, e),
                    ));
                }
            };
            let arr = arrow_array::array::make_array(array_data);
            let farr = convert_column(arr);
            batch.push(farr);
        }
        let arrow_batch =
            arrow_array::RecordBatch::try_new(schema.clone(), batch).expect("batched");
        match streamer.write(&arrow_batch) {
            Ok(_) => {}
            Err(e) => {
                return Err(InsError::ConvertError(
                    "failed writing batch to stream",
                    e.to_string(),
                ));
            }
        }
    }
    Ok(bytes)
}
