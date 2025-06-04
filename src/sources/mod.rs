use std::fmt::Display;

use polars::{frame::DataFrame, prelude::Column};

// simplification
pub enum PgType {
    Boolean,
    Char,
    VarChar,
    Text,
    Date,
    Int,
    BigInt,
    Float,
    Double,
    Time,
    Timestamp,
    Array(Box<PgType>)
}

impl Display for PgType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            PgType::Text => write!(f, "TEXT"),
            PgType::Char => write!(f, "CHAR"),
            PgType::VarChar => write!(f, "VARCHAR"),
            PgType::Boolean => write!(f, "BOOLEAN"),
            PgType::Int => write!(f, "INT"),
            PgType::BigInt => write!(f, "BIGINT"),
            PgType::Float => write!(f, "FLOAT"),
            PgType::Double => write!(f, "DOUBLE"),
            PgType::Time => write!(f, "TIME"),
            PgType::Timestamp => write!(f, "TIMESTAMP"),
            PgType::Date => write!(f, "DATE"),
            _ => panic!("unsupported pgtype")
        }
    }
}

pub fn pl_to_pg_type(column: &Column) -> PgType {
    match *column.dtype() {
        polars::datatypes::DataType::String => PgType::Text,
        polars::datatypes::DataType::Boolean => PgType::Boolean,
        polars::datatypes::DataType::Int8 => PgType::Int,
        polars::datatypes::DataType::Int16 => PgType::Int,
        polars::datatypes::DataType::Int32 => PgType::Int,
        polars::datatypes::DataType::Int64 => PgType::BigInt,
        polars::datatypes::DataType::UInt8 => PgType::Int,
        polars::datatypes::DataType::UInt16 => PgType::Int,
        polars::datatypes::DataType::UInt32 => PgType::BigInt,
        polars::datatypes::DataType::Float32 => PgType::Float,
        polars::datatypes::DataType::Float64 => PgType::Double,
        _ => panic!("unsupported column type")
    }
}
