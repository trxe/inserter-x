use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::common::{
    ArField, CreateCmd, InsError, InsResult, PlArrowDtype, PlColumn, arrow_to_bytes,
    polars_to_arrow_datatype,
};

pub struct ClickhouseInserter {
    pub override_creation: Option<String>,
    pub table_name: String,
    pub db_name: Option<String>,
    pub fields: HashMap<String, Option<PlArrowDtype>>,
    pub engine: Option<String>,
    pub not_null: HashSet<String>,
    pub override_fields: HashMap<String, String>,
    pub order_by: Vec<String>,
    pub primary_key: Vec<String>,
    schema: Arc<arrow::datatypes::Schema>,
    cached_create_query: Option<String>,
    cached_insert_query: Option<String>,
}

fn polars_to_clickhouse_sql(pl: &PlArrowDtype) -> InsResult<String> {
    Ok(match pl {
        PlArrowDtype::Null => "Null".to_owned(),
        PlArrowDtype::Boolean => "Bool".to_owned(),
        PlArrowDtype::Int8 => "Int8".to_owned(),
        PlArrowDtype::Int16 => "Int16".to_owned(),
        PlArrowDtype::Int32 => "Int32".to_owned(),
        PlArrowDtype::Int64 => "Int64".to_owned(),
        PlArrowDtype::UInt8 => "UInt8".to_owned(),
        PlArrowDtype::UInt16 => "UInt16".to_owned(),
        PlArrowDtype::UInt32 => "UInt32".to_owned(),
        PlArrowDtype::UInt64 => "UInt64".to_owned(),
        PlArrowDtype::Decimal(p, s) => format!("Decimal({}, {})", p, s),
        PlArrowDtype::Decimal256(p, s) => format!("Decimal({}, {})", p, s),
        PlArrowDtype::Float32 => "Float32".to_owned(),
        PlArrowDtype::Float64 => "Float64".to_owned(),
        PlArrowDtype::Utf8 => "String".to_owned(),
        PlArrowDtype::Utf8View => "String".to_owned(),
        PlArrowDtype::Date32 => "Date32".to_owned(),
        PlArrowDtype::Date64 => "DateTime".to_owned(),
        PlArrowDtype::Timestamp(..) => "DateTime64".to_owned(),
        PlArrowDtype::Time32(_) => "Timestamp".to_owned(),
        PlArrowDtype::Time64(_) => "Timestamp".to_owned(),
        PlArrowDtype::List(ll) => {
            if ll.is_nullable && !ll.dtype().is_nested() {
                format!("Array(Nullable({}))", polars_to_clickhouse_sql(ll.dtype())?)
            } else {
                format!("Array({})", polars_to_clickhouse_sql(ll.dtype())?)
            }
        }
        PlArrowDtype::LargeList(ll) => {
            if ll.is_nullable && !ll.dtype().is_nested() {
                format!("Array(Nullable({}))", polars_to_clickhouse_sql(ll.dtype())?)
            } else {
                format!("Array({})", polars_to_clickhouse_sql(ll.dtype())?)
            }
        }
        PlArrowDtype::FixedSizeList(ll, _) => {
            if ll.is_nullable && !ll.dtype().is_nested() {
                format!("Array(Nullable({}))", polars_to_clickhouse_sql(ll.dtype())?)
            } else {
                format!("Array({})", polars_to_clickhouse_sql(ll.dtype())?)
            }
        }
        PlArrowDtype::Struct(ls) => {
            let mut new_types = vec![];
            for ll in ls {
                new_types.push(if ll.is_nullable && !ll.dtype().is_nested() {
                    format!("Nullable({})", polars_to_clickhouse_sql(ll.dtype())?)
                } else {
                    polars_to_clickhouse_sql(ll.dtype())?
                });
            }
            format!("Tuple({})", new_types.join(","))
        }
        x => {
            return Err(InsError::ConversionNotImplementedError(format!(
                "not a Clickhouse SQL DataType: {:?}",
                x
            )));
        }
    })
}

impl CreateCmd for ClickhouseInserter {}

impl ClickhouseInserter {
    pub fn default(table: &str) -> Self {
        Self {
            override_creation: None,
            table_name: table.to_owned(),
            db_name: None,
            fields: HashMap::new(),
            engine: None,
            order_by: vec![],
            primary_key: vec![],
            not_null: HashSet::new(),
            override_fields: HashMap::new(),
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            cached_create_query: None,
            cached_insert_query: None,
        }
    }

    pub fn with_order_by(mut self, subkeys: Vec<String>) -> Self {
        self.order_by.extend_from_slice(subkeys.as_slice());
        self.not_null.extend(subkeys);
        self
    }

    pub fn replace_order_by(mut self, subkeys: Vec<String>) -> Self {
        self.order_by.extend_from_slice(subkeys.as_slice());
        self.not_null.extend(subkeys);
        self
    }

    pub fn with_not_null(mut self, subkeys: Vec<String>) -> Self {
        self.not_null.extend(subkeys);
        self
    }

    pub fn replace_not_null(mut self, subkeys: Vec<String>) -> Self {
        self.not_null.clear();
        self.not_null.extend(subkeys);
        self
    }

    pub fn with_primary_key(mut self, subkeys: Vec<String>) -> Self {
        self.primary_key.extend_from_slice(subkeys.as_slice());
        self.not_null.extend(subkeys);
        self
    }

    pub fn replace_primary_key(mut self, subkeys: Vec<String>) -> Self {
        self.primary_key.extend_from_slice(subkeys.as_slice());
        self.not_null.extend(subkeys);
        self
    }

    pub fn with_field(mut self, column: &str, constraint: &str) -> Self {
        self.fields.insert(column.to_owned(), None);
        self.override_fields.insert(column.to_owned(), constraint.to_owned());
        self
    }

    pub fn with_dbname(mut self, db_name: &str) -> Self {
        let _ = self.db_name.insert(db_name.to_owned());
        self
    }

    pub fn with_engine(mut self, engine_name: &str) -> Self {
        let _ = self.engine.insert(engine_name.to_owned());
        self
    }

    pub fn with_create_method(mut self, override_creation: &str) -> Self {
        let _ = self.override_creation.insert(override_creation.to_owned());
        self
    }

    pub fn with_table_name(mut self, table_name: &str) -> Self {
        self.table_name = table_name.to_owned();
        self
    }

    pub fn build_queries(mut self) -> InsResult<Self> {
        let table_name = if let Some(x) = self.db_name.as_ref() {
            format!("{}.{}", x, self.table_name)
        } else {
            self.table_name.clone()
        };
        let engine = self
            .engine
            .as_ref()
            .map(|x| format!("Engine = {}", x))
            .unwrap_or_default();
        let order_by = if self.order_by.is_empty() {
            String::new()
        } else if self.order_by.len() == 1 {
            format!("ORDER BY {}", self.order_by.first().unwrap())
        } else {
            format!("ORDER BY ({})", self.order_by.join(", "))
        };
        let primary_key = if self.primary_key.is_empty() {
            String::new()
        } else if self.primary_key.len() == 1 {
            format!("PRIMARY KEY {}", self.primary_key.first().unwrap())
        } else {
            format!("PRIMARY KEY ({})", self.primary_key.join(", "))
        };
        let table_config = format!("{} {} {}", engine, order_by, primary_key)
            .trim()
            .to_owned();
        let mut fields = vec![];
        for (name, pladt)  in self.fields.iter() {
            let is_nested = pladt.as_ref().map(|f| f.is_nested()).unwrap_or(true);
            let typename = if let Some(pl) = &pladt {
                Some(polars_to_clickhouse_sql(pl)?)
            } else {
                None
            };
            fields.push(
                Self::field(name, 
                    !self.not_null.contains(name) && !is_nested, 
                    typename.as_deref(),
                    self.override_fields.get(name).map(|x| x.as_str())
                )
            );
        };
        self.cached_create_query = Some(Self::table(
            table_name.as_str(),
            fields.as_slice(),
            table_config.as_str(),
            self.override_creation.as_deref(),
        ));
        self.cached_insert_query = Some(Self::insert(table_name.as_str()));
        Ok(self)
    }

    pub fn get_create_query(&self) -> InsResult<&str> {
        match self.cached_create_query.as_deref() {
            Some(x) => Ok(x),
            None => Err(InsError::BuildError(
                "clickhouse create_query",
                "not yet built, first run self.build_create_query".to_owned(),
            )),
        }
    }

    pub fn with_schema_from_cols(mut self, columns: &[PlColumn]) -> InsResult<Self> {
        let mut schema_builder = arrow::datatypes::SchemaBuilder::new();
        for column in columns {
            let pladt = column
                .dtype()
                .to_arrow(polars::prelude::CompatLevel::newest());
            let adt = polars_to_arrow_datatype(&pladt)?;
            let afield = ArField::new(
                column.name().as_str(),
                adt,
                !self.not_null.contains(column.name().as_str()),
            );
            self.fields.insert(column.name().to_string(), Some(pladt.to_owned()));
            schema_builder.push(afield);
        }
        self.schema = Arc::new(schema_builder.finish());
        Ok(self)
    }

    pub fn get_insert_query(&self) -> InsResult<&str> {
        match self.cached_insert_query.as_deref() {
            Some(x) => Ok(x),
            None => Err(InsError::BuildError(
                "clickhouse insert_query",
                "not yet built, first run self.build_queries".to_owned(),
            )),
        }
    }

    pub fn get_arrow_body(&self, frame: &polars::prelude::DataFrame) -> InsResult<Vec<u8>> {
        let schema = self.schema.clone();
        arrow_to_bytes(schema, frame)
    }
}
