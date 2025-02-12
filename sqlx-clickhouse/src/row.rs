use crate::column::ColumnIndex;
use crate::error::Error;
use crate::message::DataRow;
use crate::statement::ClickHouseStatementMetadata;
use crate::value::ClickHouseValueFormat;
use crate::{ClickHouseColumn, ClickHouseValueRef, ClickHouse};
pub(crate) use sqlx_core::row::Row;
use sqlx_core::type_checking::TypeChecking;
use sqlx_core::value::ValueRef;
use std::fmt::Debug;
use std::sync::Arc;

/// Implementation of [`Row`] for ClickHouse.
pub struct ClickHouseRow {
    pub(crate) data: DataRow,
    pub(crate) format: ClickHouseValueFormat,
    pub(crate) metadata: Arc<ClickHouseStatementMetadata>,
}

impl Row for ClickHouseRow {
    type Database = ClickHouse;

    fn columns(&self) -> &[ClickHouseColumn] {
        &self.metadata.columns
    }

    fn try_get_raw<I>(&self, index: I) -> Result<ClickHouseValueRef<'_>, Error>
    where
        I: ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        let column = &self.metadata.columns[index];
        let value = self.data.get(index);

        Ok(ClickHouseValueRef {
            format: self.format,
            row: Some(&self.data.storage),
            type_info: column.type_info.clone(),
            value,
        })
    }
}

impl ColumnIndex<ClickHouseRow> for &'_ str {
    fn index(&self, row: &ClickHouseRow) -> Result<usize, Error> {
        row.metadata
            .column_names
            .get(*self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
            .copied()
    }
}

impl Debug for ClickHouseRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClickHouseRow ")?;

        let mut debug_map = f.debug_map();
        for (index, column) in self.columns().iter().enumerate() {
            match self.try_get_raw(index) {
                Ok(value) => {
                    debug_map.entry(
                        &column.name,
                        &ClickHouse::fmt_value_debug(&<ClickHouseValueRef as ValueRef>::to_owned(&value)),
                    );
                }
                Err(error) => {
                    debug_map.entry(&column.name, &format!("decode error: {error:?}"));
                }
            }
        }

        debug_map.finish()
    }
}
