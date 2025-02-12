use crate::arguments::ClickHouseArgumentBuffer;
use crate::value::{ClickHouseValue, ClickHouseValueRef};
use crate::{
    ClickHouseArguments, ClickHouseColumn, ClickHouseConnection, ClickHouseQueryResult, ClickHouseRow, ClickHouseStatement, ClickHouseTransactionManager,
    ClickHouseTypeInfo,
};

pub(crate) use sqlx_core::database::{Database, HasStatementCache};

/// ClickHouse database driver.
#[derive(Debug)]
pub struct ClickHouse;

impl Database for ClickHouse {
    type Connection = ClickHouseConnection;

    type TransactionManager = ClickHouseTransactionManager;

    type Row = ClickHouseRow;

    type QueryResult = ClickHouseQueryResult;

    type Column = ClickHouseColumn;

    type TypeInfo = ClickHouseTypeInfo;

    type Value = ClickHouseValue;
    type ValueRef<'r> = ClickHouseValueRef<'r>;

    type Arguments<'q> = ClickHouseArguments;
    type ArgumentBuffer<'q> = ClickHouseArgumentBuffer;

    type Statement<'q> = ClickHouseStatement<'q>;

    const NAME: &'static str = "ClickHouse";

    const URL_SCHEMES: &'static [&'static str] = &["postgres", "postgresql"];
}

impl HasStatementCache for ClickHouse {}
