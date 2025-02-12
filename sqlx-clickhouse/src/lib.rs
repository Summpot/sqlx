//! **ClickHouse** database driver.

#[macro_use]
extern crate sqlx_core;

use crate::executor::Executor;

mod advisory_lock;
mod arguments;
mod column;
mod connection;
mod copy;
mod database;
mod error;
mod io;
mod listener;
mod message;
mod options;
mod query_result;
mod row;
mod statement;
mod transaction;
mod type_checking;
mod type_info;
pub mod types;
mod value;

#[cfg(feature = "any")]
// We are hiding the any module with its AnyConnectionBackend trait
// so that IDEs don't show it in the autocompletion list
// and end users don't accidentally use it. This can result in
// nested transactions not behaving as expected.
// For more information, see https://github.com/launchbadge/sqlx/pull/3254#issuecomment-2144043823
#[doc(hidden)]
pub mod any;

#[doc(hidden)]
pub use copy::PG_COPY_MAX_DATA_LEN;

#[cfg(feature = "migrate")]
mod migrate;

#[cfg(feature = "migrate")]
mod testing;

pub(crate) use sqlx_core::driver_prelude::*;

pub use advisory_lock::{ClickHouseAdvisoryLock, ClickHouseAdvisoryLockGuard, ClickHouseAdvisoryLockKey};
pub use arguments::{ClickHouseArgumentBuffer, ClickHouseArguments};
pub use column::ClickHouseColumn;
pub use connection::ClickHouseConnection;
pub use copy::{ClickHouseCopyIn, ClickHousePoolCopyExt};
pub use database::ClickHouse;
pub use error::{ClickHouseDatabaseError, ClickHouseErrorPosition};
pub use listener::{ClickHouseListener, ClickHouseNotification};
pub use message::ClickHouseSeverity;
pub use options::{ClickHouseConnectOptions, ClickHouseSslMode};
pub use query_result::ClickHouseQueryResult;
pub use row::ClickHouseRow;
pub use statement::ClickHouseStatement;
pub use transaction::ClickHouseTransactionManager;
pub use type_info::{ClickHouseTypeInfo, ClickHouseTypeKind};
pub use types::ClickHouseHasArrayType;
pub use value::{ClickHouseValue, ClickHouseValueFormat, ClickHouseValueRef};

/// An alias for [`Pool`][crate::pool::Pool], specialized for ClickHouse.
pub type ClickHousePool = crate::pool::Pool<ClickHouse>;

/// An alias for [`PoolOptions`][crate::pool::PoolOptions], specialized for ClickHouse.
pub type ClickHousePoolOptions = crate::pool::PoolOptions<ClickHouse>;

/// An alias for [`Executor<'_, Database = ClickHouse>`][Executor].
pub trait ClickHouseExecutor<'c>: Executor<'c, Database = ClickHouse> {}
impl<'c, T: Executor<'c, Database = ClickHouse>> ClickHouseExecutor<'c> for T {}

/// An alias for [`Transaction`][crate::transaction::Transaction], specialized for ClickHouse.
pub type ClickHouseTransaction<'c> = crate::transaction::Transaction<'c, ClickHouse>;

impl_into_arguments_for_arguments!(ClickHouseArguments);
impl_acquire!(ClickHouse, ClickHouseConnection);
impl_column_index_for_row!(ClickHouseRow);
impl_column_index_for_statement!(ClickHouseStatement);
impl_encode_for_option!(ClickHouse);
