use futures_core::future::BoxFuture;

use crate::error::Error;
use crate::executor::Executor;

use crate::{ClickHouseConnection, ClickHouse};

pub(crate) use sqlx_core::transaction::*;

/// Implementation of [`TransactionManager`] for ClickHouse.
pub struct ClickHouseTransactionManager;

impl TransactionManager for ClickHouseTransactionManager {
    type Database = ClickHouse;

    fn begin(conn: &mut ClickHouseConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let rollback = Rollback::new(conn);
            let query = begin_ansi_transaction_sql(rollback.conn.inner.transaction_depth);
            rollback.conn.queue_simple_query(&query)?;
            rollback.conn.inner.transaction_depth += 1;
            rollback.conn.wait_until_ready().await?;
            rollback.defuse();

            Ok(())
        })
    }

    fn commit(conn: &mut ClickHouseConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            if conn.inner.transaction_depth > 0 {
                conn.execute(&*commit_ansi_transaction_sql(conn.inner.transaction_depth))
                    .await?;

                conn.inner.transaction_depth -= 1;
            }

            Ok(())
        })
    }

    fn rollback(conn: &mut ClickHouseConnection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            if conn.inner.transaction_depth > 0 {
                conn.execute(&*rollback_ansi_transaction_sql(
                    conn.inner.transaction_depth,
                ))
                .await?;

                conn.inner.transaction_depth -= 1;
            }

            Ok(())
        })
    }

    fn start_rollback(conn: &mut ClickHouseConnection) {
        if conn.inner.transaction_depth > 0 {
            conn.queue_simple_query(&rollback_ansi_transaction_sql(conn.inner.transaction_depth))
                .expect("BUG: Rollback query somehow too large for protocol");

            conn.inner.transaction_depth -= 1;
        }
    }
}

struct Rollback<'c> {
    conn: &'c mut ClickHouseConnection,
    defuse: bool,
}

impl Drop for Rollback<'_> {
    fn drop(&mut self) {
        if !self.defuse {
            ClickHouseTransactionManager::start_rollback(self.conn)
        }
    }
}

impl<'c> Rollback<'c> {
    fn new(conn: &'c mut ClickHouseConnection) -> Self {
        Self {
            conn,
            defuse: false,
        }
    }
    fn defuse(mut self) {
        self.defuse = true;
    }
}
