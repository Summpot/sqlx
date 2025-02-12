use std::iter::{Extend, IntoIterator};

#[derive(Debug, Default)]
pub struct ClickHouseQueryResult {
    pub(super) rows_affected: u64,
}

impl ClickHouseQueryResult {
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

impl Extend<ClickHouseQueryResult> for ClickHouseQueryResult {
    fn extend<T: IntoIterator<Item = ClickHouseQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.rows_affected += elem.rows_affected;
        }
    }
}

#[cfg(feature = "any")]
impl From<ClickHouseQueryResult> for sqlx_core::any::AnyQueryResult {
    fn from(done: ClickHouseQueryResult) -> Self {
        sqlx_core::any::AnyQueryResult {
            rows_affected: done.rows_affected,
            last_insert_id: None,
        }
    }
}
