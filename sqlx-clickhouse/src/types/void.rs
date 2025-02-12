use crate::decode::Decode;
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseTypeInfo, ClickHouseValueRef, ClickHouse};

impl Type<ClickHouse> for () {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::VOID
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        // RECORD is here so we can support the empty tuple
        *ty == ClickHouseTypeInfo::VOID || *ty == ClickHouseTypeInfo::RECORD
    }
}

impl<'r> Decode<'r, ClickHouse> for () {
    fn decode(_value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(())
    }
}
