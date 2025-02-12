use std::net::IpAddr;

use ipnetwork::IpNetwork;

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueRef, ClickHouse};

impl Type<ClickHouse> for IpAddr
where
    IpNetwork: Type<ClickHouse>,
{
    fn type_info() -> ClickHouseTypeInfo {
        IpNetwork::type_info()
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        IpNetwork::compatible(ty)
    }
}

impl ClickHouseHasArrayType for IpAddr {
    fn array_type_info() -> ClickHouseTypeInfo {
        <IpNetwork as ClickHouseHasArrayType>::array_type_info()
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        <IpNetwork as ClickHouseHasArrayType>::array_compatible(ty)
    }
}

impl<'db> Encode<'db, ClickHouse> for IpAddr
where
    IpNetwork: Encode<'db, ClickHouse>,
{
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        IpNetwork::from(*self).encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        IpNetwork::from(*self).size_hint()
    }
}

impl<'db> Decode<'db, ClickHouse> for IpAddr
where
    IpNetwork: Decode<'db, ClickHouse>,
{
    fn decode(value: ClickHouseValueRef<'db>) -> Result<Self, BoxDynError> {
        let ipnetwork = IpNetwork::decode(value)?;

        if ipnetwork.is_ipv4() && ipnetwork.prefix() != 32
            || ipnetwork.is_ipv6() && ipnetwork.prefix() != 128
        {
            Err("lossy decode from inet/cidr")?
        }

        Ok(ipnetwork.ip())
    }
}
