use crate::types::array_compatible;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueRef, ClickHouse};
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;

/// Case-insensitive text (`citext`) support for ClickHouse.
///
/// Note that SQLx considers the `citext` type to be compatible with `String`
/// and its various derivatives, so direct usage of this type is generally unnecessary.
///
/// However, it may be needed, for example, when binding a `citext[]` array,
/// as ClickHouse will generally not accept a `text[]` array (mapped from `Vec<String>`) in its place.
///
/// See [the ClickHouse manual, Appendix F, Section 10][PG.F.10] for details on using `citext`.
///
/// [PG.F.10]: https://www.postgresql.org/docs/current/citext.html
///
/// ### Note: Extension Required
/// The `citext` extension is not enabled by default in ClickHouse. You will need to do so explicitly:
///
/// ```ignore
/// CREATE EXTENSION IF NOT EXISTS "citext";
/// ```
///
/// ### Note: `PartialEq` is Case-Sensitive
/// This type derives `PartialEq` which forwards to the implementation on `String`, which
/// is case-sensitive. This impl exists mainly for testing.
///
/// To properly emulate the case-insensitivity of `citext` would require use of locale-aware
/// functions in `libc`, and even then would require querying the locale of the database server
/// and setting it locally, which is unsafe.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClickHouseCiText(pub String);

impl Type<ClickHouse> for ClickHouseCiText {
    fn type_info() -> ClickHouseTypeInfo {
        // Since `citext` is enabled by an extension, it does not have a stable OID.
        ClickHouseTypeInfo::with_name("citext")
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        <&str as Type<ClickHouse>>::compatible(ty)
    }
}

impl Deref for ClickHouseCiText {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl From<String> for ClickHouseCiText {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<ClickHouseCiText> for String {
    fn from(value: ClickHouseCiText) -> Self {
        value.0
    }
}

impl FromStr for ClickHouseCiText {
    type Err = core::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ClickHouseCiText(s.parse()?))
    }
}

impl Display for ClickHouseCiText {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl ClickHouseHasArrayType for ClickHouseCiText {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::with_name("_citext")
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        array_compatible::<&str>(ty)
    }
}

impl Encode<'_, ClickHouse> for ClickHouseCiText {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&str as Encode<ClickHouse>>::encode(&**self, buf)
    }
}

impl Decode<'_, ClickHouse> for ClickHouseCiText {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(ClickHouseCiText(value.as_str()?.to_owned()))
    }
}
