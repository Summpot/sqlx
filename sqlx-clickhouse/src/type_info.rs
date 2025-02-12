#![allow(dead_code)]

use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use crate::ext::ustr::UStr;
use crate::types::Oid;

pub(crate) use sqlx_core::type_info::TypeInfo;

/// Type information for a ClickHouse type.
///
/// ### Note: Implementation of `==` ([`PartialEq::eq()`])
/// Because `==` on [`TypeInfo`]s has been used throughout the SQLx API as a synonym for type compatibility,
/// e.g. in the default impl of [`Type::compatible()`][sqlx_core::types::Type::compatible],
/// some concessions have been made in the implementation.
///
/// When comparing two `ClickHouseTypeInfo`s using the `==` operator ([`PartialEq::eq()`]),
/// if one was constructed with [`Self::with_oid()`] and the other with [`Self::with_name()`] or
/// [`Self::array_of()`], `==` will return `true`:
///
/// ```
/// # use sqlx::postgres::{types::Oid, ClickHouseTypeInfo};
/// // Potentially surprising result, this assert will pass:
/// assert_eq!(ClickHouseTypeInfo::with_oid(Oid(1)), ClickHouseTypeInfo::with_name("definitely_not_real"));
/// ```
///
/// Since it is not possible in this case to prove the types are _not_ compatible (because
/// both `ClickHouseTypeInfo`s need to be resolved by an active connection to know for sure)
/// and type compatibility is mainly done as a sanity check anyway,
/// it was deemed acceptable to fudge equality in this very specific case.
///
/// This also applies when querying with the text protocol (not using prepared statements,
/// e.g. [`sqlx::raw_sql()`][sqlx_core::raw_sql::raw_sql]), as the connection will be unable
/// to look up the type info like it normally does when preparing a statement: it won't know
/// what the OIDs of the output columns will be until it's in the middle of reading the result,
/// and by that time it's too late.
///
/// To compare types for exact equality, use [`Self::type_eq()`] instead.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub struct ClickHouseTypeInfo(pub(crate) ClickHouseType);

impl Deref for ClickHouseTypeInfo {
    type Target = ClickHouseType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
#[repr(u32)]
pub enum ClickHouseType {
    Bool,
    Bytea,
    Char,
    Name,
    Int8,
    Int2,
    Int4,
    Text,
    Oid,
    Json,
    JsonArray,
    Point,
    Lseg,
    Path,
    Box,
    Polygon,
    Line,
    LineArray,
    Cidr,
    CidrArray,
    Float4,
    Float8,
    Unknown,
    Circle,
    CircleArray,
    Macaddr8,
    Macaddr8Array,
    Macaddr,
    Inet,
    BoolArray,
    ByteaArray,
    CharArray,
    NameArray,
    Int2Array,
    Int4Array,
    TextArray,
    BpcharArray,
    VarcharArray,
    Int8Array,
    PointArray,
    LsegArray,
    PathArray,
    BoxArray,
    Float4Array,
    Float8Array,
    PolygonArray,
    OidArray,
    MacaddrArray,
    InetArray,
    Bpchar,
    Varchar,
    Date,
    Time,
    Timestamp,
    TimestampArray,
    DateArray,
    TimeArray,
    Timestamptz,
    TimestamptzArray,
    Interval,
    IntervalArray,
    NumericArray,
    Timetz,
    TimetzArray,
    Bit,
    BitArray,
    Varbit,
    VarbitArray,
    Numeric,
    Record,
    RecordArray,
    Uuid,
    UuidArray,
    Jsonb,
    JsonbArray,
    Int4Range,
    Int4RangeArray,
    NumRange,
    NumRangeArray,
    TsRange,
    TsRangeArray,
    TstzRange,
    TstzRangeArray,
    DateRange,
    DateRangeArray,
    Int8Range,
    Int8RangeArray,
    Jsonpath,
    JsonpathArray,
    Money,
    MoneyArray,

    // https://www.postgresql.org/docs/9.3/datatype-pseudo.html
    Void,

    // A realized user-defined type. When a connection sees a DeclareXX variant it resolves
    // into this one before passing it along to `accepts` or inside of `Value` objects.
    Custom(Arc<ClickHouseCustomType>),

    // From [`ClickHouseTypeInfo::with_name`]
    DeclareWithName(UStr),

    // NOTE: Do we want to bring back type declaration by ID? It's notoriously fragile but
    //       someone may have a user for it
    DeclareWithOid(Oid),

    DeclareArrayOf(Arc<ClickHouseArrayOf>),
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub struct ClickHouseCustomType {
    #[cfg_attr(feature = "offline", serde(skip))]
    pub(crate) oid: Oid,
    pub(crate) name: UStr,
    pub(crate) kind: ClickHouseTypeKind,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub enum ClickHouseTypeKind {
    Simple,
    Pseudo,
    Domain(ClickHouseTypeInfo),
    Composite(Arc<[(String, ClickHouseTypeInfo)]>),
    Array(ClickHouseTypeInfo),
    Enum(Arc<[String]>),
    Range(ClickHouseTypeInfo),
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub struct ClickHouseArrayOf {
    pub(crate) elem_name: UStr,
    pub(crate) name: Box<str>,
}

impl ClickHouseTypeInfo {
    /// Returns the corresponding `ClickHouseTypeInfo` if the OID is a built-in type and recognized by SQLx.
    pub(crate) fn try_from_oid(oid: Oid) -> Option<Self> {
        ClickHouseType::try_from_oid(oid).map(Self)
    }

    /// Returns the _kind_ (simple, array, enum, etc.) for this type.
    pub fn kind(&self) -> &ClickHouseTypeKind {
        self.0.kind()
    }

    /// Returns the OID for this type, if available.
    ///
    /// The OID may not be available if SQLx only knows the type by name.
    /// It will have to be resolved by a `ClickHouseConnection` at runtime which
    /// will yield a new and semantically distinct `TypeInfo` instance.
    ///
    /// This method does not perform any such lookup.
    ///
    /// ### Note
    /// With the exception of [the default `pg_type` catalog][pg_type], type OIDs are *not* stable in ClickHouse.
    /// If a type is added by an extension, its OID will be assigned when the `CREATE EXTENSION` statement is executed,
    /// and so can change depending on what extensions are installed and in what order, as well as the exact
    /// version of ClickHouse.
    ///
    /// [pg_type]: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
    pub fn oid(&self) -> Option<Oid> {
        self.0.try_oid()
    }

    #[doc(hidden)]
    pub fn __type_feature_gate(&self) -> Option<&'static str> {
        if [
            ClickHouseTypeInfo::DATE,
            ClickHouseTypeInfo::TIME,
            ClickHouseTypeInfo::TIMESTAMP,
            ClickHouseTypeInfo::TIMESTAMPTZ,
            ClickHouseTypeInfo::DATE_ARRAY,
            ClickHouseTypeInfo::TIME_ARRAY,
            ClickHouseTypeInfo::TIMESTAMP_ARRAY,
            ClickHouseTypeInfo::TIMESTAMPTZ_ARRAY,
        ]
        .contains(self)
        {
            Some("time")
        } else if [ClickHouseTypeInfo::UUID, ClickHouseTypeInfo::UUID_ARRAY].contains(self) {
            Some("uuid")
        } else if [
            ClickHouseTypeInfo::JSON,
            ClickHouseTypeInfo::JSONB,
            ClickHouseTypeInfo::JSON_ARRAY,
            ClickHouseTypeInfo::JSONB_ARRAY,
        ]
        .contains(self)
        {
            Some("json")
        } else if [
            ClickHouseTypeInfo::CIDR,
            ClickHouseTypeInfo::INET,
            ClickHouseTypeInfo::CIDR_ARRAY,
            ClickHouseTypeInfo::INET_ARRAY,
        ]
        .contains(self)
        {
            Some("ipnetwork")
        } else if [ClickHouseTypeInfo::MACADDR].contains(self) {
            Some("mac_address")
        } else if [ClickHouseTypeInfo::NUMERIC, ClickHouseTypeInfo::NUMERIC_ARRAY].contains(self) {
            Some("bigdecimal")
        } else {
            None
        }
    }

    /// Create a `ClickHouseTypeInfo` from a type name.
    ///
    /// The OID for the type will be fetched from ClickHouse on use of
    /// a value of this type. The fetched OID will be cached per-connection.
    ///
    /// ### Note: Type Names Prefixed with `_`
    /// In `pg_catalog.pg_type`, ClickHouse prefixes a type name with `_` to denote an array of that
    /// type, e.g. `int4[]` actually exists in `pg_type` as `_int4`.
    ///
    /// Previously, it was necessary in manual [`ClickHouseHasArrayType`][crate::ClickHouseHasArrayType] impls
    /// to return [`ClickHouseTypeInfo::with_name()`] with the type name prefixed with `_` to denote
    /// an array type, but this would not work with schema-qualified names.
    ///
    /// As of 0.8, [`ClickHouseTypeInfo::array_of()`] is used to declare an array type,
    /// and the ClickHouse driver is now able to properly resolve arrays of custom types,
    /// even in other schemas, which was not previously supported.
    ///
    /// It is highly recommended to migrate existing usages to [`ClickHouseTypeInfo::array_of()`] where
    /// applicable.
    ///
    /// However, to maintain compatibility, the driver now infers any type name prefixed with `_`
    /// to be an array of that type. This may introduce some breakages for types which use
    /// a `_` prefix but which are not arrays.
    ///
    /// As a workaround, type names with `_` as a prefix but which are not arrays should be wrapped
    /// in quotes, e.g.:
    /// ```
    /// use sqlx::postgres::ClickHouseTypeInfo;
    /// use sqlx::{Type, TypeInfo};
    ///
    /// /// `CREATE TYPE "_foo" AS ENUM ('Bar', 'Baz');`
    /// #[derive(sqlx::Type)]
    /// // Will prevent SQLx from inferring `_foo` as an array type.
    /// #[sqlx(type_name = r#""_foo""#)]
    /// enum Foo {
    ///     Bar,
    ///     Baz
    /// }
    ///
    /// assert_eq!(Foo::type_info().name(), r#""_foo""#);
    /// ```
    pub const fn with_name(name: &'static str) -> Self {
        Self(ClickHouseType::DeclareWithName(UStr::Static(name)))
    }

    /// Create a `ClickHouseTypeInfo` of an array from the name of its element type.
    ///
    /// The array type OID will be fetched from ClickHouse on use of a value of this type.
    /// The fetched OID will be cached per-connection.
    pub fn array_of(elem_name: &'static str) -> Self {
        // to satisfy `name()` and `display_name()`, we need to construct strings to return
        Self(ClickHouseType::DeclareArrayOf(Arc::new(ClickHouseArrayOf {
            elem_name: elem_name.into(),
            name: format!("{elem_name}[]").into(),
        })))
    }

    /// Create a `ClickHouseTypeInfo` from an OID.
    ///
    /// Note that the OID for a type is very dependent on the environment. If you only ever use
    /// one database or if this is an unhandled built-in type, you should be fine. Otherwise,
    /// you will be better served using [`Self::with_name()`].
    ///
    /// ### Note: Interaction with `==`
    /// This constructor may give surprising results with `==`.
    ///
    /// See [the type-level docs][Self] for details.
    pub const fn with_oid(oid: Oid) -> Self {
        Self(ClickHouseType::DeclareWithOid(oid))
    }

    /// Returns `true` if `self` can be compared exactly to `other`.
    ///
    /// Unlike `==`, this will return false if
    pub fn type_eq(&self, other: &Self) -> bool {
        self.eq_impl(other, false)
    }
}

// DEVELOPER PRO TIP: find builtin type OIDs easily by grepping this file
// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
//
// If you have ClickHouse running locally you can also try
// SELECT oid, typarray FROM pg_type where typname = '<type name>'

impl ClickHouseType {
    /// Returns the corresponding `ClickHouseType` if the OID is a built-in type and recognized by SQLx.
    pub(crate) fn try_from_oid(oid: Oid) -> Option<Self> {
        Some(match oid.0 {
            16 => ClickHouseType::Bool,
            17 => ClickHouseType::Bytea,
            18 => ClickHouseType::Char,
            19 => ClickHouseType::Name,
            20 => ClickHouseType::Int8,
            21 => ClickHouseType::Int2,
            23 => ClickHouseType::Int4,
            25 => ClickHouseType::Text,
            26 => ClickHouseType::Oid,
            114 => ClickHouseType::Json,
            199 => ClickHouseType::JsonArray,
            600 => ClickHouseType::Point,
            601 => ClickHouseType::Lseg,
            602 => ClickHouseType::Path,
            603 => ClickHouseType::Box,
            604 => ClickHouseType::Polygon,
            628 => ClickHouseType::Line,
            629 => ClickHouseType::LineArray,
            650 => ClickHouseType::Cidr,
            651 => ClickHouseType::CidrArray,
            700 => ClickHouseType::Float4,
            701 => ClickHouseType::Float8,
            705 => ClickHouseType::Unknown,
            718 => ClickHouseType::Circle,
            719 => ClickHouseType::CircleArray,
            774 => ClickHouseType::Macaddr8,
            775 => ClickHouseType::Macaddr8Array,
            790 => ClickHouseType::Money,
            791 => ClickHouseType::MoneyArray,
            829 => ClickHouseType::Macaddr,
            869 => ClickHouseType::Inet,
            1000 => ClickHouseType::BoolArray,
            1001 => ClickHouseType::ByteaArray,
            1002 => ClickHouseType::CharArray,
            1003 => ClickHouseType::NameArray,
            1005 => ClickHouseType::Int2Array,
            1007 => ClickHouseType::Int4Array,
            1009 => ClickHouseType::TextArray,
            1014 => ClickHouseType::BpcharArray,
            1015 => ClickHouseType::VarcharArray,
            1016 => ClickHouseType::Int8Array,
            1017 => ClickHouseType::PointArray,
            1018 => ClickHouseType::LsegArray,
            1019 => ClickHouseType::PathArray,
            1020 => ClickHouseType::BoxArray,
            1021 => ClickHouseType::Float4Array,
            1022 => ClickHouseType::Float8Array,
            1027 => ClickHouseType::PolygonArray,
            1028 => ClickHouseType::OidArray,
            1040 => ClickHouseType::MacaddrArray,
            1041 => ClickHouseType::InetArray,
            1042 => ClickHouseType::Bpchar,
            1043 => ClickHouseType::Varchar,
            1082 => ClickHouseType::Date,
            1083 => ClickHouseType::Time,
            1114 => ClickHouseType::Timestamp,
            1115 => ClickHouseType::TimestampArray,
            1182 => ClickHouseType::DateArray,
            1183 => ClickHouseType::TimeArray,
            1184 => ClickHouseType::Timestamptz,
            1185 => ClickHouseType::TimestamptzArray,
            1186 => ClickHouseType::Interval,
            1187 => ClickHouseType::IntervalArray,
            1231 => ClickHouseType::NumericArray,
            1266 => ClickHouseType::Timetz,
            1270 => ClickHouseType::TimetzArray,
            1560 => ClickHouseType::Bit,
            1561 => ClickHouseType::BitArray,
            1562 => ClickHouseType::Varbit,
            1563 => ClickHouseType::VarbitArray,
            1700 => ClickHouseType::Numeric,
            2278 => ClickHouseType::Void,
            2249 => ClickHouseType::Record,
            2287 => ClickHouseType::RecordArray,
            2950 => ClickHouseType::Uuid,
            2951 => ClickHouseType::UuidArray,
            3802 => ClickHouseType::Jsonb,
            3807 => ClickHouseType::JsonbArray,
            3904 => ClickHouseType::Int4Range,
            3905 => ClickHouseType::Int4RangeArray,
            3906 => ClickHouseType::NumRange,
            3907 => ClickHouseType::NumRangeArray,
            3908 => ClickHouseType::TsRange,
            3909 => ClickHouseType::TsRangeArray,
            3910 => ClickHouseType::TstzRange,
            3911 => ClickHouseType::TstzRangeArray,
            3912 => ClickHouseType::DateRange,
            3913 => ClickHouseType::DateRangeArray,
            3926 => ClickHouseType::Int8Range,
            3927 => ClickHouseType::Int8RangeArray,
            4072 => ClickHouseType::Jsonpath,
            4073 => ClickHouseType::JsonpathArray,

            _ => {
                return None;
            }
        })
    }

    pub(crate) fn oid(&self) -> Oid {
        match self.try_oid() {
            Some(oid) => oid,
            None => unreachable!("(bug) use of unresolved type declaration [oid]"),
        }
    }

    pub(crate) fn try_oid(&self) -> Option<Oid> {
        Some(match self {
            ClickHouseType::Bool => Oid(16),
            ClickHouseType::Bytea => Oid(17),
            ClickHouseType::Char => Oid(18),
            ClickHouseType::Name => Oid(19),
            ClickHouseType::Int8 => Oid(20),
            ClickHouseType::Int2 => Oid(21),
            ClickHouseType::Int4 => Oid(23),
            ClickHouseType::Text => Oid(25),
            ClickHouseType::Oid => Oid(26),
            ClickHouseType::Json => Oid(114),
            ClickHouseType::JsonArray => Oid(199),
            ClickHouseType::Point => Oid(600),
            ClickHouseType::Lseg => Oid(601),
            ClickHouseType::Path => Oid(602),
            ClickHouseType::Box => Oid(603),
            ClickHouseType::Polygon => Oid(604),
            ClickHouseType::Line => Oid(628),
            ClickHouseType::LineArray => Oid(629),
            ClickHouseType::Cidr => Oid(650),
            ClickHouseType::CidrArray => Oid(651),
            ClickHouseType::Float4 => Oid(700),
            ClickHouseType::Float8 => Oid(701),
            ClickHouseType::Unknown => Oid(705),
            ClickHouseType::Circle => Oid(718),
            ClickHouseType::CircleArray => Oid(719),
            ClickHouseType::Macaddr8 => Oid(774),
            ClickHouseType::Macaddr8Array => Oid(775),
            ClickHouseType::Money => Oid(790),
            ClickHouseType::MoneyArray => Oid(791),
            ClickHouseType::Macaddr => Oid(829),
            ClickHouseType::Inet => Oid(869),
            ClickHouseType::BoolArray => Oid(1000),
            ClickHouseType::ByteaArray => Oid(1001),
            ClickHouseType::CharArray => Oid(1002),
            ClickHouseType::NameArray => Oid(1003),
            ClickHouseType::Int2Array => Oid(1005),
            ClickHouseType::Int4Array => Oid(1007),
            ClickHouseType::TextArray => Oid(1009),
            ClickHouseType::BpcharArray => Oid(1014),
            ClickHouseType::VarcharArray => Oid(1015),
            ClickHouseType::Int8Array => Oid(1016),
            ClickHouseType::PointArray => Oid(1017),
            ClickHouseType::LsegArray => Oid(1018),
            ClickHouseType::PathArray => Oid(1019),
            ClickHouseType::BoxArray => Oid(1020),
            ClickHouseType::Float4Array => Oid(1021),
            ClickHouseType::Float8Array => Oid(1022),
            ClickHouseType::PolygonArray => Oid(1027),
            ClickHouseType::OidArray => Oid(1028),
            ClickHouseType::MacaddrArray => Oid(1040),
            ClickHouseType::InetArray => Oid(1041),
            ClickHouseType::Bpchar => Oid(1042),
            ClickHouseType::Varchar => Oid(1043),
            ClickHouseType::Date => Oid(1082),
            ClickHouseType::Time => Oid(1083),
            ClickHouseType::Timestamp => Oid(1114),
            ClickHouseType::TimestampArray => Oid(1115),
            ClickHouseType::DateArray => Oid(1182),
            ClickHouseType::TimeArray => Oid(1183),
            ClickHouseType::Timestamptz => Oid(1184),
            ClickHouseType::TimestamptzArray => Oid(1185),
            ClickHouseType::Interval => Oid(1186),
            ClickHouseType::IntervalArray => Oid(1187),
            ClickHouseType::NumericArray => Oid(1231),
            ClickHouseType::Timetz => Oid(1266),
            ClickHouseType::TimetzArray => Oid(1270),
            ClickHouseType::Bit => Oid(1560),
            ClickHouseType::BitArray => Oid(1561),
            ClickHouseType::Varbit => Oid(1562),
            ClickHouseType::VarbitArray => Oid(1563),
            ClickHouseType::Numeric => Oid(1700),
            ClickHouseType::Void => Oid(2278),
            ClickHouseType::Record => Oid(2249),
            ClickHouseType::RecordArray => Oid(2287),
            ClickHouseType::Uuid => Oid(2950),
            ClickHouseType::UuidArray => Oid(2951),
            ClickHouseType::Jsonb => Oid(3802),
            ClickHouseType::JsonbArray => Oid(3807),
            ClickHouseType::Int4Range => Oid(3904),
            ClickHouseType::Int4RangeArray => Oid(3905),
            ClickHouseType::NumRange => Oid(3906),
            ClickHouseType::NumRangeArray => Oid(3907),
            ClickHouseType::TsRange => Oid(3908),
            ClickHouseType::TsRangeArray => Oid(3909),
            ClickHouseType::TstzRange => Oid(3910),
            ClickHouseType::TstzRangeArray => Oid(3911),
            ClickHouseType::DateRange => Oid(3912),
            ClickHouseType::DateRangeArray => Oid(3913),
            ClickHouseType::Int8Range => Oid(3926),
            ClickHouseType::Int8RangeArray => Oid(3927),
            ClickHouseType::Jsonpath => Oid(4072),
            ClickHouseType::JsonpathArray => Oid(4073),

            ClickHouseType::Custom(ty) => ty.oid,

            ClickHouseType::DeclareWithOid(oid) => *oid,
            ClickHouseType::DeclareWithName(_) => {
                return None;
            }
            ClickHouseType::DeclareArrayOf(_) => {
                return None;
            }
        })
    }

    pub(crate) fn display_name(&self) -> &str {
        match self {
            ClickHouseType::Bool => "BOOL",
            ClickHouseType::Bytea => "BYTEA",
            ClickHouseType::Char => "\"CHAR\"",
            ClickHouseType::Name => "NAME",
            ClickHouseType::Int8 => "INT8",
            ClickHouseType::Int2 => "INT2",
            ClickHouseType::Int4 => "INT4",
            ClickHouseType::Text => "TEXT",
            ClickHouseType::Oid => "OID",
            ClickHouseType::Json => "JSON",
            ClickHouseType::JsonArray => "JSON[]",
            ClickHouseType::Point => "POINT",
            ClickHouseType::Lseg => "LSEG",
            ClickHouseType::Path => "PATH",
            ClickHouseType::Box => "BOX",
            ClickHouseType::Polygon => "POLYGON",
            ClickHouseType::Line => "LINE",
            ClickHouseType::LineArray => "LINE[]",
            ClickHouseType::Cidr => "CIDR",
            ClickHouseType::CidrArray => "CIDR[]",
            ClickHouseType::Float4 => "FLOAT4",
            ClickHouseType::Float8 => "FLOAT8",
            ClickHouseType::Unknown => "UNKNOWN",
            ClickHouseType::Circle => "CIRCLE",
            ClickHouseType::CircleArray => "CIRCLE[]",
            ClickHouseType::Macaddr8 => "MACADDR8",
            ClickHouseType::Macaddr8Array => "MACADDR8[]",
            ClickHouseType::Macaddr => "MACADDR",
            ClickHouseType::Inet => "INET",
            ClickHouseType::BoolArray => "BOOL[]",
            ClickHouseType::ByteaArray => "BYTEA[]",
            ClickHouseType::CharArray => "\"CHAR\"[]",
            ClickHouseType::NameArray => "NAME[]",
            ClickHouseType::Int2Array => "INT2[]",
            ClickHouseType::Int4Array => "INT4[]",
            ClickHouseType::TextArray => "TEXT[]",
            ClickHouseType::BpcharArray => "CHAR[]",
            ClickHouseType::VarcharArray => "VARCHAR[]",
            ClickHouseType::Int8Array => "INT8[]",
            ClickHouseType::PointArray => "POINT[]",
            ClickHouseType::LsegArray => "LSEG[]",
            ClickHouseType::PathArray => "PATH[]",
            ClickHouseType::BoxArray => "BOX[]",
            ClickHouseType::Float4Array => "FLOAT4[]",
            ClickHouseType::Float8Array => "FLOAT8[]",
            ClickHouseType::PolygonArray => "POLYGON[]",
            ClickHouseType::OidArray => "OID[]",
            ClickHouseType::MacaddrArray => "MACADDR[]",
            ClickHouseType::InetArray => "INET[]",
            ClickHouseType::Bpchar => "CHAR",
            ClickHouseType::Varchar => "VARCHAR",
            ClickHouseType::Date => "DATE",
            ClickHouseType::Time => "TIME",
            ClickHouseType::Timestamp => "TIMESTAMP",
            ClickHouseType::TimestampArray => "TIMESTAMP[]",
            ClickHouseType::DateArray => "DATE[]",
            ClickHouseType::TimeArray => "TIME[]",
            ClickHouseType::Timestamptz => "TIMESTAMPTZ",
            ClickHouseType::TimestamptzArray => "TIMESTAMPTZ[]",
            ClickHouseType::Interval => "INTERVAL",
            ClickHouseType::IntervalArray => "INTERVAL[]",
            ClickHouseType::NumericArray => "NUMERIC[]",
            ClickHouseType::Timetz => "TIMETZ",
            ClickHouseType::TimetzArray => "TIMETZ[]",
            ClickHouseType::Bit => "BIT",
            ClickHouseType::BitArray => "BIT[]",
            ClickHouseType::Varbit => "VARBIT",
            ClickHouseType::VarbitArray => "VARBIT[]",
            ClickHouseType::Numeric => "NUMERIC",
            ClickHouseType::Record => "RECORD",
            ClickHouseType::RecordArray => "RECORD[]",
            ClickHouseType::Uuid => "UUID",
            ClickHouseType::UuidArray => "UUID[]",
            ClickHouseType::Jsonb => "JSONB",
            ClickHouseType::JsonbArray => "JSONB[]",
            ClickHouseType::Int4Range => "INT4RANGE",
            ClickHouseType::Int4RangeArray => "INT4RANGE[]",
            ClickHouseType::NumRange => "NUMRANGE",
            ClickHouseType::NumRangeArray => "NUMRANGE[]",
            ClickHouseType::TsRange => "TSRANGE",
            ClickHouseType::TsRangeArray => "TSRANGE[]",
            ClickHouseType::TstzRange => "TSTZRANGE",
            ClickHouseType::TstzRangeArray => "TSTZRANGE[]",
            ClickHouseType::DateRange => "DATERANGE",
            ClickHouseType::DateRangeArray => "DATERANGE[]",
            ClickHouseType::Int8Range => "INT8RANGE",
            ClickHouseType::Int8RangeArray => "INT8RANGE[]",
            ClickHouseType::Jsonpath => "JSONPATH",
            ClickHouseType::JsonpathArray => "JSONPATH[]",
            ClickHouseType::Money => "MONEY",
            ClickHouseType::MoneyArray => "MONEY[]",
            ClickHouseType::Void => "VOID",
            ClickHouseType::Custom(ty) => &ty.name,
            ClickHouseType::DeclareWithOid(_) => "?",
            ClickHouseType::DeclareWithName(name) => name,
            ClickHouseType::DeclareArrayOf(array) => &array.name,
        }
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            ClickHouseType::Bool => "bool",
            ClickHouseType::Bytea => "bytea",
            ClickHouseType::Char => "char",
            ClickHouseType::Name => "name",
            ClickHouseType::Int8 => "int8",
            ClickHouseType::Int2 => "int2",
            ClickHouseType::Int4 => "int4",
            ClickHouseType::Text => "text",
            ClickHouseType::Oid => "oid",
            ClickHouseType::Json => "json",
            ClickHouseType::JsonArray => "_json",
            ClickHouseType::Point => "point",
            ClickHouseType::Lseg => "lseg",
            ClickHouseType::Path => "path",
            ClickHouseType::Box => "box",
            ClickHouseType::Polygon => "polygon",
            ClickHouseType::Line => "line",
            ClickHouseType::LineArray => "_line",
            ClickHouseType::Cidr => "cidr",
            ClickHouseType::CidrArray => "_cidr",
            ClickHouseType::Float4 => "float4",
            ClickHouseType::Float8 => "float8",
            ClickHouseType::Unknown => "unknown",
            ClickHouseType::Circle => "circle",
            ClickHouseType::CircleArray => "_circle",
            ClickHouseType::Macaddr8 => "macaddr8",
            ClickHouseType::Macaddr8Array => "_macaddr8",
            ClickHouseType::Macaddr => "macaddr",
            ClickHouseType::Inet => "inet",
            ClickHouseType::BoolArray => "_bool",
            ClickHouseType::ByteaArray => "_bytea",
            ClickHouseType::CharArray => "_char",
            ClickHouseType::NameArray => "_name",
            ClickHouseType::Int2Array => "_int2",
            ClickHouseType::Int4Array => "_int4",
            ClickHouseType::TextArray => "_text",
            ClickHouseType::BpcharArray => "_bpchar",
            ClickHouseType::VarcharArray => "_varchar",
            ClickHouseType::Int8Array => "_int8",
            ClickHouseType::PointArray => "_point",
            ClickHouseType::LsegArray => "_lseg",
            ClickHouseType::PathArray => "_path",
            ClickHouseType::BoxArray => "_box",
            ClickHouseType::Float4Array => "_float4",
            ClickHouseType::Float8Array => "_float8",
            ClickHouseType::PolygonArray => "_polygon",
            ClickHouseType::OidArray => "_oid",
            ClickHouseType::MacaddrArray => "_macaddr",
            ClickHouseType::InetArray => "_inet",
            ClickHouseType::Bpchar => "bpchar",
            ClickHouseType::Varchar => "varchar",
            ClickHouseType::Date => "date",
            ClickHouseType::Time => "time",
            ClickHouseType::Timestamp => "timestamp",
            ClickHouseType::TimestampArray => "_timestamp",
            ClickHouseType::DateArray => "_date",
            ClickHouseType::TimeArray => "_time",
            ClickHouseType::Timestamptz => "timestamptz",
            ClickHouseType::TimestamptzArray => "_timestamptz",
            ClickHouseType::Interval => "interval",
            ClickHouseType::IntervalArray => "_interval",
            ClickHouseType::NumericArray => "_numeric",
            ClickHouseType::Timetz => "timetz",
            ClickHouseType::TimetzArray => "_timetz",
            ClickHouseType::Bit => "bit",
            ClickHouseType::BitArray => "_bit",
            ClickHouseType::Varbit => "varbit",
            ClickHouseType::VarbitArray => "_varbit",
            ClickHouseType::Numeric => "numeric",
            ClickHouseType::Record => "record",
            ClickHouseType::RecordArray => "_record",
            ClickHouseType::Uuid => "uuid",
            ClickHouseType::UuidArray => "_uuid",
            ClickHouseType::Jsonb => "jsonb",
            ClickHouseType::JsonbArray => "_jsonb",
            ClickHouseType::Int4Range => "int4range",
            ClickHouseType::Int4RangeArray => "_int4range",
            ClickHouseType::NumRange => "numrange",
            ClickHouseType::NumRangeArray => "_numrange",
            ClickHouseType::TsRange => "tsrange",
            ClickHouseType::TsRangeArray => "_tsrange",
            ClickHouseType::TstzRange => "tstzrange",
            ClickHouseType::TstzRangeArray => "_tstzrange",
            ClickHouseType::DateRange => "daterange",
            ClickHouseType::DateRangeArray => "_daterange",
            ClickHouseType::Int8Range => "int8range",
            ClickHouseType::Int8RangeArray => "_int8range",
            ClickHouseType::Jsonpath => "jsonpath",
            ClickHouseType::JsonpathArray => "_jsonpath",
            ClickHouseType::Money => "money",
            ClickHouseType::MoneyArray => "_money",
            ClickHouseType::Void => "void",
            ClickHouseType::Custom(ty) => &ty.name,
            ClickHouseType::DeclareWithOid(_) => "?",
            ClickHouseType::DeclareWithName(name) => name,
            ClickHouseType::DeclareArrayOf(array) => &array.name,
        }
    }

    pub(crate) fn kind(&self) -> &ClickHouseTypeKind {
        match self {
            ClickHouseType::Bool => &ClickHouseTypeKind::Simple,
            ClickHouseType::Bytea => &ClickHouseTypeKind::Simple,
            ClickHouseType::Char => &ClickHouseTypeKind::Simple,
            ClickHouseType::Name => &ClickHouseTypeKind::Simple,
            ClickHouseType::Int8 => &ClickHouseTypeKind::Simple,
            ClickHouseType::Int2 => &ClickHouseTypeKind::Simple,
            ClickHouseType::Int4 => &ClickHouseTypeKind::Simple,
            ClickHouseType::Text => &ClickHouseTypeKind::Simple,
            ClickHouseType::Oid => &ClickHouseTypeKind::Simple,
            ClickHouseType::Json => &ClickHouseTypeKind::Simple,
            ClickHouseType::JsonArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Json)),
            ClickHouseType::Point => &ClickHouseTypeKind::Simple,
            ClickHouseType::Lseg => &ClickHouseTypeKind::Simple,
            ClickHouseType::Path => &ClickHouseTypeKind::Simple,
            ClickHouseType::Box => &ClickHouseTypeKind::Simple,
            ClickHouseType::Polygon => &ClickHouseTypeKind::Simple,
            ClickHouseType::Line => &ClickHouseTypeKind::Simple,
            ClickHouseType::LineArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Line)),
            ClickHouseType::Cidr => &ClickHouseTypeKind::Simple,
            ClickHouseType::CidrArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Cidr)),
            ClickHouseType::Float4 => &ClickHouseTypeKind::Simple,
            ClickHouseType::Float8 => &ClickHouseTypeKind::Simple,
            ClickHouseType::Unknown => &ClickHouseTypeKind::Simple,
            ClickHouseType::Circle => &ClickHouseTypeKind::Simple,
            ClickHouseType::CircleArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Circle)),
            ClickHouseType::Macaddr8 => &ClickHouseTypeKind::Simple,
            ClickHouseType::Macaddr8Array => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Macaddr8)),
            ClickHouseType::Macaddr => &ClickHouseTypeKind::Simple,
            ClickHouseType::Inet => &ClickHouseTypeKind::Simple,
            ClickHouseType::BoolArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Bool)),
            ClickHouseType::ByteaArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Bytea)),
            ClickHouseType::CharArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Char)),
            ClickHouseType::NameArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Name)),
            ClickHouseType::Int2Array => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Int2)),
            ClickHouseType::Int4Array => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Int4)),
            ClickHouseType::TextArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Text)),
            ClickHouseType::BpcharArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Bpchar)),
            ClickHouseType::VarcharArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Varchar)),
            ClickHouseType::Int8Array => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Int8)),
            ClickHouseType::PointArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Point)),
            ClickHouseType::LsegArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Lseg)),
            ClickHouseType::PathArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Path)),
            ClickHouseType::BoxArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Box)),
            ClickHouseType::Float4Array => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Float4)),
            ClickHouseType::Float8Array => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Float8)),
            ClickHouseType::PolygonArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Polygon)),
            ClickHouseType::OidArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Oid)),
            ClickHouseType::MacaddrArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Macaddr)),
            ClickHouseType::InetArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Inet)),
            ClickHouseType::Bpchar => &ClickHouseTypeKind::Simple,
            ClickHouseType::Varchar => &ClickHouseTypeKind::Simple,
            ClickHouseType::Date => &ClickHouseTypeKind::Simple,
            ClickHouseType::Time => &ClickHouseTypeKind::Simple,
            ClickHouseType::Timestamp => &ClickHouseTypeKind::Simple,
            ClickHouseType::TimestampArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Timestamp)),
            ClickHouseType::DateArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Date)),
            ClickHouseType::TimeArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Time)),
            ClickHouseType::Timestamptz => &ClickHouseTypeKind::Simple,
            ClickHouseType::TimestamptzArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Timestamptz)),
            ClickHouseType::Interval => &ClickHouseTypeKind::Simple,
            ClickHouseType::IntervalArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Interval)),
            ClickHouseType::NumericArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Numeric)),
            ClickHouseType::Timetz => &ClickHouseTypeKind::Simple,
            ClickHouseType::TimetzArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Timetz)),
            ClickHouseType::Bit => &ClickHouseTypeKind::Simple,
            ClickHouseType::BitArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Bit)),
            ClickHouseType::Varbit => &ClickHouseTypeKind::Simple,
            ClickHouseType::VarbitArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Varbit)),
            ClickHouseType::Numeric => &ClickHouseTypeKind::Simple,
            ClickHouseType::Record => &ClickHouseTypeKind::Simple,
            ClickHouseType::RecordArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Record)),
            ClickHouseType::Uuid => &ClickHouseTypeKind::Simple,
            ClickHouseType::UuidArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Uuid)),
            ClickHouseType::Jsonb => &ClickHouseTypeKind::Simple,
            ClickHouseType::JsonbArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Jsonb)),
            ClickHouseType::Int4Range => &ClickHouseTypeKind::Range(ClickHouseTypeInfo::INT4),
            ClickHouseType::Int4RangeArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Int4Range)),
            ClickHouseType::NumRange => &ClickHouseTypeKind::Range(ClickHouseTypeInfo::NUMERIC),
            ClickHouseType::NumRangeArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::NumRange)),
            ClickHouseType::TsRange => &ClickHouseTypeKind::Range(ClickHouseTypeInfo::TIMESTAMP),
            ClickHouseType::TsRangeArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::TsRange)),
            ClickHouseType::TstzRange => &ClickHouseTypeKind::Range(ClickHouseTypeInfo::TIMESTAMPTZ),
            ClickHouseType::TstzRangeArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::TstzRange)),
            ClickHouseType::DateRange => &ClickHouseTypeKind::Range(ClickHouseTypeInfo::DATE),
            ClickHouseType::DateRangeArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::DateRange)),
            ClickHouseType::Int8Range => &ClickHouseTypeKind::Range(ClickHouseTypeInfo::INT8),
            ClickHouseType::Int8RangeArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Int8Range)),
            ClickHouseType::Jsonpath => &ClickHouseTypeKind::Simple,
            ClickHouseType::JsonpathArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Jsonpath)),
            ClickHouseType::Money => &ClickHouseTypeKind::Simple,
            ClickHouseType::MoneyArray => &ClickHouseTypeKind::Array(ClickHouseTypeInfo(ClickHouseType::Money)),

            ClickHouseType::Void => &ClickHouseTypeKind::Pseudo,

            ClickHouseType::Custom(ty) => &ty.kind,

            ClickHouseType::DeclareWithOid(oid) => {
                unreachable!("(bug) use of unresolved type declaration [oid={}]", oid.0);
            }
            ClickHouseType::DeclareWithName(name) => {
                unreachable!("(bug) use of unresolved type declaration [name={name}]");
            }
            ClickHouseType::DeclareArrayOf(array) => {
                unreachable!(
                    "(bug) use of unresolved type declaration [array of={}]",
                    array.elem_name
                );
            }
        }
    }

    /// If `self` is an array type, return the type info for its element.
    pub(crate) fn try_array_element(&self) -> Option<Cow<'_, ClickHouseTypeInfo>> {
        // We explicitly match on all the `None` cases to ensure an exhaustive match.
        match self {
            ClickHouseType::Bool => None,
            ClickHouseType::BoolArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Bool))),
            ClickHouseType::Bytea => None,
            ClickHouseType::ByteaArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Bytea))),
            ClickHouseType::Char => None,
            ClickHouseType::CharArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Char))),
            ClickHouseType::Name => None,
            ClickHouseType::NameArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Name))),
            ClickHouseType::Int8 => None,
            ClickHouseType::Int8Array => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Int8))),
            ClickHouseType::Int2 => None,
            ClickHouseType::Int2Array => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Int2))),
            ClickHouseType::Int4 => None,
            ClickHouseType::Int4Array => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Int4))),
            ClickHouseType::Text => None,
            ClickHouseType::TextArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Text))),
            ClickHouseType::Oid => None,
            ClickHouseType::OidArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Oid))),
            ClickHouseType::Json => None,
            ClickHouseType::JsonArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Json))),
            ClickHouseType::Point => None,
            ClickHouseType::PointArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Point))),
            ClickHouseType::Lseg => None,
            ClickHouseType::LsegArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Lseg))),
            ClickHouseType::Path => None,
            ClickHouseType::PathArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Path))),
            ClickHouseType::Box => None,
            ClickHouseType::BoxArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Box))),
            ClickHouseType::Polygon => None,
            ClickHouseType::PolygonArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Polygon))),
            ClickHouseType::Line => None,
            ClickHouseType::LineArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Line))),
            ClickHouseType::Cidr => None,
            ClickHouseType::CidrArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Cidr))),
            ClickHouseType::Float4 => None,
            ClickHouseType::Float4Array => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Float4))),
            ClickHouseType::Float8 => None,
            ClickHouseType::Float8Array => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Float8))),
            ClickHouseType::Circle => None,
            ClickHouseType::CircleArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Circle))),
            ClickHouseType::Macaddr8 => None,
            ClickHouseType::Macaddr8Array => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Macaddr8))),
            ClickHouseType::Money => None,
            ClickHouseType::MoneyArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Money))),
            ClickHouseType::Macaddr => None,
            ClickHouseType::MacaddrArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Macaddr))),
            ClickHouseType::Inet => None,
            ClickHouseType::InetArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Inet))),
            ClickHouseType::Bpchar => None,
            ClickHouseType::BpcharArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Bpchar))),
            ClickHouseType::Varchar => None,
            ClickHouseType::VarcharArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Varchar))),
            ClickHouseType::Date => None,
            ClickHouseType::DateArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Date))),
            ClickHouseType::Time => None,
            ClickHouseType::TimeArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Time))),
            ClickHouseType::Timestamp => None,
            ClickHouseType::TimestampArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Timestamp))),
            ClickHouseType::Timestamptz => None,
            ClickHouseType::TimestamptzArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Timestamptz))),
            ClickHouseType::Interval => None,
            ClickHouseType::IntervalArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Interval))),
            ClickHouseType::Timetz => None,
            ClickHouseType::TimetzArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Timetz))),
            ClickHouseType::Bit => None,
            ClickHouseType::BitArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Bit))),
            ClickHouseType::Varbit => None,
            ClickHouseType::VarbitArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Varbit))),
            ClickHouseType::Numeric => None,
            ClickHouseType::NumericArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Numeric))),
            ClickHouseType::Record => None,
            ClickHouseType::RecordArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Record))),
            ClickHouseType::Uuid => None,
            ClickHouseType::UuidArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Uuid))),
            ClickHouseType::Jsonb => None,
            ClickHouseType::JsonbArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Jsonb))),
            ClickHouseType::Int4Range => None,
            ClickHouseType::Int4RangeArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Int4Range))),
            ClickHouseType::NumRange => None,
            ClickHouseType::NumRangeArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::NumRange))),
            ClickHouseType::TsRange => None,
            ClickHouseType::TsRangeArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::TsRange))),
            ClickHouseType::TstzRange => None,
            ClickHouseType::TstzRangeArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::TstzRange))),
            ClickHouseType::DateRange => None,
            ClickHouseType::DateRangeArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::DateRange))),
            ClickHouseType::Int8Range => None,
            ClickHouseType::Int8RangeArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Int8Range))),
            ClickHouseType::Jsonpath => None,
            ClickHouseType::JsonpathArray => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::Jsonpath))),
            // There is no `UnknownArray`
            ClickHouseType::Unknown => None,
            // There is no `VoidArray`
            ClickHouseType::Void => None,

            ClickHouseType::Custom(ty) => match &ty.kind {
                ClickHouseTypeKind::Simple => None,
                ClickHouseTypeKind::Pseudo => None,
                ClickHouseTypeKind::Domain(_) => None,
                ClickHouseTypeKind::Composite(_) => None,
                ClickHouseTypeKind::Array(ref elem_type_info) => Some(Cow::Borrowed(elem_type_info)),
                ClickHouseTypeKind::Enum(_) => None,
                ClickHouseTypeKind::Range(_) => None,
            },
            ClickHouseType::DeclareWithOid(_) => None,
            ClickHouseType::DeclareWithName(name) => {
                // LEGACY: infer the array element name from a `_` prefix
                UStr::strip_prefix(name, "_")
                    .map(|elem| Cow::Owned(ClickHouseTypeInfo(ClickHouseType::DeclareWithName(elem))))
            }
            ClickHouseType::DeclareArrayOf(array) => Some(Cow::Owned(ClickHouseTypeInfo(ClickHouseType::DeclareWithName(
                array.elem_name.clone(),
            )))),
        }
    }

    /// Returns `true` if this type cannot be matched by name.
    fn is_declare_with_oid(&self) -> bool {
        matches!(self, Self::DeclareWithOid(_))
    }

    /// Compare two `ClickHouseType`s, first by OID, then by array element, then by name.
    ///
    /// If `soft_eq` is true and `self` or `other` is `DeclareWithOid` but not both, return `true`
    /// before checking names.
    fn eq_impl(&self, other: &Self, soft_eq: bool) -> bool {
        if let (Some(a), Some(b)) = (self.try_oid(), other.try_oid()) {
            // If there are OIDs available, use OIDs to perform a direct match
            return a == b;
        }

        if soft_eq && (self.is_declare_with_oid() || other.is_declare_with_oid()) {
            // If we get to this point, one instance is `DeclareWithOid()` and the other is
            // `DeclareArrayOf()` or `DeclareWithName()`, which means we can't compare the two.
            //
            // Since this is only likely to occur when using the text protocol where we can't
            // resolve type names before executing a query, we can just opt out of typechecking.
            return true;
        }

        if let (Some(elem_a), Some(elem_b)) = (self.try_array_element(), other.try_array_element())
        {
            return elem_a == elem_b;
        }

        // Otherwise, perform a match on the name
        name_eq(self.name(), other.name())
    }
}

impl TypeInfo for ClickHouseTypeInfo {
    fn name(&self) -> &str {
        self.0.display_name()
    }

    fn is_null(&self) -> bool {
        false
    }

    fn is_void(&self) -> bool {
        matches!(self.0, ClickHouseType::Void)
    }

    fn type_compatible(&self, other: &Self) -> bool
    where
        Self: Sized,
    {
        self == other
    }
}

impl PartialEq<ClickHouseCustomType> for ClickHouseCustomType {
    fn eq(&self, other: &ClickHouseCustomType) -> bool {
        other.oid == self.oid
    }
}

impl ClickHouseTypeInfo {
    // boolean, state of true or false
    pub(crate) const BOOL: Self = Self(ClickHouseType::Bool);
    pub(crate) const BOOL_ARRAY: Self = Self(ClickHouseType::BoolArray);

    // binary data types, variable-length binary string
    pub(crate) const BYTEA: Self = Self(ClickHouseType::Bytea);
    pub(crate) const BYTEA_ARRAY: Self = Self(ClickHouseType::ByteaArray);

    // uuid
    pub(crate) const UUID: Self = Self(ClickHouseType::Uuid);
    pub(crate) const UUID_ARRAY: Self = Self(ClickHouseType::UuidArray);

    // record
    pub(crate) const RECORD: Self = Self(ClickHouseType::Record);
    pub(crate) const RECORD_ARRAY: Self = Self(ClickHouseType::RecordArray);

    //
    // JSON types
    // https://www.postgresql.org/docs/current/datatype-json.html
    //

    pub(crate) const JSON: Self = Self(ClickHouseType::Json);
    pub(crate) const JSON_ARRAY: Self = Self(ClickHouseType::JsonArray);

    pub(crate) const JSONB: Self = Self(ClickHouseType::Jsonb);
    pub(crate) const JSONB_ARRAY: Self = Self(ClickHouseType::JsonbArray);

    pub(crate) const JSONPATH: Self = Self(ClickHouseType::Jsonpath);
    pub(crate) const JSONPATH_ARRAY: Self = Self(ClickHouseType::JsonpathArray);

    //
    // network address types
    // https://www.postgresql.org/docs/current/datatype-net-types.html
    //

    pub(crate) const CIDR: Self = Self(ClickHouseType::Cidr);
    pub(crate) const CIDR_ARRAY: Self = Self(ClickHouseType::CidrArray);

    pub(crate) const INET: Self = Self(ClickHouseType::Inet);
    pub(crate) const INET_ARRAY: Self = Self(ClickHouseType::InetArray);

    pub(crate) const MACADDR: Self = Self(ClickHouseType::Macaddr);
    pub(crate) const MACADDR_ARRAY: Self = Self(ClickHouseType::MacaddrArray);

    pub(crate) const MACADDR8: Self = Self(ClickHouseType::Macaddr8);
    pub(crate) const MACADDR8_ARRAY: Self = Self(ClickHouseType::Macaddr8Array);

    //
    // character types
    // https://www.postgresql.org/docs/current/datatype-character.html
    //

    // internal type for object names
    pub(crate) const NAME: Self = Self(ClickHouseType::Name);
    pub(crate) const NAME_ARRAY: Self = Self(ClickHouseType::NameArray);

    // character type, fixed-length, blank-padded
    pub(crate) const BPCHAR: Self = Self(ClickHouseType::Bpchar);
    pub(crate) const BPCHAR_ARRAY: Self = Self(ClickHouseType::BpcharArray);

    // character type, variable-length with limit
    pub(crate) const VARCHAR: Self = Self(ClickHouseType::Varchar);
    pub(crate) const VARCHAR_ARRAY: Self = Self(ClickHouseType::VarcharArray);

    // character type, variable-length
    pub(crate) const TEXT: Self = Self(ClickHouseType::Text);
    pub(crate) const TEXT_ARRAY: Self = Self(ClickHouseType::TextArray);

    // unknown type, transmitted as text
    pub(crate) const UNKNOWN: Self = Self(ClickHouseType::Unknown);

    //
    // numeric types
    // https://www.postgresql.org/docs/current/datatype-numeric.html
    //

    // single-byte internal type
    pub(crate) const CHAR: Self = Self(ClickHouseType::Char);
    pub(crate) const CHAR_ARRAY: Self = Self(ClickHouseType::CharArray);

    // internal type for type ids
    pub(crate) const OID: Self = Self(ClickHouseType::Oid);
    pub(crate) const OID_ARRAY: Self = Self(ClickHouseType::OidArray);

    // small-range integer; -32768 to +32767
    pub(crate) const INT2: Self = Self(ClickHouseType::Int2);
    pub(crate) const INT2_ARRAY: Self = Self(ClickHouseType::Int2Array);

    // typical choice for integer; -2147483648 to +2147483647
    pub(crate) const INT4: Self = Self(ClickHouseType::Int4);
    pub(crate) const INT4_ARRAY: Self = Self(ClickHouseType::Int4Array);

    // large-range integer; -9223372036854775808 to +9223372036854775807
    pub(crate) const INT8: Self = Self(ClickHouseType::Int8);
    pub(crate) const INT8_ARRAY: Self = Self(ClickHouseType::Int8Array);

    // variable-precision, inexact, 6 decimal digits precision
    pub(crate) const FLOAT4: Self = Self(ClickHouseType::Float4);
    pub(crate) const FLOAT4_ARRAY: Self = Self(ClickHouseType::Float4Array);

    // variable-precision, inexact, 15 decimal digits precision
    pub(crate) const FLOAT8: Self = Self(ClickHouseType::Float8);
    pub(crate) const FLOAT8_ARRAY: Self = Self(ClickHouseType::Float8Array);

    // user-specified precision, exact
    pub(crate) const NUMERIC: Self = Self(ClickHouseType::Numeric);
    pub(crate) const NUMERIC_ARRAY: Self = Self(ClickHouseType::NumericArray);

    // user-specified precision, exact
    pub(crate) const MONEY: Self = Self(ClickHouseType::Money);
    pub(crate) const MONEY_ARRAY: Self = Self(ClickHouseType::MoneyArray);

    //
    // date/time types
    // https://www.postgresql.org/docs/current/datatype-datetime.html
    //

    // both date and time (no time zone)
    pub(crate) const TIMESTAMP: Self = Self(ClickHouseType::Timestamp);
    pub(crate) const TIMESTAMP_ARRAY: Self = Self(ClickHouseType::TimestampArray);

    // both date and time (with time zone)
    pub(crate) const TIMESTAMPTZ: Self = Self(ClickHouseType::Timestamptz);
    pub(crate) const TIMESTAMPTZ_ARRAY: Self = Self(ClickHouseType::TimestamptzArray);

    // date (no time of day)
    pub(crate) const DATE: Self = Self(ClickHouseType::Date);
    pub(crate) const DATE_ARRAY: Self = Self(ClickHouseType::DateArray);

    // time of day (no date)
    pub(crate) const TIME: Self = Self(ClickHouseType::Time);
    pub(crate) const TIME_ARRAY: Self = Self(ClickHouseType::TimeArray);

    // time of day (no date), with time zone
    pub(crate) const TIMETZ: Self = Self(ClickHouseType::Timetz);
    pub(crate) const TIMETZ_ARRAY: Self = Self(ClickHouseType::TimetzArray);

    // time interval
    pub(crate) const INTERVAL: Self = Self(ClickHouseType::Interval);
    pub(crate) const INTERVAL_ARRAY: Self = Self(ClickHouseType::IntervalArray);

    //
    // geometric types
    // https://www.postgresql.org/docs/current/datatype-geometric.html
    //

    // point on a plane
    pub(crate) const POINT: Self = Self(ClickHouseType::Point);
    pub(crate) const POINT_ARRAY: Self = Self(ClickHouseType::PointArray);

    // infinite line
    pub(crate) const LINE: Self = Self(ClickHouseType::Line);
    pub(crate) const LINE_ARRAY: Self = Self(ClickHouseType::LineArray);

    // finite line segment
    pub(crate) const LSEG: Self = Self(ClickHouseType::Lseg);
    pub(crate) const LSEG_ARRAY: Self = Self(ClickHouseType::LsegArray);

    // rectangular box
    pub(crate) const BOX: Self = Self(ClickHouseType::Box);
    pub(crate) const BOX_ARRAY: Self = Self(ClickHouseType::BoxArray);

    // open or closed path
    pub(crate) const PATH: Self = Self(ClickHouseType::Path);
    pub(crate) const PATH_ARRAY: Self = Self(ClickHouseType::PathArray);

    // polygon
    pub(crate) const POLYGON: Self = Self(ClickHouseType::Polygon);
    pub(crate) const POLYGON_ARRAY: Self = Self(ClickHouseType::PolygonArray);

    // circle
    pub(crate) const CIRCLE: Self = Self(ClickHouseType::Circle);
    pub(crate) const CIRCLE_ARRAY: Self = Self(ClickHouseType::CircleArray);

    //
    // bit string types
    // https://www.postgresql.org/docs/current/datatype-bit.html
    //

    pub(crate) const BIT: Self = Self(ClickHouseType::Bit);
    pub(crate) const BIT_ARRAY: Self = Self(ClickHouseType::BitArray);

    pub(crate) const VARBIT: Self = Self(ClickHouseType::Varbit);
    pub(crate) const VARBIT_ARRAY: Self = Self(ClickHouseType::VarbitArray);

    //
    // range types
    // https://www.postgresql.org/docs/current/rangetypes.html
    //

    pub(crate) const INT4_RANGE: Self = Self(ClickHouseType::Int4Range);
    pub(crate) const INT4_RANGE_ARRAY: Self = Self(ClickHouseType::Int4RangeArray);

    pub(crate) const NUM_RANGE: Self = Self(ClickHouseType::NumRange);
    pub(crate) const NUM_RANGE_ARRAY: Self = Self(ClickHouseType::NumRangeArray);

    pub(crate) const TS_RANGE: Self = Self(ClickHouseType::TsRange);
    pub(crate) const TS_RANGE_ARRAY: Self = Self(ClickHouseType::TsRangeArray);

    pub(crate) const TSTZ_RANGE: Self = Self(ClickHouseType::TstzRange);
    pub(crate) const TSTZ_RANGE_ARRAY: Self = Self(ClickHouseType::TstzRangeArray);

    pub(crate) const DATE_RANGE: Self = Self(ClickHouseType::DateRange);
    pub(crate) const DATE_RANGE_ARRAY: Self = Self(ClickHouseType::DateRangeArray);

    pub(crate) const INT8_RANGE: Self = Self(ClickHouseType::Int8Range);
    pub(crate) const INT8_RANGE_ARRAY: Self = Self(ClickHouseType::Int8RangeArray);

    //
    // pseudo types
    // https://www.postgresql.org/docs/9.3/datatype-pseudo.html
    //

    pub(crate) const VOID: Self = Self(ClickHouseType::Void);
}

impl Display for ClickHouseTypeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(self.name())
    }
}

impl PartialEq<ClickHouseType> for ClickHouseType {
    fn eq(&self, other: &ClickHouseType) -> bool {
        self.eq_impl(other, true)
    }
}

/// Check type names for equality, respecting ClickHouse' case sensitivity rules for identifiers.
///
/// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
fn name_eq(name1: &str, name2: &str) -> bool {
    // Cop-out of processing Unicode escapes by just using string equality.
    if name1.starts_with("U&") {
        // If `name2` doesn't start with `U&` this will automatically be `false`.
        return name1 == name2;
    }

    let mut chars1 = identifier_chars(name1);
    let mut chars2 = identifier_chars(name2);

    while let (Some(a), Some(b)) = (chars1.next(), chars2.next()) {
        if !a.eq(&b) {
            return false;
        }
    }

    chars1.next().is_none() && chars2.next().is_none()
}

struct IdentifierChar {
    ch: char,
    case_sensitive: bool,
}

impl IdentifierChar {
    fn eq(&self, other: &Self) -> bool {
        if self.case_sensitive || other.case_sensitive {
            self.ch == other.ch
        } else {
            self.ch.eq_ignore_ascii_case(&other.ch)
        }
    }
}

/// Return an iterator over all significant characters of an identifier.
///
/// Ignores non-escaped quotation marks.
fn identifier_chars(ident: &str) -> impl Iterator<Item = IdentifierChar> + '_ {
    let mut case_sensitive = false;
    let mut last_char_quote = false;

    ident.chars().filter_map(move |ch| {
        if ch == '"' {
            if last_char_quote {
                last_char_quote = false;
            } else {
                last_char_quote = true;
                return None;
            }
        } else if last_char_quote {
            last_char_quote = false;
            case_sensitive = !case_sensitive;
        }

        Some(IdentifierChar { ch, case_sensitive })
    })
}

#[test]
fn test_name_eq() {
    let test_values = [
        ("foo", "foo", true),
        ("foo", "Foo", true),
        ("foo", "FOO", true),
        ("foo", r#""foo""#, true),
        ("foo", r#""Foo""#, false),
        ("foo", "foo.foo", false),
        ("foo.foo", "foo.foo", true),
        ("foo.foo", "foo.Foo", true),
        ("foo.foo", "foo.FOO", true),
        ("foo.foo", "Foo.foo", true),
        ("foo.foo", "Foo.Foo", true),
        ("foo.foo", "FOO.FOO", true),
        ("foo.foo", "foo", false),
        ("foo.foo", r#"foo."foo""#, true),
        ("foo.foo", r#"foo."Foo""#, false),
        ("foo.foo", r#"foo."FOO""#, false),
    ];

    for (left, right, eq) in test_values {
        assert_eq!(
            name_eq(left, right),
            eq,
            "failed check for name_eq({left:?}, {right:?})"
        );
        assert_eq!(
            name_eq(right, left),
            eq,
            "failed check for name_eq({right:?}, {left:?})"
        );
    }
}
