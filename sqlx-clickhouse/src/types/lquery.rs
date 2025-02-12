use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};
use bitflags::bitflags;
use std::fmt::{self, Display, Formatter};
use std::io::Write;
use std::ops::Deref;
use std::str::FromStr;

use crate::types::ltree::{ClickHouseLTreeLabel, ClickHouseLTreeParseError};

/// Represents lquery specific errors
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClickHouseLQueryParseError {
    #[error("lquery cannot be empty")]
    EmptyString,
    #[error("unexpected character in lquery")]
    UnexpectedCharacter,
    #[error("error parsing integer: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("error parsing integer: {0}")]
    LTreeParrseError(#[from] ClickHouseLTreeParseError),
    /// LQuery version not supported
    #[error("lquery version not supported")]
    InvalidLqueryVersion,
}

/// Container for a Label Tree Query (`lquery`) in ClickHouse.
///
/// See <https://www.postgresql.org/docs/current/ltree.html>
///
/// ### Note: Requires ClickHouse 13+
///
/// This integration requires that the `lquery` type support the binary format in the ClickHouse
/// wire protocol, which only became available in ClickHouse 13.
/// ([ClickHouse 13.0 Release Notes, Additional Modules](https://www.postgresql.org/docs/13/release-13.html#id-1.11.6.11.5.14))
///
/// Ideally, SQLx's ClickHouse driver should support falling back to text format for types
/// which don't have `typsend` and `typrecv` entries in `pg_type`, but that work still needs
/// to be done.
///
/// ### Note: Extension Required
/// The `ltree` extension is not enabled by default in ClickHouse. You will need to do so explicitly:
///
/// ```ignore
/// CREATE EXTENSION IF NOT EXISTS "ltree";
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClickHouseLQuery {
    levels: Vec<ClickHouseLQueryLevel>,
}

// TODO: maybe a QueryBuilder pattern would be nice here
impl ClickHouseLQuery {
    /// creates default/empty lquery
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from(levels: Vec<ClickHouseLQueryLevel>) -> Self {
        Self { levels }
    }

    /// push a query level
    pub fn push(&mut self, level: ClickHouseLQueryLevel) {
        self.levels.push(level);
    }

    /// pop a query level
    pub fn pop(&mut self) -> Option<ClickHouseLQueryLevel> {
        self.levels.pop()
    }

    /// creates lquery from an iterator with checking labels
    // TODO: this should just be removed but I didn't want to bury it in a massive diff
    #[deprecated = "renamed to `try_from_iter()`"]
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<I, S>(levels: I) -> Result<Self, ClickHouseLQueryParseError>
    where
        S: Into<String>,
        I: IntoIterator<Item = S>,
    {
        let mut lquery = Self::default();
        for level in levels {
            lquery.push(ClickHouseLQueryLevel::from_str(&level.into())?);
        }
        Ok(lquery)
    }

    /// Create an `LQUERY` from an iterator of label strings.
    ///
    /// Returns an error if any label fails to parse according to [`ClickHouseLQueryLevel::from_str()`].
    pub fn try_from_iter<I, S>(levels: I) -> Result<Self, ClickHouseLQueryParseError>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        levels
            .into_iter()
            .map(|level| level.as_ref().parse::<ClickHouseLQueryLevel>())
            .collect()
    }
}

impl FromIterator<ClickHouseLQueryLevel> for ClickHouseLQuery {
    fn from_iter<T: IntoIterator<Item = ClickHouseLQueryLevel>>(iter: T) -> Self {
        Self::from(iter.into_iter().collect())
    }
}

impl IntoIterator for ClickHouseLQuery {
    type Item = ClickHouseLQueryLevel;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.levels.into_iter()
    }
}

impl FromStr for ClickHouseLQuery {
    type Err = ClickHouseLQueryParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            levels: s
                .split('.')
                .map(ClickHouseLQueryLevel::from_str)
                .collect::<Result<_, Self::Err>>()?,
        })
    }
}

impl Display for ClickHouseLQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut iter = self.levels.iter();
        if let Some(label) = iter.next() {
            write!(f, "{label}")?;
            for label in iter {
                write!(f, ".{label}")?;
            }
        }
        Ok(())
    }
}

impl Deref for ClickHouseLQuery {
    type Target = [ClickHouseLQueryLevel];

    fn deref(&self) -> &Self::Target {
        &self.levels
    }
}

impl Type<ClickHouse> for ClickHouseLQuery {
    fn type_info() -> ClickHouseTypeInfo {
        // Since `ltree` is enabled by an extension, it does not have a stable OID.
        ClickHouseTypeInfo::with_name("lquery")
    }
}

impl ClickHouseHasArrayType for ClickHouseLQuery {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::with_name("_lquery")
    }
}

impl Encode<'_, ClickHouse> for ClickHouseLQuery {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend(1i8.to_le_bytes());
        write!(buf, "{self}")?;

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, ClickHouse> for ClickHouseLQuery {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.format() {
            ClickHouseValueFormat::Binary => {
                let bytes = value.as_bytes()?;
                let version = i8::from_le_bytes([bytes[0]; 1]);
                if version != 1 {
                    return Err(Box::new(ClickHouseLQueryParseError::InvalidLqueryVersion));
                }
                Ok(Self::from_str(std::str::from_utf8(&bytes[1..])?)?)
            }
            ClickHouseValueFormat::Text => Ok(Self::from_str(value.as_str()?)?),
        }
    }
}

bitflags! {
    /// Modifiers that can be set to non-star labels
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ClickHouseLQueryVariantFlag: u16 {
        /// * - Match any label with this prefix, for example foo* matches foobar
        const ANY_END = 0x01;
        /// @ - Match case-insensitively, for example a@ matches A
        const IN_CASE = 0x02;
        /// % - Match initial underscore-separated words
        const SUBLEXEME = 0x04;
    }
}

impl Display for ClickHouseLQueryVariantFlag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.contains(ClickHouseLQueryVariantFlag::ANY_END) {
            write!(f, "*")?;
        }
        if self.contains(ClickHouseLQueryVariantFlag::IN_CASE) {
            write!(f, "@")?;
        }
        if self.contains(ClickHouseLQueryVariantFlag::SUBLEXEME) {
            write!(f, "%")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClickHouseLQueryVariant {
    label: ClickHouseLTreeLabel,
    modifiers: ClickHouseLQueryVariantFlag,
}

impl Display for ClickHouseLQueryVariant {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.label, self.modifiers)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClickHouseLQueryLevel {
    /// match any label (*) with optional at least / at most numbers
    Star(Option<u16>, Option<u16>),
    /// match any of specified labels with optional flags
    NonStar(Vec<ClickHouseLQueryVariant>),
    /// match none of specified labels with optional flags
    NotNonStar(Vec<ClickHouseLQueryVariant>),
}

impl FromStr for ClickHouseLQueryLevel {
    type Err = ClickHouseLQueryParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = s.as_bytes();
        if bytes.is_empty() {
            Err(ClickHouseLQueryParseError::EmptyString)
        } else {
            match bytes[0] {
                b'*' => {
                    if bytes.len() > 1 {
                        let parts = s[2..s.len() - 1].split(',').collect::<Vec<_>>();
                        match parts.len() {
                            1 => {
                                let number = parts[0].parse()?;
                                Ok(ClickHouseLQueryLevel::Star(Some(number), Some(number)))
                            }
                            2 => Ok(ClickHouseLQueryLevel::Star(
                                Some(parts[0].parse()?),
                                Some(parts[1].parse()?),
                            )),
                            _ => Err(ClickHouseLQueryParseError::UnexpectedCharacter),
                        }
                    } else {
                        Ok(ClickHouseLQueryLevel::Star(None, None))
                    }
                }
                b'!' => Ok(ClickHouseLQueryLevel::NotNonStar(
                    s[1..]
                        .split('|')
                        .map(ClickHouseLQueryVariant::from_str)
                        .collect::<Result<Vec<_>, ClickHouseLQueryParseError>>()?,
                )),
                _ => Ok(ClickHouseLQueryLevel::NonStar(
                    s.split('|')
                        .map(ClickHouseLQueryVariant::from_str)
                        .collect::<Result<Vec<_>, ClickHouseLQueryParseError>>()?,
                )),
            }
        }
    }
}

impl FromStr for ClickHouseLQueryVariant {
    type Err = ClickHouseLQueryParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut label_length = s.len();
        let mut modifiers = ClickHouseLQueryVariantFlag::empty();

        for b in s.bytes().rev() {
            match b {
                b'@' => modifiers.insert(ClickHouseLQueryVariantFlag::IN_CASE),
                b'*' => modifiers.insert(ClickHouseLQueryVariantFlag::ANY_END),
                b'%' => modifiers.insert(ClickHouseLQueryVariantFlag::SUBLEXEME),
                _ => break,
            }
            label_length -= 1;
        }

        Ok(ClickHouseLQueryVariant {
            label: ClickHouseLTreeLabel::new(&s[0..label_length])?,
            modifiers,
        })
    }
}

fn write_variants(f: &mut Formatter<'_>, variants: &[ClickHouseLQueryVariant], not: bool) -> fmt::Result {
    let mut iter = variants.iter();
    if let Some(variant) = iter.next() {
        write!(f, "{}{}", if not { "!" } else { "" }, variant)?;
        for variant in iter {
            write!(f, ".{variant}")?;
        }
    }
    Ok(())
}

impl Display for ClickHouseLQueryLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ClickHouseLQueryLevel::Star(Some(at_least), Some(at_most)) => {
                if at_least == at_most {
                    write!(f, "*{{{at_least}}}")
                } else {
                    write!(f, "*{{{at_least},{at_most}}}")
                }
            }
            ClickHouseLQueryLevel::Star(Some(at_least), _) => write!(f, "*{{{at_least},}}"),
            ClickHouseLQueryLevel::Star(_, Some(at_most)) => write!(f, "*{{,{at_most}}}"),
            ClickHouseLQueryLevel::Star(_, _) => write!(f, "*"),
            ClickHouseLQueryLevel::NonStar(variants) => write_variants(f, variants, false),
            ClickHouseLQueryLevel::NotNonStar(variants) => write_variants(f, variants, true),
        }
    }
}
