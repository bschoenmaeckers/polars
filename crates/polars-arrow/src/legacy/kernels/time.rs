use std::str::FromStr;
#[cfg(feature = "timezones")]
use jiff::{
    civil::DateTime,
    tz::{
        AmbiguousZoned,
        TimeZone
    }
};
#[cfg(feature = "timezones")]
use polars_error::PolarsResult;
use polars_error::{PolarsError, polars_bail};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use strum_macros::IntoStaticStr;

pub enum Ambiguous {
    Earliest,
    Latest,
    Null,
    Raise,
}
impl FromStr for Ambiguous {
    type Err = PolarsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "earliest" => Ok(Ambiguous::Earliest),
            "latest" => Ok(Ambiguous::Latest),
            "raise" => Ok(Ambiguous::Raise),
            "null" => Ok(Ambiguous::Null),
            s => polars_bail!(InvalidOperation:
                "Invalid argument {}, expected one of: \"earliest\", \"latest\", \"null\", \"raise\"", s
            ),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, IntoStaticStr)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
#[strum(serialize_all = "snake_case")]
pub enum NonExistent {
    Null,
    Raise,
}

#[cfg(feature = "timezones")]
pub fn convert_to_naive_local(
    from_tz: &TimeZone,
    to_tz: &TimeZone,
    ndt: DateTime,
    ambiguous: Ambiguous,
    non_existent: NonExistent,
) -> PolarsResult<Option<DateTime>> {
    // TODO Can this be shorter?
    let ndt = ndt
        .to_zoned(TimeZone::UTC)
        .unwrap()
        .with_time_zone(from_tz.clone())
        .datetime();
    let ambiguous_zoned = to_tz.to_ambiguous_zoned(ndt);
    // match ambiguous {
    //     Ambiguous::Earliest => {ambiguous_zoned.earlier()}
    //     Ambiguous::Latest => {ambiguous_zoned.latest()}
    //     _ => todo!()
    // };
    todo!()
}

/// Same as convert_to_naive_local, but return `None` instead
/// raising - in some cases this can be used to save a string allocation.
#[cfg(feature = "timezones")]
pub fn convert_to_naive_local_opt(
    from_tz: &TimeZone,
    to_tz: &TimeZone,
    ndt: DateTime,
    ambiguous: Ambiguous,
) -> Option<Option<DateTime>> {
    let ndt = ndt
        .to_zoned(TimeZone::UTC)
        .unwrap()
        .with_time_zone(from_tz.clone())
        .datetime();
    // let ndt = from_tz.from_utc_datetime(&ndt).naive_local();
    // match to_tz.from_local_datetime(&ndt) {
    //     LocalResult::Single(dt) => Some(Some(dt.naive_utc())),
    //     LocalResult::Ambiguous(dt_earliest, dt_latest) => match ambiguous {
    //         Ambiguous::Earliest => Some(Some(dt_earliest.naive_utc())),
    //         Ambiguous::Latest => Some(Some(dt_latest.naive_utc())),
    //         Ambiguous::Null => Some(None),
    //         Ambiguous::Raise => None,
    //     },
    //     LocalResult::None => None,
    // }
    todo!()
}
