//! Frame → [`Message`]. A backend (`serde_json` or `sonic-rs`) makes one validating pass over
//! the frame and collects the fields the tracker reads as raw slices of the frame
//! ([`Fields`]); then the JS `FastTracker` rules are applied to those slices ([`interpret`]).
//! Duplicate keys: the last one wins, like `JSON.parse`.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, Deserialize, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;
use smallvec::SmallVec;
use wt_core::{AnnounceEvent, Key};

use crate::json::{decode_cow, decode_key, is_string, number, string_equals};
use crate::{Payload, ProtoError};

/// Offers kept inline per announce (more spill to the heap).
pub const INLINE_OFFERS: usize = 20;

/// A parsed message.
// Offers are inline on purpose: boxing them would allocate per message.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Message<'a> {
    Announce {
        info_hash: Key,
        peer_id: Key,
        event: AnnounceEvent,
        left_zero: bool,
        numwant: Option<u32>,
        offers: Option<SmallVec<[Payload<'a>; INLINE_OFFERS]>>,
    },
    /// `None` ids (too long, lone surrogate) can never match: the answer is dropped.
    Answer {
        /// The swarm (a string is required).
        info_hash: Option<Key>,
        /// The sender: must be a peer of the requesting connection in the swarm (spec §5.3).
        peer_id: Option<Key>,
        to_peer_id: Key,
        answer: Payload<'a>,
    },
    /// `None` fields can never match (wrong type or too long): the stop is a no-op.
    Stop {
        info_hash: Option<Key>,
        peer_id: Option<Key>,
    },
    Scrape {
        /// `None`: all swarms. Otherwise the requested hashes in order (may be empty).
        info_hashes: Option<SmallVec<[Cow<'a, [u8]>; 4]>>,
    },
}

impl Message<'_> {
    /// The `info_hash` that decides which shard owns the message. `None` for scrapes (they
    /// may span shards), and for stops and answers that cannot match.
    pub fn route_info_hash(&self) -> Option<Key> {
        match self {
            Message::Announce { info_hash, .. } => Some(*info_hash),
            Message::Answer { info_hash, .. } | Message::Stop { info_hash, .. } => *info_hash,
            Message::Scrape { .. } => None,
        }
    }
}

/// A JSON parsing backend.
pub trait Backend {
    fn parse(frame: &[u8]) -> Result<Message<'_>, ProtoError>;
}

/// Raw JSON text of the fields the tracker reads, borrowed from the frame.
#[derive(Default)]
struct Fields<'a> {
    action: Option<&'a [u8]>,
    event: Option<&'a [u8]>,
    info_hash: Option<&'a [u8]>,
    peer_id: Option<&'a [u8]>,
    to_peer_id: Option<&'a [u8]>,
    to_peer_id_count: u32,
    answer: Option<&'a [u8]>,
    numwant: Option<&'a [u8]>,
    left: Option<&'a [u8]>,
    offers: Option<Offers<'a>>,
}

#[allow(clippy::large_enum_variant)] // inline on purpose: no allocation per message
enum Offers<'a> {
    NotArray,
    Items(SmallVec<[Item<'a>; INLINE_OFFERS]>),
}

/// One offer item. `valid`: an object whose `offer` is an object or array (JS `typeof
/// "object"`, not `null`).
#[derive(Default)]
struct Item<'a> {
    valid: bool,
    offer_id: Option<&'a [u8]>,
    sdp: Option<&'a [u8]>,
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum FieldName {
    Action,
    Event,
    InfoHash,
    PeerId,
    ToPeerId,
    Answer,
    Numwant,
    Left,
    Offers,
    #[serde(other)]
    Other,
}

impl FieldName {
    #[cfg(feature = "sonic")]
    fn from_key(key: &str) -> Self {
        match key {
            "action" => Self::Action,
            "event" => Self::Event,
            "info_hash" => Self::InfoHash,
            "peer_id" => Self::PeerId,
            "to_peer_id" => Self::ToPeerId,
            "answer" => Self::Answer,
            "numwant" => Self::Numwant,
            "left" => Self::Left,
            "offers" => Self::Offers,
            _ => Self::Other,
        }
    }
}

impl<'a> Fields<'a> {
    /// Stores a scalar field (everything but `offers`).
    fn set(&mut self, name: FieldName, raw: &'a [u8]) {
        let slot = match name {
            FieldName::Action => &mut self.action,
            FieldName::Event => &mut self.event,
            FieldName::InfoHash => &mut self.info_hash,
            FieldName::PeerId => &mut self.peer_id,
            FieldName::ToPeerId => {
                self.to_peer_id_count += 1;
                &mut self.to_peer_id
            }
            FieldName::Answer => &mut self.answer,
            FieldName::Numwant => &mut self.numwant,
            FieldName::Left => &mut self.left,
            FieldName::Offers | FieldName::Other => return,
        };
        *slot = Some(raw);
    }
}

/// The top level must be an object. (Backends would otherwise read e.g. a JSON array
/// positionally.) Non-objects are classified on this cold path only.
fn require_object(frame: &[u8]) -> Result<(), ProtoError> {
    if frame.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'{') {
        return Ok(());
    }
    Err(match serde_json::from_slice::<IgnoredAny>(frame) {
        Ok(_) => ProtoError::NotAnObject,
        Err(_) => ProtoError::InvalidJson,
    })
}

// ---- serde_json backend ----

/// `serde_json`: scalar parsing, always available. Raw values via `&RawValue`.
pub struct SerdeJson;

impl Backend for SerdeJson {
    fn parse(frame: &[u8]) -> Result<Message<'_>, ProtoError> {
        // serde_json does not UTF-8-validate skipped values, but unknown members are forwarded
        // verbatim (answers): validate the whole frame.
        std::str::from_utf8(frame).map_err(|_| ProtoError::InvalidJson)?;
        require_object(frame)?;
        let FieldsDe(fields) =
            serde_json::from_slice(frame).map_err(|_| ProtoError::InvalidJson)?;
        interpret(frame, fields)
    }
}

struct FieldsDe<'a>(Fields<'a>);
struct OffersDe<'a>(Offers<'a>);
struct ItemDe<'a>(Item<'a>);
/// The `offer` member of an item: `(valid, sdp)`.
struct OfferDe<'a>(bool, Option<&'a [u8]>);

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum ItemField {
    Offer,
    OfferId,
    #[serde(other)]
    Other,
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum OfferField {
    Sdp,
    #[serde(other)]
    Other,
}

fn raw_slice<'de, A: MapAccess<'de>>(map: &mut A) -> Result<&'de [u8], A::Error> {
    Ok(map.next_value::<&'de RawValue>()?.get().as_bytes())
}

/// Visitor accepting any JSON value; the impl for `T` decides what each shape means.
struct Any<T>(PhantomData<T>);

macro_rules! scalars_are {
    ($value:expr) => {
        fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok($value)
        }
    };
}

fn drain_seq<'de, A: SeqAccess<'de>>(mut seq: A) -> Result<(), A::Error> {
    while seq.next_element::<IgnoredAny>()?.is_some() {}
    Ok(())
}

fn drain_map<'de, A: MapAccess<'de>>(mut map: A) -> Result<(), A::Error> {
    while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
    Ok(())
}

impl<'de> Deserialize<'de> for FieldsDe<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Top;
        impl<'de> Visitor<'de> for Top {
            type Value = FieldsDe<'de>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut fields = Fields::default();
                while let Some(name) = map.next_key::<FieldName>()? {
                    match name {
                        FieldName::Offers => {
                            fields.offers = Some(map.next_value::<OffersDe<'de>>()?.0)
                        }
                        FieldName::Other => {
                            map.next_value::<IgnoredAny>()?;
                        }
                        name => {
                            let raw = raw_slice(&mut map)?;
                            fields.set(name, raw);
                        }
                    }
                }
                Ok(FieldsDe(fields))
            }
        }
        d.deserialize_map(Top)
    }
}

impl<'de> Visitor<'de> for Any<OffersDe<'de>> {
    type Value = OffersDe<'de>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    scalars_are!(OffersDe(Offers::NotArray));
    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        drain_map(map)?;
        Ok(OffersDe(Offers::NotArray))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut items = SmallVec::new();
        while let Some(ItemDe(item)) = seq.next_element::<ItemDe<'de>>()? {
            items.push(item);
        }
        Ok(OffersDe(Offers::Items(items)))
    }
}

impl<'de> Deserialize<'de> for OffersDe<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(Any::<OffersDe<'de>>(PhantomData))
    }
}

impl<'de> Visitor<'de> for Any<ItemDe<'de>> {
    type Value = ItemDe<'de>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    scalars_are!(ItemDe(Item::default()));
    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        // An array is `typeof "object"` in JS, but has no `offer` member.
        drain_seq(seq)?;
        Ok(ItemDe(Item::default()))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut item = Item::default();
        while let Some(field) = map.next_key::<ItemField>()? {
            match field {
                ItemField::Offer => {
                    let OfferDe(valid, sdp) = map.next_value()?;
                    item.valid = valid;
                    item.sdp = sdp;
                }
                ItemField::OfferId => item.offer_id = Some(raw_slice(&mut map)?),
                ItemField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(ItemDe(item))
    }
}

impl<'de> Deserialize<'de> for ItemDe<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(Any::<ItemDe<'de>>(PhantomData))
    }
}

impl<'de> Visitor<'de> for Any<OfferDe<'de>> {
    type Value = OfferDe<'de>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    scalars_are!(OfferDe(false, None));
    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        drain_seq(seq)?;
        Ok(OfferDe(true, None))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut sdp = None;
        while let Some(field) = map.next_key::<OfferField>()? {
            match field {
                OfferField::Sdp => sdp = Some(raw_slice(&mut map)?),
                OfferField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(OfferDe(true, sdp))
    }
}

impl<'de> Deserialize<'de> for OfferDe<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(Any::<OfferDe<'de>>(PhantomData))
    }
}

// ---- sonic-rs backend ----

/// `sonic-rs`: SIMD parsing (NEON / AVX2) with its lazy, validating object/array iterators,
/// which borrow raw values from the frame even when they contain escapes.
#[cfg(feature = "sonic")]
pub struct Sonic;

#[cfg(feature = "sonic")]
mod sonic {
    use super::*;
    use sonic_rs::LazyValue;

    fn slice<'a>(value: &LazyValue<'a>) -> Result<&'a [u8], ProtoError> {
        match value.as_raw_cow() {
            Cow::Borrowed(s) => Ok(s.as_bytes()),
            Cow::Owned(_) => Err(ProtoError::InvalidJson),
        }
    }

    /// Calls `f(key, raw value)` for every member of the object `raw`.
    fn members<'a>(
        raw: &'a [u8],
        mut f: impl FnMut(&str, &'a [u8]) -> Result<(), ProtoError>,
    ) -> Result<(), ProtoError> {
        for member in sonic_rs::to_object_iter(raw) {
            let (key, value) = member.map_err(|_| ProtoError::InvalidJson)?;
            f(&key, slice(&value)?)?;
        }
        Ok(())
    }

    fn offers(raw: &[u8]) -> Result<Offers<'_>, ProtoError> {
        if raw.first() != Some(&b'[') {
            return Ok(Offers::NotArray);
        }
        let mut items = SmallVec::new();
        for element in sonic_rs::to_array_iter(raw) {
            let element = slice(&element.map_err(|_| ProtoError::InvalidJson)?)?;
            let mut item = Item::default();
            if element.first() == Some(&b'{') {
                members(element, |key, value| {
                    match key {
                        "offer" => {
                            item.valid = matches!(value.first(), Some(b'{' | b'['));
                            item.sdp = None;
                            if value.first() == Some(&b'{') {
                                members(value, |key, value| {
                                    if key == "sdp" {
                                        item.sdp = Some(value);
                                    }
                                    Ok(())
                                })?;
                            }
                        }
                        "offer_id" => item.offer_id = Some(value),
                        _ => {}
                    }
                    Ok(())
                })?;
            }
            items.push(item);
        }
        Ok(Offers::Items(items))
    }

    impl Backend for Sonic {
        fn parse(frame: &[u8]) -> Result<Message<'_>, ProtoError> {
            require_object(frame)?;
            let mut fields = Fields::default();
            // End of the last member value (or of the opening `{`).
            let mut end = frame
                .iter()
                .position(|&b| b == b'{')
                .expect("require_object")
                + 1;
            members(frame, |key, value| {
                end = value.as_ptr() as usize - frame.as_ptr() as usize + value.len();
                match FieldName::from_key(key) {
                    FieldName::Offers => fields.offers = Some(offers(value)?),
                    name => fields.set(name, value),
                }
                Ok(())
            })?;
            // The iterator stops at the closing `}`; like JSON.parse, allow only whitespace
            // after it.
            let mut rest = frame[end..].iter().skip_while(|b| b.is_ascii_whitespace());
            if rest.next() != Some(&b'}') || !rest.all(|b| b.is_ascii_whitespace()) {
                return Err(ProtoError::InvalidJson);
            }
            interpret(frame, fields)
        }
    }
}

// ---- JS FastTracker rules ----

/// A required string id: must be a JSON string that fits in a [`Key`].
fn required_key(raw: Option<&[u8]>, field: &'static str) -> Result<Key, ProtoError> {
    match raw {
        Some(raw) if is_string(raw) => match decode_key(raw) {
            Ok(Some(key)) => Ok(key),
            Ok(None) => Err(ProtoError::KeyTooLong),
            Err(_) => Err(ProtoError::BadField(field)),
        },
        _ => Err(ProtoError::BadField(field)),
    }
}

/// A lookup-only id: anything that cannot name an existing peer or swarm (not a string, too
/// long, lone surrogate) is `None`.
fn lookup_key(raw: Option<&[u8]>) -> Option<Key> {
    raw.filter(|r| is_string(r))
        .and_then(|r| decode_key(r).ok().flatten())
}

fn interpret<'a>(frame: &'a [u8], f: Fields<'a>) -> Result<Message<'a>, ProtoError> {
    let action = f
        .action
        .filter(|a| is_string(a))
        .ok_or(ProtoError::UnknownAction)?;
    if string_equals(action, "scrape") {
        return scrape(f.info_hash);
    }
    if !string_equals(action, "announce") {
        return Err(ProtoError::UnknownAction);
    }

    let event = match f.event {
        None => AnnounceEvent::None,
        Some(e) if is_string(e) && string_equals(e, "started") => AnnounceEvent::Started,
        Some(e) if is_string(e) && string_equals(e, "completed") => AnnounceEvent::Completed,
        Some(e) if is_string(e) && string_equals(e, "stopped") => {
            if !f.peer_id.is_some_and(is_string) {
                return Err(ProtoError::BadField("peer_id"));
            }
            return Ok(Message::Stop {
                info_hash: lookup_key(f.info_hash),
                peer_id: lookup_key(f.peer_id),
            });
        }
        Some(_) => return Err(ProtoError::UnknownEvent),
    };

    // Any `answer` value, even `null`, makes it an answer (JS checks `!== undefined`).
    if event == AnnounceEvent::None && f.answer.is_some() {
        let to_peer_id = required_key(f.to_peer_id, "to_peer_id")?;
        if !f.peer_id.is_some_and(is_string) {
            return Err(ProtoError::BadField("peer_id"));
        }
        if f.to_peer_id_count > 1 {
            // JS drops every copy; cutting one would forward the other.
            return Err(ProtoError::BadField("to_peer_id"));
        }
        // Required to check the swarm (spec §5.3; JS does not check it).
        if !f.info_hash.is_some_and(is_string) {
            return Err(ProtoError::BadField("info_hash"));
        }
        return Ok(Message::Answer {
            info_hash: lookup_key(f.info_hash),
            peer_id: lookup_key(f.peer_id),
            to_peer_id,
            answer: cut_member(frame, f.to_peer_id.expect("required_key checked it"))?,
        });
    }

    let peer_id = required_key(f.peer_id, "peer_id")?;
    let info_hash = required_key(f.info_hash, "info_hash")?;

    let numwant = f.numwant.and_then(number).and_then(|n| {
        // JS `Number.isInteger`; a negative count sends no offers.
        (n.is_finite() && n.fract() == 0.0).then(|| n.clamp(0.0, u32::MAX as f64) as u32)
    });
    let left_zero = f.left.and_then(number) == Some(0.0);

    let offers = match f.offers {
        None => None,
        Some(Offers::NotArray) => return Err(ProtoError::BadField("offers")),
        Some(Offers::Items(items)) => {
            let mut offers = SmallVec::new();
            for item in &items {
                if !item.valid {
                    return Err(ProtoError::BadField("offers"));
                }
                offers.push(Payload::Offer {
                    offer_id: item.offer_id,
                    sdp: item.sdp,
                });
            }
            Some(offers)
        }
    };

    Ok(Message::Announce {
        info_hash,
        peer_id,
        event,
        left_zero,
        numwant,
        offers,
    })
}

fn scrape(info_hash: Option<&[u8]>) -> Result<Message<'_>, ProtoError> {
    let decode = |raw| decode_cow(raw).map_err(|_| ProtoError::BadField("info_hash"));
    let info_hashes = match info_hash {
        None => None,
        Some(raw) if is_string(raw) => Some(SmallVec::from_iter([decode(raw)?])),
        Some(raw) if raw.first() == Some(&b'[') => {
            // Rare: re-read the array; non-string elements are skipped (JS semantics).
            let items: Vec<&RawValue> =
                serde_json::from_slice(raw).map_err(|_| ProtoError::InvalidJson)?;
            let mut hashes = SmallVec::new();
            for item in items {
                let item = item.get().as_bytes();
                if is_string(item) {
                    // Re-borrow from `raw` (the element borrows from `raw`) to keep lifetime 'a.
                    let offset = item.as_ptr() as usize - raw.as_ptr() as usize;
                    hashes.push(decode(&raw[offset..offset + item.len()])?);
                }
            }
            Some(hashes)
        }
        // Numbers, null, objects: JS produces an empty `files` object.
        Some(_) => Some(SmallVec::new()),
    };
    Ok(Message::Scrape { info_hashes })
}

/// The frame with the `to_peer_id` member removed, as two slices (no copy). `value` is the
/// raw value slice of `to_peer_id` inside `frame`.
fn cut_member<'a>(frame: &'a [u8], value: &'a [u8]) -> Result<Payload<'a>, ProtoError> {
    const KEY: &[u8] = b"\"to_peer_id\"";
    let unsupported = ProtoError::BadField("to_peer_id");
    let value_start = value.as_ptr() as usize - frame.as_ptr() as usize;
    let value_end = value_start + value.len();
    let skip_ws_back = |mut i: usize| {
        while i > 0 && frame[i - 1].is_ascii_whitespace() {
            i -= 1;
        }
        i
    };

    // Back from the value: whitespace, ':', whitespace, then the literal key.
    let colon = skip_ws_back(value_start);
    if colon == 0 || frame[colon - 1] != b':' {
        return Err(unsupported);
    }
    let key_end = skip_ws_back(colon - 1);
    if key_end < KEY.len() || &frame[key_end - KEY.len()..key_end] != KEY {
        // The key was spelled with escapes: not produced by JSON.stringify.
        return Err(unsupported);
    }
    let before = skip_ws_back(key_end - KEY.len());
    match before.checked_sub(1).map(|i| frame[i]) {
        // `…, "to_peer_id": v …` → drop from the comma through the value.
        Some(b',') => Ok(Payload::Answer {
            head: &frame[..before - 1],
            tail: &frame[value_end..],
        }),
        // `{ "to_peer_id": v, …` → drop the member and the comma after it.
        Some(b'{') => {
            let mut j = value_end;
            while j < frame.len() && frame[j].is_ascii_whitespace() {
                j += 1;
            }
            if frame.get(j) == Some(&b',') {
                j += 1;
            }
            Ok(Payload::Answer {
                head: &frame[..before],
                tail: &frame[j..],
            })
        }
        _ => Err(unsupported),
    }
}
