//! PVA BitSet — compact bit array for delta encoding.
//!
//! Used by CMD_MONITOR responses to indicate which fields of a
//! structure have changed since the last update. Each bit corresponds
//! to one field in the structure's flattened field tree.
//!
//! ## Wire format (from PVA spec)
//!
//! Encoded as: `size` (PVA size encoding) + `size` raw bytes.
//! Bits are packed LSB-first within each byte. Trailing zero bytes
//! are omitted (serialization is size-optimized).
//!
//! ```text
//! {0}         -> 01 01           (1 byte: bit 0 set)
//! {7}         -> 01 80           (1 byte: bit 7 set)
//! {8}         -> 02 00 01        (2 bytes: bit 8 set in byte 1)
//! {0,1,2,4}   -> 01 17           (1 byte: 0b00010111)
//! {}          -> 00              (0 bytes: empty)
//! ```

use super::pvdata::{DecodeError, PvaReader, PvaWriter};
use std::fmt;
use std::ops::{BitAnd, BitOr, BitOrAssign};

const INLINE_WORDS: usize = 2;
const INLINE_BITS: usize = INLINE_WORDS * 64;

/// Compact bit set with inline storage for small structures.
#[derive(Clone, PartialEq, Eq)]
pub struct PvaBitSet {
    inline: [u64; INLINE_WORDS],
    overflow: Vec<u64>,
    bit_count: usize,
}

impl PvaBitSet {
    pub fn new() -> Self {
        Self {
            inline: [0; INLINE_WORDS],
            overflow: Vec::new(),
            bit_count: 0,
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        let mut bs = Self::new();
        bs.bit_count = n;
        if n > INLINE_BITS {
            let extra = (n - INLINE_BITS).div_ceil(64);
            bs.overflow = vec![0u64; extra];
        }
        bs
    }

    pub fn from_bits(bits: &[usize]) -> Self {
        if bits.is_empty() {
            return Self::new();
        }
        let max = bits.iter().copied().max().unwrap_or(0);
        let mut bs = Self::with_capacity(max + 1);
        for &b in bits {
            bs.set(b);
        }
        bs
    }

    /// Create with all bits 0..n set using full-word masks.
    pub fn all_set(n: usize) -> Self {
        let mut bs = Self::with_capacity(n);
        if n == 0 {
            return bs;
        }

        let full_words = n / 64;
        let remainder = n % 64;

        for i in 0..full_words.min(INLINE_WORDS) {
            bs.inline[i] = u64::MAX;
        }
        for i in INLINE_WORDS..full_words {
            let ow = i - INLINE_WORDS;
            if ow < bs.overflow.len() {
                bs.overflow[ow] = u64::MAX;
            }
        }
        if remainder > 0 {
            let mask = (1u64 << remainder) - 1;
            let word_idx = full_words;
            if word_idx < INLINE_WORDS {
                bs.inline[word_idx] = mask;
            } else {
                let ow = word_idx - INLINE_WORDS;
                if ow < bs.overflow.len() {
                    bs.overflow[ow] = mask;
                }
            }
        }
        bs
    }

    #[inline]
    pub fn is_set(&self, index: usize) -> bool {
        let (word, mask) = Self::word_mask(index);
        if index < INLINE_BITS {
            self.inline[word] & mask != 0
        } else {
            let ow = word - INLINE_WORDS;
            ow < self.overflow.len() && self.overflow[ow] & mask != 0
        }
    }

    #[inline]
    pub fn set(&mut self, index: usize) {
        let (word, mask) = Self::word_mask(index);
        if index < INLINE_BITS {
            self.inline[word] |= mask;
        } else {
            let ow = word - INLINE_WORDS;
            if ow >= self.overflow.len() {
                self.overflow.resize(ow + 1, 0);
            }
            self.overflow[ow] |= mask;
        }
        if index >= self.bit_count {
            self.bit_count = index + 1;
        }
    }

    #[inline]
    pub fn clear_bit(&mut self, index: usize) {
        let (word, mask) = Self::word_mask(index);
        if index < INLINE_BITS {
            self.inline[word] &= !mask;
        } else {
            let ow = word - INLINE_WORDS;
            if ow < self.overflow.len() {
                self.overflow[ow] &= !mask;
            }
        }
    }

    pub fn clear_all(&mut self) {
        self.inline = [0; INLINE_WORDS];
        for w in &mut self.overflow {
            *w = 0;
        }
    }

    /// Toggle bit (set if clear, clear if set).
    #[inline]
    pub fn toggle(&mut self, index: usize) {
        let (word, mask) = Self::word_mask(index);
        if index < INLINE_BITS {
            self.inline[word] ^= mask;
        } else {
            let ow = word - INLINE_WORDS;
            if ow >= self.overflow.len() {
                self.overflow.resize(ow + 1, 0);
            }
            self.overflow[ow] ^= mask;
        }
        if index >= self.bit_count {
            self.bit_count = index + 1;
        }
    }

    pub fn count_set(&self) -> usize {
        self.inline
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum::<usize>()
            + self
                .overflow
                .iter()
                .map(|w| w.count_ones() as usize)
                .sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.inline.iter().all(|&w| w == 0) && self.overflow.iter().all(|&w| w == 0)
    }

    #[inline]
    pub fn bit_count(&self) -> usize {
        self.bit_count
    }

    pub fn iter_set(&self) -> impl Iterator<Item = usize> + '_ {
        self.inline
            .iter()
            .enumerate()
            .flat_map(|(wi, &word)| BitWordIter::new(word, wi * 64))
            .chain(
                self.overflow
                    .iter()
                    .enumerate()
                    .flat_map(|(wi, &word)| BitWordIter::new(word, INLINE_BITS + wi * 64)),
            )
    }

    /// Collect set bits into a Vec.
    pub fn to_vec(&self) -> Vec<usize> {
        self.iter_set().collect()
    }

    pub fn decode(reader: &mut PvaReader<'_>) -> Result<Self, DecodeError> {
        let byte_count = reader.read_size_non_null()?;
        if byte_count == 0 {
            return Ok(Self::new());
        }

        let raw = reader.read_bytes(byte_count)?;
        let mut bs = Self::with_capacity(byte_count * 8);

        // Build words directly from raw bytes (8 bytes per u64, LE).
        let mut word_idx = 0;
        let mut byte_in_word = 0;
        let mut current_word = 0u64;

        for &byte_val in raw {
            current_word |= (byte_val as u64) << (byte_in_word * 8);
            byte_in_word += 1;
            if byte_in_word == 8 {
                if word_idx < INLINE_WORDS {
                    bs.inline[word_idx] = current_word;
                } else {
                    let ow = word_idx - INLINE_WORDS;
                    if ow < bs.overflow.len() {
                        bs.overflow[ow] = current_word;
                    }
                }
                word_idx += 1;
                byte_in_word = 0;
                current_word = 0;
            }
        }
        // Flush remaining partial word.
        if byte_in_word > 0 {
            if word_idx < INLINE_WORDS {
                bs.inline[word_idx] = current_word;
            } else {
                let ow = word_idx - INLINE_WORDS;
                if ow < bs.overflow.len() {
                    bs.overflow[ow] = current_word;
                }
            }
        }

        Ok(bs)
    }

    /// Single-pass encode: extract bytes from u64 words directly.
    pub fn encode(&self, writer: &mut PvaWriter) {
        // Find the last non-zero word.
        let total_words = INLINE_WORDS + self.overflow.len();
        let mut last_word = 0usize; // exclusive
        for i in (0..total_words).rev() {
            let w = self.get_word(i);
            if w != 0 {
                last_word = i + 1;
                break;
            }
        }

        if last_word == 0 {
            writer.write_size(0);
            return;
        }

        // Find last non-zero byte in the last non-zero word.
        let last_w = self.get_word(last_word - 1);
        let bytes_in_last = if last_w == 0 {
            0
        } else {
            8 - (last_w.leading_zeros() / 8) as usize
        };
        let total_bytes = (last_word - 1) * 8 + bytes_in_last;

        writer.write_size(total_bytes);

        // Write bytes from each word.
        for wi in 0..last_word {
            let w = self.get_word(wi);
            let bytes_to_write = if wi == last_word - 1 {
                bytes_in_last
            } else {
                8
            };
            for bi in 0..bytes_to_write {
                writer.write_u8(((w >> (bi * 8)) & 0xFF) as u8);
            }
        }
    }

    #[inline]
    fn word_mask(index: usize) -> (usize, u64) {
        (index / 64, 1u64 << (index % 64))
    }

    #[inline]
    fn get_word(&self, idx: usize) -> u64 {
        if idx < INLINE_WORDS {
            self.inline[idx]
        } else {
            let ow = idx - INLINE_WORDS;
            if ow < self.overflow.len() {
                self.overflow[ow]
            } else {
                0
            }
        }
    }
}

impl BitOr for &PvaBitSet {
    type Output = PvaBitSet;
    fn bitor(self, rhs: Self) -> PvaBitSet {
        let mut result = self.clone();
        result |= rhs.clone();
        result
    }
}

impl BitOrAssign for PvaBitSet {
    fn bitor_assign(&mut self, rhs: Self) {
        for i in 0..INLINE_WORDS {
            self.inline[i] |= rhs.inline[i];
        }
        let max_ow = self.overflow.len().max(rhs.overflow.len());
        self.overflow.resize(max_ow, 0);
        for (i, &w) in rhs.overflow.iter().enumerate() {
            self.overflow[i] |= w;
        }
        self.bit_count = self.bit_count.max(rhs.bit_count);
    }
}

impl BitAnd for &PvaBitSet {
    type Output = PvaBitSet;
    fn bitand(self, rhs: Self) -> PvaBitSet {
        let mut result = PvaBitSet::with_capacity(self.bit_count.min(rhs.bit_count));
        for i in 0..INLINE_WORDS {
            result.inline[i] = self.inline[i] & rhs.inline[i];
        }
        let min_ow = self.overflow.len().min(rhs.overflow.len());
        result.overflow.resize(min_ow, 0);
        for i in 0..min_ow {
            result.overflow[i] = self.overflow[i] & rhs.overflow[i];
        }
        result
    }
}

impl Default for PvaBitSet {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PvaBitSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PvaBitSet({:?})", self.to_vec())
    }
}

impl fmt::Display for PvaBitSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{} bits set}}", self.count_set())
    }
}

struct BitWordIter {
    word: u64,
    base: usize,
}

impl BitWordIter {
    fn new(word: u64, base: usize) -> Self {
        Self { word, base }
    }
}

impl Iterator for BitWordIter {
    type Item = usize;
    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.word == 0 {
            return None;
        }
        let tz = self.word.trailing_zeros() as usize;
        self.word &= self.word - 1;
        Some(self.base + tz)
    }
}

#[cfg(test)]
mod tests {
    use super::super::header::ByteOrder;
    use super::*;

    fn le_w() -> PvaWriter {
        PvaWriter::new(ByteOrder::LittleEndian)
    }
    fn le_r(d: &[u8]) -> PvaReader<'_> {
        PvaReader::new(d, ByteOrder::LittleEndian)
    }
    fn be_w() -> PvaWriter {
        PvaWriter::new(ByteOrder::BigEndian)
    }
    fn be_r(d: &[u8]) -> PvaReader<'_> {
        PvaReader::new(d, ByteOrder::BigEndian)
    }

    #[test]
    fn test_new() {
        let bs = PvaBitSet::new();
        assert!(bs.is_empty());
        assert_eq!(bs.count_set(), 0);
        assert_eq!(bs.bit_count(), 0);
    }
    #[test]
    fn test_with_capacity_inline() {
        let bs = PvaBitSet::with_capacity(64);
        assert!(bs.overflow.is_empty());
    }
    #[test]
    fn test_with_capacity_128() {
        let bs = PvaBitSet::with_capacity(128);
        assert!(bs.overflow.is_empty());
    }
    #[test]
    fn test_with_capacity_overflow() {
        let bs = PvaBitSet::with_capacity(200);
        assert!(!bs.overflow.is_empty());
        assert_eq!(bs.bit_count(), 200);
    }
    #[test]
    fn test_with_capacity_zero() {
        let bs = PvaBitSet::with_capacity(0);
        assert!(bs.is_empty());
    }
    #[test]
    fn test_from_bits() {
        let bs = PvaBitSet::from_bits(&[0, 3, 7]);
        assert!(bs.is_set(0));
        assert!(bs.is_set(3));
        assert!(bs.is_set(7));
        assert!(!bs.is_set(1));
        assert_eq!(bs.count_set(), 3);
    }
    #[test]
    fn test_from_bits_empty() {
        assert!(PvaBitSet::from_bits(&[]).is_empty());
    }
    #[test]
    fn test_from_bits_single() {
        let bs = PvaBitSet::from_bits(&[42]);
        assert_eq!(bs.count_set(), 1);
        assert!(bs.is_set(42));
    }

    #[test]
    fn test_all_set_10() {
        let bs = PvaBitSet::all_set(10);
        assert_eq!(bs.count_set(), 10);
        for i in 0..10 {
            assert!(bs.is_set(i));
        }
        assert!(!bs.is_set(10));
    }
    #[test]
    fn test_all_set_0() {
        assert!(PvaBitSet::all_set(0).is_empty());
    }
    #[test]
    fn test_all_set_1() {
        let bs = PvaBitSet::all_set(1);
        assert_eq!(bs.count_set(), 1);
        assert!(bs.is_set(0));
        assert!(!bs.is_set(1));
    }
    #[test]
    fn test_all_set_64() {
        let bs = PvaBitSet::all_set(64);
        assert_eq!(bs.count_set(), 64);
        assert_eq!(bs.inline[0], u64::MAX);
        assert_eq!(bs.inline[1], 0);
    }
    #[test]
    fn test_all_set_65() {
        let bs = PvaBitSet::all_set(65);
        assert_eq!(bs.count_set(), 65);
        assert_eq!(bs.inline[0], u64::MAX);
        assert_eq!(bs.inline[1], 1);
    }
    #[test]
    fn test_all_set_128() {
        let bs = PvaBitSet::all_set(128);
        assert_eq!(bs.count_set(), 128);
        assert_eq!(bs.inline[0], u64::MAX);
        assert_eq!(bs.inline[1], u64::MAX);
    }
    #[test]
    fn test_all_set_129() {
        let bs = PvaBitSet::all_set(129);
        assert_eq!(bs.count_set(), 129);
        assert!(bs.is_set(128));
        assert!(!bs.is_set(129));
    }
    #[test]
    fn test_all_set_200() {
        let bs = PvaBitSet::all_set(200);
        assert_eq!(bs.count_set(), 200);
    }

    #[test]
    fn test_set_get_inline() {
        let mut bs = PvaBitSet::new();
        for i in [0, 1, 63, 64, 127] {
            bs.set(i);
        }
        for i in [0, 1, 63, 64, 127] {
            assert!(bs.is_set(i), "bit {i}");
        }
        assert!(!bs.is_set(2));
        assert_eq!(bs.count_set(), 5);
    }
    #[test]
    fn test_set_get_overflow() {
        let mut bs = PvaBitSet::new();
        bs.set(128);
        bs.set(200);
        bs.set(500);
        assert!(bs.is_set(128));
        assert!(bs.is_set(500));
        assert!(!bs.is_set(129));
        assert_eq!(bs.count_set(), 3);
    }
    #[test]
    fn test_set_updates_bit_count() {
        let mut bs = PvaBitSet::new();
        bs.set(42);
        assert_eq!(bs.bit_count(), 43);
        bs.set(100);
        assert_eq!(bs.bit_count(), 101);
        bs.set(10);
        assert_eq!(bs.bit_count(), 101); // doesn't shrink
    }
    #[test]
    fn test_clear_bit() {
        let mut bs = PvaBitSet::from_bits(&[0, 1, 2]);
        bs.clear_bit(1);
        assert!(bs.is_set(0));
        assert!(!bs.is_set(1));
        assert!(bs.is_set(2));
    }
    #[test]
    fn test_clear_bit_overflow() {
        let mut bs = PvaBitSet::from_bits(&[0, 200]);
        bs.clear_bit(200);
        assert!(!bs.is_set(200));
        assert!(bs.is_set(0));
    }
    #[test]
    fn test_clear_all() {
        let mut bs = PvaBitSet::from_bits(&[0, 50, 100, 200]);
        bs.clear_all();
        assert!(bs.is_empty());
        assert_eq!(bs.count_set(), 0);
    }
    #[test]
    fn test_is_set_out_of_range() {
        assert!(!PvaBitSet::from_bits(&[0]).is_set(1000));
    }
    #[test]
    fn test_clear_out_of_range() {
        let mut bs = PvaBitSet::from_bits(&[0]);
        bs.clear_bit(1000);
        assert!(bs.is_set(0));
    }

    #[test]
    fn test_toggle_set() {
        let mut bs = PvaBitSet::new();
        bs.toggle(5);
        assert!(bs.is_set(5));
    }
    #[test]
    fn test_toggle_clear() {
        let mut bs = PvaBitSet::from_bits(&[5]);
        bs.toggle(5);
        assert!(!bs.is_set(5));
    }
    #[test]
    fn test_toggle_overflow() {
        let mut bs = PvaBitSet::new();
        bs.toggle(200);
        assert!(bs.is_set(200));
        bs.toggle(200);
        assert!(!bs.is_set(200));
    }
    #[test]
    fn test_toggle_double() {
        let mut bs = PvaBitSet::new();
        bs.toggle(42);
        bs.toggle(42);
        assert!(!bs.is_set(42));
    }

    #[test]
    fn test_iter_empty() {
        assert_eq!(PvaBitSet::new().iter_set().count(), 0);
    }
    #[test]
    fn test_iter_single() {
        assert_eq!(PvaBitSet::from_bits(&[42]).to_vec(), vec![42]);
    }
    #[test]
    fn test_iter_multiple() {
        assert_eq!(
            PvaBitSet::from_bits(&[0, 1, 7, 8, 63, 64, 127]).to_vec(),
            vec![0, 1, 7, 8, 63, 64, 127]
        );
    }
    #[test]
    fn test_iter_overflow() {
        assert_eq!(
            PvaBitSet::from_bits(&[0, 128, 200]).to_vec(),
            vec![0, 128, 200]
        );
    }
    #[test]
    fn test_iter_ascending() {
        assert_eq!(
            PvaBitSet::from_bits(&[200, 0, 100, 50]).to_vec(),
            vec![0, 50, 100, 200]
        );
    }
    #[test]
    fn test_to_vec() {
        assert_eq!(PvaBitSet::all_set(5).to_vec(), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_bitor() {
        let a = PvaBitSet::from_bits(&[0, 2, 4]);
        let b = PvaBitSet::from_bits(&[1, 3, 5]);
        let c = &a | &b;
        assert_eq!(c.to_vec(), vec![0, 1, 2, 3, 4, 5]);
    }
    #[test]
    fn test_bitor_overlap() {
        let a = PvaBitSet::from_bits(&[0, 1, 2]);
        let b = PvaBitSet::from_bits(&[1, 2, 3]);
        assert_eq!((&a | &b).to_vec(), vec![0, 1, 2, 3]);
    }
    #[test]
    fn test_bitor_overflow() {
        let a = PvaBitSet::from_bits(&[0, 200]);
        let b = PvaBitSet::from_bits(&[100, 300]);
        let c = &a | &b;
        assert_eq!(c.to_vec(), vec![0, 100, 200, 300]);
    }
    #[test]
    fn test_bitor_assign() {
        let mut a = PvaBitSet::from_bits(&[0, 2]);
        a |= PvaBitSet::from_bits(&[1, 3]);
        assert_eq!(a.to_vec(), vec![0, 1, 2, 3]);
    }
    #[test]
    fn test_bitand() {
        let a = PvaBitSet::from_bits(&[0, 1, 2, 3]);
        let b = PvaBitSet::from_bits(&[1, 3, 5]);
        assert_eq!((&a & &b).to_vec(), vec![1, 3]);
    }
    #[test]
    fn test_bitand_empty() {
        let a = PvaBitSet::from_bits(&[0, 1]);
        let b = PvaBitSet::from_bits(&[2, 3]);
        assert!((&a & &b).is_empty());
    }
    #[test]
    fn test_bitand_overflow() {
        let a = PvaBitSet::from_bits(&[0, 200, 300]);
        let b = PvaBitSet::from_bits(&[200, 400]);
        assert_eq!((&a & &b).to_vec(), vec![200]);
    }

    #[test]
    fn test_encode_empty() {
        let mut w = le_w();
        PvaBitSet::new().encode(&mut w);
        assert_eq!(w.as_bytes(), &[0x00]);
    }
    #[test]
    fn test_encode_bit_0() {
        let mut w = le_w();
        PvaBitSet::from_bits(&[0]).encode(&mut w);
        assert_eq!(w.as_bytes(), &[0x01, 0x01]);
    }
    #[test]
    fn test_encode_bit_1() {
        let mut w = le_w();
        PvaBitSet::from_bits(&[1]).encode(&mut w);
        assert_eq!(w.as_bytes(), &[0x01, 0x02]);
    }
    #[test]
    fn test_encode_bit_7() {
        let mut w = le_w();
        PvaBitSet::from_bits(&[7]).encode(&mut w);
        assert_eq!(w.as_bytes(), &[0x01, 0x80]);
    }
    #[test]
    fn test_encode_bit_8() {
        let mut w = le_w();
        PvaBitSet::from_bits(&[8]).encode(&mut w);
        assert_eq!(w.as_bytes(), &[0x02, 0x00, 0x01]);
    }
    #[test]
    fn test_encode_0124() {
        let mut w = le_w();
        PvaBitSet::from_bits(&[0, 1, 2, 4]).encode(&mut w);
        assert_eq!(w.as_bytes(), &[0x01, 0x17]);
    }
    #[test]
    fn test_encode_01248() {
        let mut w = le_w();
        PvaBitSet::from_bits(&[0, 1, 2, 4, 8]).encode(&mut w);
        assert_eq!(w.as_bytes(), &[0x02, 0x17, 0x01]);
    }

    #[test]
    fn test_decode_empty() {
        assert!(PvaBitSet::decode(&mut le_r(&[0x00])).unwrap().is_empty());
    }
    #[test]
    fn test_decode_bit_0() {
        let bs = PvaBitSet::decode(&mut le_r(&[0x01, 0x01])).unwrap();
        assert!(bs.is_set(0));
        assert_eq!(bs.count_set(), 1);
    }
    #[test]
    fn test_decode_bit_7() {
        assert!(
            PvaBitSet::decode(&mut le_r(&[0x01, 0x80]))
                .unwrap()
                .is_set(7)
        );
    }
    #[test]
    fn test_decode_bit_8() {
        let bs = PvaBitSet::decode(&mut le_r(&[0x02, 0x00, 0x01])).unwrap();
        assert!(bs.is_set(8));
        assert!(!bs.is_set(0));
    }
    #[test]
    fn test_decode_complex() {
        assert_eq!(
            PvaBitSet::decode(&mut le_r(&[0x02, 0x17, 0x01]))
                .unwrap()
                .to_vec(),
            vec![0, 1, 2, 4, 8]
        );
    }
    #[test]
    fn test_decode_truncated() {
        assert!(PvaBitSet::decode(&mut le_r(&[0x05, 0x01])).is_err());
    }

    #[test]
    fn test_decode_empty_input() {
        assert!(PvaBitSet::decode(&mut le_r(&[])).is_err());
    }

    #[test]
    fn test_rt_empty() {
        let o = PvaBitSet::new();
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            PvaBitSet::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .count_set(),
            0
        );
    }

    #[test]
    fn test_rt_single_bits() {
        for bit in [0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 100, 127] {
            let o = PvaBitSet::from_bits(&[bit]);
            let mut w = le_w();
            o.encode(&mut w);
            let d = PvaBitSet::decode(&mut le_r(w.as_bytes())).unwrap();
            assert!(d.is_set(bit), "bit {bit}");
            assert_eq!(d.count_set(), 1);
        }
    }

    #[test]
    fn test_rt_multiple() {
        let bits = vec![0, 1, 2, 4, 8, 15, 16, 31, 32, 63, 64, 100, 127];
        let o = PvaBitSet::from_bits(&bits);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            PvaBitSet::decode(&mut le_r(w.as_bytes())).unwrap().to_vec(),
            bits
        );
    }

    #[test]
    fn test_rt_all_set_50() {
        let o = PvaBitSet::all_set(50);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            PvaBitSet::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .count_set(),
            50
        );
    }
    #[test]
    fn test_rt_all_set_128() {
        let o = PvaBitSet::all_set(128);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            PvaBitSet::decode(&mut le_r(w.as_bytes()))
                .unwrap()
                .count_set(),
            128
        );
    }

    #[test]
    fn test_rt_overflow() {
        let o = PvaBitSet::from_bits(&[0, 64, 128, 192]);
        let mut w = le_w();
        o.encode(&mut w);
        assert_eq!(
            PvaBitSet::decode(&mut le_r(w.as_bytes())).unwrap().to_vec(),
            vec![0, 64, 128, 192]
        );
    }

    #[test]
    fn test_rt_be() {
        // BitSet wire format is byte-level, byte order only affects size encoding.
        let o = PvaBitSet::from_bits(&[0, 7, 8]);
        let mut w = be_w();
        o.encode(&mut w);
        assert_eq!(
            PvaBitSet::decode(&mut be_r(w.as_bytes())).unwrap().to_vec(),
            vec![0, 7, 8]
        );
    }

    #[test]
    fn test_bwi_zero() {
        assert_eq!(BitWordIter::new(0, 0).next(), None);
    }
    #[test]
    fn test_bwi_one() {
        let mut it = BitWordIter::new(1, 0);
        assert_eq!(it.next(), Some(0));
        assert_eq!(it.next(), None);
    }
    #[test]
    fn test_bwi_multiple() {
        assert_eq!(
            BitWordIter::new(0b10010001, 100).collect::<Vec<_>>(),
            vec![100, 104, 107]
        );
    }
    #[test]
    fn test_bwi_all_set() {
        let v: Vec<_> = BitWordIter::new(u64::MAX, 0).collect();
        assert_eq!(v.len(), 64);
        assert_eq!(v[63], 63);
    }
    #[test]
    fn test_bwi_high_bit() {
        assert_eq!(
            BitWordIter::new(1u64 << 63, 0).collect::<Vec<_>>(),
            vec![63]
        );
    }

    #[test]
    fn test_clone() {
        let a = PvaBitSet::from_bits(&[0, 42, 100]);
        assert_eq!(a.clone(), a);
    }
    #[test]
    fn test_default() {
        assert!(PvaBitSet::default().is_empty());
    }
    #[test]
    fn test_display() {
        assert_eq!(PvaBitSet::from_bits(&[0, 1, 2]).to_string(), "{3 bits set}");
    }
    #[test]
    fn test_display_empty() {
        assert_eq!(PvaBitSet::new().to_string(), "{0 bits set}");
    }
    #[test]
    fn test_debug() {
        let d = format!("{:?}", PvaBitSet::from_bits(&[0, 7]));
        assert!(d.contains("0"));
        assert!(d.contains("7"));
    }
    #[test]
    fn test_eq() {
        assert_eq!(
            PvaBitSet::from_bits(&[0, 5, 10]),
            PvaBitSet::from_bits(&[0, 5, 10])
        );
    }
    #[test]
    fn test_ne() {
        assert_ne!(
            PvaBitSet::from_bits(&[0, 5, 10]),
            PvaBitSet::from_bits(&[0, 5, 11])
        );
    }

    #[test]
    fn test_boundary_127() {
        let bs = PvaBitSet::from_bits(&[127]);
        assert!(bs.is_set(127));
        assert!(bs.overflow.is_empty());
    }
    #[test]
    fn test_boundary_128() {
        let bs = PvaBitSet::from_bits(&[128]);
        assert!(bs.is_set(128));
        assert!(!bs.overflow.is_empty());
    }
    #[test]
    fn test_boundary_cross() {
        assert_eq!(
            PvaBitSet::from_bits(&[63, 64, 127, 128]).to_vec(),
            vec![63, 64, 127, 128]
        );
    }

    #[test]
    fn test_inline_size() {
        assert_eq!(INLINE_BITS, 128);
        assert!(std::mem::size_of::<PvaBitSet>() <= 64);
    }
}
