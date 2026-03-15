//! # Unofficial parser for SypexGeo (SxGeo) databases
//! A fast, zero-dependency parser for SypexGeo IP location databases (SxGeo City/Country).
//! Reads data directly from memory, minimizing allocations.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::fmt;

/// Internal headers of the SypexGeo database.
#[derive(Debug, Clone)]
pub struct SxGeoHeader {
    pub b_idx_len: usize,
    pub b_idx_offset: usize,
    pub block_len: usize,
    pub true_db_offset: usize,
    pub regions_offset: usize,
    pub cities_offset: usize,
    pub country_size: usize,
    pub country_bin_size: usize,
    pub country_str_count: usize,
    pub region_bin_size: usize,
    pub region_str_count: usize,
    pub city_bin_size: usize,
    pub city_str_count: usize,
}

/// Main structure representing the parsed SypexGeo database.
pub struct SxGeo {
    pub header: SxGeoHeader,
    data: Vec<u8>,
    countries_map: HashMap<u8, String>,
}

impl fmt::Debug for SxGeo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SxGeo")
            .field("header", &self.header)
            .field("data", &format_args!("<{} bytes>", self.data.len()))
            .field("countries_map", &format_args!("<{} countries>", self.countries_map.len()))
            .finish()
    }
}


impl SxGeo {
    /// Loads the SypexGeo database from a file into memory.
    ///
    /// # Example
    /// ```no_run
    /// use sxgeo_rs::SxGeo;
    /// let db = SxGeo::load("SxGeoCity.dat").expect("Failed to load database");
    /// ```
    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        if data.len() < 40 || &data[0..3] != b"SxG" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid database signature. Expected SxG.",
            ));
        }

        let b_idx_len = data[10] as usize;
        let m_idx_len = u16::from_be_bytes(data[11..13].try_into().unwrap()) as usize;
        let db_items = u32::from_be_bytes(data[15..19].try_into().unwrap()) as usize;
        let id_len = data[19] as usize;
        let region_size = u32::from_be_bytes(data[24..28].try_into().unwrap()) as usize;

        let country_size = u32::from_be_bytes(data[34..38].try_into().unwrap()) as usize;
        let pack_size = u16::from_be_bytes(data[38..40].try_into().unwrap()) as usize;

        let block_len = 3 + id_len;
        let b_idx_offset = 40 + pack_size;

        let true_db_offset = b_idx_offset + (b_idx_len * 4) + (m_idx_len * 4);
        let strings_base = true_db_offset + (db_items * block_len);

        let formats_chunk = &data[40..40 + pack_size];
        let raw_formats: Vec<&[u8]> = formats_chunk.split(|&b| b == 0).collect();

        let parse_fmt = |fmt: &[u8]| -> (usize, usize) {
            let s = std::str::from_utf8(fmt).unwrap_or("");
            let mut b_size = 0;
            let mut s_cnt = 0;
            for part in s.split('/') {
                if part.is_empty() {
                    continue;
                }
                let mut chars = part.chars();
                if let Some(c) = chars.next() {
                    let mut num_str = String::new();
                    for ch in chars.take_while(|ch| ch.is_ascii_digit()) {
                        num_str.push(ch);
                    }
                    let num = num_str.parse::<usize>().unwrap_or(0);

                    match c {
                        't' | 'T' | 'c' | 'C' => b_size += if num > 0 { num } else { 1 },
                        's' | 'S' | 'n' => b_size += 2,
                        'm' | 'M' => b_size += 3,
                        'i' | 'I' | 'f' | 'N' => b_size += 4,
                        'd' => b_size += 8,
                        'b' => s_cnt += 1,
                        _ => {}
                    }
                }
            }
            (b_size, s_cnt)
        };

        let (country_bin_size, country_str_count) = if !raw_formats.is_empty() {
            parse_fmt(raw_formats[0])
        } else {
            (0, 0)
        };
        let (region_bin_size, region_str_count) = if raw_formats.len() > 1 {
            parse_fmt(raw_formats[1])
        } else {
            (0, 0)
        };
        let (city_bin_size, city_str_count) = if raw_formats.len() > 2 {
            parse_fmt(raw_formats[2])
        } else {
            (0, 0)
        };

        let cities_offset = strings_base + region_size;

        // Preload countries into a fast HashMap
        let mut countries_map = HashMap::new();
        let mut c_offset = cities_offset;

        while c_offset < cities_offset + country_size {
            if c_offset >= data.len() {
                break;
            }
            let country_id = data[c_offset];

            let mut current = c_offset + country_bin_size;
            let mut str_parts = Vec::new();

            for _ in 0..country_str_count {
                if current >= data.len() {
                    break;
                }
                let mut end = current;
                while end < data.len() && data[end] != 0 {
                    end += 1;
                }
                if let Ok(text) = std::str::from_utf8(&data[current..end]) {
                    if !text.trim().is_empty() {
                        str_parts.push(text.trim().to_string());
                    }
                }
                current = end + 1;
            }

            // Prefer the localized name (usually the second to last in Sypex formats)
            if str_parts.len() >= 2 {
                countries_map.insert(country_id, str_parts[str_parts.len() - 2].clone());
            } else if !str_parts.is_empty() {
                countries_map.insert(country_id, str_parts[0].clone());
            }
            c_offset = current;
        }

        let header = SxGeoHeader {
            b_idx_len,
            b_idx_offset,
            block_len,
            true_db_offset,
            regions_offset: strings_base,
            cities_offset,
            country_size,
            country_bin_size,
            country_str_count,
            region_bin_size,
            region_str_count,
            city_bin_size,
            city_str_count,
        };

        Ok(Self {
            header,
            data,
            countries_map,
        })
    }

    /// Finds the internal ID for a given IP address.
    pub fn get_num(&self, ip_str: &str) -> Option<usize> {
        let ip: std::net::Ipv4Addr = ip_str.parse().ok()?;
        let octets = ip.octets();
        let first_byte = octets[0] as usize;
        let ip_tail = [octets[1], octets[2], octets[3]];

        if first_byte >= self.header.b_idx_len {
            return None;
        }

        let read_b_idx = |idx: usize| -> usize {
            let off = self.header.b_idx_offset + (idx * 4);
            if off + 4 > self.data.len() {
                return 0;
            }
            u32::from_be_bytes(self.data[off..off + 4].try_into().unwrap()) as usize
        };

        let min_idx = if first_byte > 0 {
            read_b_idx(first_byte - 1)
        } else {
            0
        };
        let max_idx = read_b_idx(first_byte);
        if max_idx <= min_idx {
            return None;
        }

        let mut min = min_idx;
        let mut max = max_idx;

        while (max - min) > 8 {
            let offset = min + (max - min) / 2;
            let mem_off = self.header.true_db_offset + (offset * self.header.block_len);
            if mem_off + 3 > self.data.len() {
                break;
            }
            if &ip_tail[..] > &self.data[mem_off..mem_off + 3] {
                min = offset;
            } else {
                max = offset;
            }
        }

        let mut best_match = min;
        while best_match < max {
            let mem_off = self.header.true_db_offset + (best_match * self.header.block_len);
            if mem_off + 3 > self.data.len() {
                break;
            }
            if &ip_tail[..] <= &self.data[mem_off..mem_off + 3] {
                break;
            }
            best_match += 1;
        }

        let record_offset = self.header.true_db_offset + (best_match * self.header.block_len);
        if record_offset + self.header.block_len > self.data.len() {
            return None;
        }

        let id_bytes = &self.data[record_offset + 3..record_offset + self.header.block_len];
        let mut id = 0;
        for &b in id_bytes {
            id = (id << 8) | (b as usize);
        }
        Some(id)
    }

    fn extract_strings(&self, start: usize, count: usize) -> Vec<String> {
        let mut strings = Vec::new();
        let mut current = start;
        for _ in 0..count {
            if current >= self.data.len() {
                break;
            }
            let mut end = current;
            while end < self.data.len() && self.data[end] != 0 {
                end += 1;
            }
            if let Ok(text) = std::str::from_utf8(&self.data[current..end]) {
                if text.len() > 2 {
                    strings.push(text.trim().to_string());
                }
            }
            current = end + 1;
        }
        strings
    }

    fn pick_name(mut strings: Vec<String>) -> Option<String> {
        if strings.is_empty() {
            return None;
        }
        if strings.len() >= 2 {
            Some(strings.remove(strings.len() - 2))
        } else {
            Some(strings.remove(0))
        }
    }

    /// Resolves an IP address and returns the location as a formatted String.
    /// The output format is either `"City, Country"` or `"Country"`.
    ///
    /// # Example
    /// ```no_run
    /// # use sxgeo_rs::SxGeo;
    /// # let db = SxGeo::load("SxGeoCity.dat").unwrap();
    /// if let Some(loc) = db.get_location("8.8.8.8") {
    ///     println!("{}", loc); // e.g., "Altuna, USA" or local translation based on DB
    /// }
    /// ```
    pub fn get_location(&self, ip: &str) -> Option<String> {
        let id = self.get_num(ip)?;
        if id == 0 {
            return None;
        }

        let mut loc_country = None;
        let mut loc_city = None;

        if id < 256 {
            // Early exit if the ID points directly to a country
            loc_country = self.countries_map.get(&(id as u8)).cloned();
        } else {
            let is_region = (id & 0x00200000) != 0;
            let clean_id = id & 0x001FFFFF;

            if is_region {
                // If only a Region is provided, extract the Country ID from the first byte (T:country_id)
                let base_offset = self.header.regions_offset + clean_id;
                if base_offset < self.data.len() {
                    let country_id = self.data[base_offset];
                    loc_country = self.countries_map.get(&country_id).cloned();
                }
            } else if clean_id < self.header.country_size {
                // Only Country provided
                let base_offset = self.header.cities_offset + clean_id;
                let strings = self.extract_strings(
                    base_offset + self.header.country_bin_size,
                    self.header.country_str_count,
                );
                loc_country = Self::pick_name(strings);
            } else {
                // Full City location provided
                let base_offset = self.header.cities_offset + clean_id;
                if base_offset + 4 <= self.data.len() {
                    // Read the 4th byte of the City header (skipping the 3-byte region_seek)
                    let country_id = self.data[base_offset + 3];
                    let c_strings = self.extract_strings(
                        base_offset + self.header.city_bin_size,
                        self.header.city_str_count,
                    );

                    loc_city = Self::pick_name(c_strings);
                    loc_country = self.countries_map.get(&country_id).cloned();
                }
            }
        }

        let mut parts = Vec::new();
        if let Some(c) = loc_city {
            parts.push(c);
        }
        if let Some(c) = loc_country {
            parts.push(c);
        }

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(", "))
        }
    }
}