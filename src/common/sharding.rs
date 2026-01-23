/*
 * BLACKER
 * Copyright (C) 2026  Juan José Caballero Rey
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use xxhash_rust::xxh3::xxh3_64;


/// Deterministic sharding function
pub fn belongs_to_shard(symbol: &str, shard_id: u32, total_shards: u32) -> bool {
    (xxh3_64(symbol.as_bytes()) % total_shards as u64) == shard_id as u64
}