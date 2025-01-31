/* Copyright 2022-2023 Bruce Merry
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#![doc = include_str!("../README.md")]

#[cfg(all(not(feature = "pcap"), not(feature = "modbus")))]
compile_error!("At least one frontend feature must be enabled");

pub mod fields;
#[cfg(feature = "influxdb2")]
pub mod influxdb2;
#[cfg(feature = "modbus")]
pub mod modbus;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "prometheus")]
pub mod prometheus;
#[cfg(feature = "pcap")]
pub mod pcap;
pub mod receiver;
