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

use clap::Parser;
use futures::channel::mpsc::UnboundedSender;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use futures::try_join;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "influxdb2")]
use sunsniff::influxdb2::Influxdb2Receiver;
#[cfg(feature = "modbus")]
use sunsniff::modbus::ModbusConfig;
#[cfg(feature = "mqtt")]
use sunsniff::mqtt::MqttReceiver;
#[cfg(feature = "prometheus")]
use sunsniff::prometheus::PrometheusReceiver;
#[cfg(feature = "pcap")]
use sunsniff::pcap::PcapConfig;
use sunsniff::receiver::{Receiver, Update, UpdateItem};

#[derive(Debug, Parser)]
#[clap(author, version)]
struct Args {
    #[clap()]
    config_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum InputConfig {
    #[cfg(feature = "pcap")]
    Pcap(PcapConfig),
    #[cfg(feature = "modbus")]
    Modbus(ModbusConfig),
}

/// Structure corresponding to the configuration file. It is constructured
/// from the config file by serde.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(flatten)]
    input: InputConfig,
    #[cfg(feature = "influxdb2")]
    #[serde(default)]
    influxdb2: Vec<sunsniff::influxdb2::Config>,
    #[cfg(feature = "mqtt")]
    #[serde(default)]
    mqtt: Vec<sunsniff::mqtt::Config>,
    #[cfg(feature = "prometheus")]
    #[serde(default)]
    prometheus: Vec<sunsniff::prometheus::Config>,
}

/// Top-level execution. Receive updates from a stream and distribute them to
/// multiple receivers.
async fn run(
    stream: &mut (dyn Stream<Item = UpdateItem> + Unpin),
    sinks: &mut [UnboundedSender<Arc<Update<'static>>>],
) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(update) = stream.next().await {
        for sink in sinks.iter_mut() {
            sink.unbounded_send(Arc::clone(&update))?;
        }
    }
    for sink in sinks.iter_mut() {
        sink.close().await?; // TODO: do these in parallel?
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();
    let config = std::fs::read_to_string(args.config_file)?;
    let config: Config = toml::from_str(&config)?;

    let mut receivers: Vec<Box<dyn Receiver>> = vec![];
    #[cfg(feature = "influxdb2")]
    {
        for backend in config.influxdb2.iter() {
            receivers.push(Box::new(Influxdb2Receiver::new(backend).await));
        }
    }
    #[cfg(feature = "mqtt")]
    {
        for backend in config.mqtt.iter() {
            receivers.push(Box::new(MqttReceiver::new(backend)?));
        }
    }
    #[cfg(feature = "prometheus")]
    {
        for backend in config.prometheus.iter() {
            receivers.push(Box::new(PrometheusReceiver::new(backend).await));
        }
    }

    let mut sinks = vec![];
    let futures = FuturesUnordered::new();
    for receiver in receivers.iter_mut() {
        let (sink, stream) = futures::channel::mpsc::unbounded();
        futures.push(receiver.run(stream));
        sinks.push(sink);
    }

    // TODO: better handling of errors from receivers
    let mut stream = match &config.input {
        #[cfg(feature = "pcap")]
        InputConfig::Pcap(pcap_config) => sunsniff::pcap::create_stream(pcap_config)?,
        #[cfg(feature = "modbus")]
        InputConfig::Modbus(modbus_config) => {
            sunsniff::modbus::create_stream(modbus_config).await?
        }
    };
    try_join!(
        run(&mut stream, &mut sinks),
        futures.collect::<Vec<_>>().map(Ok)
    )?;
    Ok(())
}
