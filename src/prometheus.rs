use async_std::task;
use async_trait::async_trait;
use futures::channel::mpsc::UnboundedReceiver;
use futures::stream::StreamExt;
use log::debug;
use prometheus_reqwest_remote_write::{Label, Sample, TimeSeries, WriteRequest};
use serde::{self, Deserialize};
use std::iter::zip;
use std::sync::Arc;

use reqwest::Client;
use std::time::Duration;
use url::Url;

use super::receiver::{Receiver, Update};

pub struct PrometheusMetric {
    labels: Vec<Label>,
    samples: Vec<Sample>,
}

impl PrometheusMetric {
    pub fn builder() -> MetricBuilder {
        MetricBuilder::new()
    }

    pub fn build_timeseries(&self) -> TimeSeries {
        TimeSeries {
            labels: self.labels.clone(),
            samples: self.samples.clone(),
        }
    }
}

pub struct MetricBuilder {
    labels: Vec<Label>,
    samples: Vec<Sample>,
}

impl MetricBuilder {
    pub fn new() -> Self {
        Self {
            labels: Default::default(),
            samples: Default::default(),
        }
    }

    pub fn build(self) -> PrometheusMetric {
        let Self {
            labels,
            samples,
        } = self;
        PrometheusMetric {
            labels,
            samples,
        }
    }

    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push(Label {
            name: name.into(),
            value: value.into(),
        });
        self
    }

    pub fn sample(mut self, timestamp: impl Into<i64>, value: impl Into<f64>) -> Self {
        self.samples.push(Sample {
            timestamp: timestamp.into(),
            value: value.into(),
        });
        self
    }
}

pub struct PrometheusClient {
    url: Url,
    user_agent: String,
}

impl PrometheusClient {
    pub fn new(url: Url) -> Self {
        PrometheusClient {
            url,
            user_agent: String::from("sunsniff"),
        }
    }

    pub fn send_write_request(
        &self,
        metrics: &Vec<PrometheusMetric>,
    ) -> Result<reqwest::Request, reqwest::Error> {
        let client = Client::new();
        let mut timeseries = vec![];
        for metric in metrics {
            let ts = metric.build_timeseries();
            timeseries.push(ts);
        }
        let write_request = WriteRequest { timeseries };
        write_request.build_http_request(client, &self.url.as_str(), &self.user_agent)
    }
}

pub struct PrometheusReceiver {
    client: PrometheusClient,
}

impl PrometheusReceiver {
    pub async fn new(config: &Config) -> Self {
        let url = Url::parse(&config.url).unwrap();
        Self {
            client: PrometheusClient::new(url),
        }
    }
}

#[async_trait]
impl Receiver for PrometheusReceiver {
    async fn run<'a>(&mut self, mut receiver: UnboundedReceiver<Arc<Update<'a>>>) {
        while let Some(update) = receiver.next().await {
            let mut metrics = vec![];
            for (field, value) in zip(update.fields.iter(), update.values.iter()) {

                let name = format!("sunsniff_{}",field.group.to_lowercase());
                debug!("name: {}", name);
                let timestamp = update.timestamp.clone()/1000000;
                debug!("ts: {}, v: {}",timestamp, *value);

                let builder = PrometheusMetric::builder()
                    .sample(timestamp, *value)
                    .label("__name__", name)
                    .label("inverter_id", update.serial.as_str())
                    .label("name", field.name)
                    .label("id", field.id)
                    .label("scale", field.scale.to_string())
                    .label("bias", field.bias.to_string())
                    .label("group", field.group);
                let builder = if field.unit.is_empty() {
                    builder
                } else {
                    builder.label("unit", field.unit)
                };
                let prometheus_metric = builder.build();

                metrics.push(prometheus_metric);
            }
            loop {
                match self.client.send_write_request(&metrics) {
                    Ok(req) => {
                        let resp = Client::new().execute(req).await;
                        match resp {
                            Ok(resp) =>{
                                debug!("{}", String::from(resp.status().as_str()));
                            }
                            Err(err) => {
                                debug!(
                                    "Error writing to prometheus; trying again in 5s ({:?})",
                                    err
                                );
                                task::sleep(Duration::from_secs(5)).await;
                            }
                        }
                        break;
                    }
                    Err(err) => {
                        debug!(
                            "Error creating prometheus request; trying again in 5s ({:?})",
                            err
                        );
                    }
                }
            }
        }

    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}
