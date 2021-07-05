use dotenv;
use exitfailure::ExitFailure;
use failure::ResultExt;
use futures::*;
use log::*;
use rdkafka::client::NativeClient;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::Headers;
use rdkafka::types::RDKafkaRespErr;
use rdkafka::util::cstr_to_owned;
use rdkafka::ClientContext;
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use rdkafka_sys as rdsys;
use rev_lines::RevLines;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::*;
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Debug, StructOpt)]
struct Cli {
    /// Comma seperated list of brokers
    #[structopt(default_value = "localhost:9092", short = "b", long = "brokers")]
    brokers: String,
    #[structopt(default_value = "kafka_dump", short = "g", long = "group")]
    group_id: String,
    #[structopt(short = "t", long = "topic")]
    topic: String,
    #[structopt(default_value = "3", short = "c", long = "clients")]
    client_count: i8,
    #[structopt(flatten)]
    verbose: clap_verbosity_flag::Verbosity,
}

fn file_prefix(topic: &str, partition: i32) -> String {
    format!("{}.{}", topic, partition)
}

struct CustomContext {
    export_files: Arc<tokio::sync::Mutex<HashMap<String, tokio::fs::File>>>,
}

impl CustomContext {
    fn find_last_offset(&self, path: &Path) -> Option<i64> {
        if !path.exists() {
            return None;
        }
        // Open a file in write-only mode, returns `io::Result<File>`
        let file = File::open(path)
            .with_context(|_| {
                format!(
                    "couldn't open export file to read latest offset: {}",
                    path.display()
                )
            })
            .unwrap();
        let mut rev_lines = RevLines::new(BufReader::new(file)).unwrap();
        let last_line_offset: Option<i64> = rev_lines
            .find(|line| line.len() > 0)
            .map(|line| {
                line.split(':')
                    .nth(0)
                    .map(|offset_str| offset_str.parse::<i64>().ok())
                    .flatten()
            })
            .flatten();
        last_line_offset
    }
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    /// Implements the rebalancing strategy
    fn rebalance(
        &self,
        native_client: &NativeClient,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        futures::executor::block_on(async move {
            // drop all file handles
            let mut files = self.export_files.lock().await;
            *files = HashMap::new();
        });
        // re-assignment
        match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                // seek to the last known offset for each assigned topic and create an export file handle
                for mut el in tpl.elements() {
                    let file_prefix = file_prefix(el.topic(), el.partition());
                    let filename = format!("{}.txt", file_prefix);
                    let path = Path::new(&filename);
                    // attempt to seek the topic partition to the last offset written in the export file
                    match self.find_last_offset(&path) {
                        Some(offset) => {
                            let next = offset + 1;
                            info!(
                                "Continue reading topic '{}' partition {} from previous offset {}",
                                el.topic(),
                                el.partition(),
                                next
                            );
                            el.set_offset(rdkafka::topic_partition_list::Offset::Offset(next))
                                .unwrap();
                        }
                        _ => {
                            info!(
                                "Start reading topic '{}' partition {} from beginning",
                                el.topic(),
                                el.partition()
                            );
                            el.set_offset(rdkafka::topic_partition_list::Offset::Beginning)
                                .unwrap();
                        }
                    }
                    // open file in append mode and add to file handles
                    futures::executor::block_on(async move {
                        let mut files = self.export_files.lock().await;
                        let file = tokio::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .write(true)
                            .read(false)
                            .open(&path)
                            .await
                            .with_context(|_| {
                                format!("couldn't create export file: {}", path.display())
                            })
                            .unwrap();
                        files.insert(file_prefix, file);
                    });
                }

                unsafe { rdsys::rd_kafka_assign(native_client.ptr(), tpl.ptr()) };
            }
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                // Also for RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
                unsafe { rdsys::rd_kafka_assign(native_client.ptr(), ptr::null()) };
            }
            _ => {
                let error = unsafe { cstr_to_owned(rdsys::rd_kafka_err2str(err)) };
                error!("Error rebalancing: {}", error);
                // Also for RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
                unsafe { rdsys::rd_kafka_assign(native_client.ptr(), ptr::null()) };
            }
        };
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        match result {
            Ok(_) => {
                for offset in offsets.elements() {
                    debug!(
                        "Committed offset for topic '{}' partition {}: {:?}",
                        offset.topic(),
                        offset.partition(),
                        offset.offset()
                    );
                }
            }
            Err(e) => debug!("Error committing offsets: {:?}", e),
        }
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

fn create_consumer(brokers: String, group_id: String) -> LoggingConsumer {
    let context = CustomContext {
        export_files: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    };
    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set_log_level(RDKafkaLogLevel::Info)
        .create_with_context(context)
        .expect("Consumer creation failed");
    consumer
}

async fn run_async_processor(consumer: LoggingConsumer, input_topic: String) {
    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");
    let context = consumer.context().clone();
    let export_files = &context.inner().export_files;

    info!("Starting event loop");
    // Create the outer pipeline on the message stream.
    let mut message_stream = consumer.stream();
    let mut count = 0i64;
    let mut payload_buf = String::new();
    while let Some(message) = message_stream.next().await {
        let borrowed_message = message.unwrap();
        // reset buffer
        payload_buf.truncate(0);
        // add offset to buffer
        let offset = borrowed_message.offset().to_string();
        payload_buf.push_str(&offset);
        payload_buf.push(':');
        // add msg timestamp to buffer
        let timestamp = borrowed_message
            .timestamp()
            .to_millis()
            .unwrap_or(-1)
            .to_string();
        payload_buf.push_str(&timestamp);
        payload_buf.push(':');
        // add msg key to buffer
        if let Some(key) = borrowed_message.key() {
            base64::encode_config_buf(key, base64::STANDARD, &mut payload_buf);
        }
        payload_buf.push(':');
        // add msg payload to buffer
        if let Some(payload) = borrowed_message.payload() {
            base64::encode_config_buf(payload, base64::STANDARD, &mut payload_buf);
        }
        // add msg headers to buffer
        if let Some(headers) = borrowed_message.headers() {
            for i in 0..headers.count() {
                if let Some((name, value)) = headers.get(i) {
                    payload_buf.push(':');
                    payload_buf.push_str(name);
                    payload_buf.push(':');
                    base64::encode_config_buf(value, base64::STANDARD, &mut payload_buf);
                }
            }
        }
        payload_buf.push('\n');
        // hold lock in the next block and write buffer to topic/partition file
        {
            let file_prefix = file_prefix(borrowed_message.topic(), borrowed_message.partition());
            let mut files = export_files.lock().await;
            let file = files
                .get_mut(&file_prefix)
                .ok_or(format!("Export file not found: {},", file_prefix))
                .unwrap();

            // println!("buf len {}", payload_buf.len());
            file.write_all(payload_buf.as_bytes())
                .await
                .with_context(|_| format!("couldn't write to: {}", file_prefix))
                .unwrap();
        }

        count += 1;
        if count % 10000 == 0 {
            // break;
            info!("Reached offset {}", borrowed_message.offset());
        }
        if let Err(e) = consumer.store_offset(&borrowed_message) {
            warn!("Error while storing offset: {}", e);
        }
    }

    info!("Stream processing terminated");
}

#[tokio::main]
async fn main() -> Result<(), ExitFailure> {
    // parse cli args
    let args = Cli::from_args();
    // loads the .env file located in the environment's current directory
    dotenv::dotenv().ok();
    // simple logger configured via cli args which writes to stdout or stderr
    let log_level = args.verbose.log_level().expect("Log level not set");
    simple_logger::init_with_level(log_level).with_context(|_| "Unsupported log level")?;

    // futures::future::join_all
    let worker_tasks: Vec<tokio::task::JoinHandle<()>> = (0..args.client_count)
        .map(|_| {
            let consumer = create_consumer(args.brokers.to_owned(), args.group_id.to_owned());
            tokio::spawn(run_async_processor(consumer, args.topic.to_owned()))
        })
        .collect();
    futures::future::join_all(worker_tasks).await;

    // cli return
    Ok(())
}
