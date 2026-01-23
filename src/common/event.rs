use pulsar::{Error as PulsarError, SerializeMessage, producer};

#[allow(dead_code)]
pub enum EventType {
    Tick,
    MBP,
}

#[allow(dead_code)]
pub struct OutEvent {
    pub symbol: String,
    pub payload: Vec<u8>,

    pub event_time: i64,
    pub event_type: EventType,
    pub received_at: i64,
}

impl SerializeMessage for OutEvent {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        Ok(producer::Message {
            payload: input.payload,
            partition_key: Some(input.symbol),
            ..Default::default()
        })
    }
}
