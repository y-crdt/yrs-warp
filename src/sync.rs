use lib0::error::Error;
use yrs::updates::decoder::{Decode, Decoder};
use yrs::updates::encoder::{Encode, Encoder};
use yrs::StateVector;

// Core Yjs defines two message types:
// • YjsSyncStep1: Includes the State Set of the sending client. When received, the client should reply with YjsSyncStep2.
// • YjsSyncStep2: Includes all missing structs and the complete delete set. When received, the client is assured that it
//   received all information from the remote client.
//
// In a peer-to-peer network, you may want to introduce a SyncDone message type. Both parties should initiate the connection
// with SyncStep1. When a client received SyncStep2, it should reply with SyncDone. When the local client received both
// SyncStep2 and SyncDone, it is assured that it is synced to the remote client.
//
// In a client-server model, you want to handle this differently: The client should initiate the connection with SyncStep1.
// When the server receives SyncStep1, it should reply with SyncStep2 immediately followed by SyncStep1. The client replies
// with SyncStep2 when it receives SyncStep1. Optionally the server may send a SyncDone after it received SyncStep2, so the
// client knows that the sync is finished.  There are two reasons for this more elaborated sync model: 1. This protocol can
// easily be implemented on top of http and websockets. 2. The server shoul only reply to requests, and not initiate them.
// Therefore it is necesarry that the client initiates the sync.
//
// Construction of a message:
// [messageType : varUint, message definition..]
//
// Note: A message does not include information about the room name. This must to be handled by the upper layer protocol!
//
// stringify[messageType] stringifies a message definition (messageType is already read from the bufffer)

pub const MSG_SYNC_STEP_1: u8 = 0;
pub const MSG_SYNC_STEP_2: u8 = 1;
pub const MSG_SYNC_UPDATE: u8 = 2;

#[derive(Debug, PartialEq, Eq)]
pub enum Message {
    SyncStep1(StateVector),
    SyncStep2(Vec<u8>),
    Update(Vec<u8>),
}

impl Encode for Message {
    fn encode<E: Encoder>(&self, encoder: &mut E) {
        match self {
            Message::SyncStep1(sv) => {
                encoder.write_var(MSG_SYNC_STEP_1);
                encoder.write_buf(sv.encode_v1());
            }
            Message::SyncStep2(u) => {
                encoder.write_var(MSG_SYNC_STEP_2);
                encoder.write_buf(u);
            }
            Message::Update(u) => {
                encoder.write_var(MSG_SYNC_UPDATE);
                encoder.write_buf(u);
            }
        }
    }
}

impl Decode for Message {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, Error> {
        let tag: u8 = decoder.read_var()?;
        match tag {
            MSG_SYNC_STEP_1 => {
                let buf = decoder.read_buf()?;
                let sv = StateVector::decode_v1(buf)?;
                Ok(Message::SyncStep1(sv))
            }
            MSG_SYNC_STEP_2 => {
                let buf = decoder.read_buf()?;
                Ok(Message::SyncStep2(buf.into()))
            }
            MSG_SYNC_UPDATE => {
                let buf = decoder.read_buf()?;
                Ok(Message::Update(buf.into()))
            }
            _ => Err(Error::UnexpectedValue),
        }
    }
}
