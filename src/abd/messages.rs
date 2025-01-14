use ractor::{ActorId, ActorRef};

/// Messages supported by the [Writer] and [Reader] actors.
#[derive(Debug, Clone)]
pub enum ABDMessage {
    /// Client requests
    Write(String),
    Read,

    /// Server responses
    WriteAck(ActorId, u64),
    ReadAck (ActorId, u64, String, u64),
}

/// Messages supported by the [Server] actor.
#[derive(Debug, Clone)]
pub enum ServerMessage {
    Write(ActorRef<ABDMessage>, u64, String, u64),
    Read(ActorRef<ABDMessage>, u64),
}
