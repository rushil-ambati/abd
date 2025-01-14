use super::messages::{ABDMessage, ServerMessage};
use ractor::{cast, Actor, ActorId, ActorProcessingErr, ActorRef};
use std::collections::{HashSet, VecDeque};

/// Reader status
#[derive(PartialEq)]
enum ReaderStatus {
    Idle,
    Phase1,
    Phase2,
}

/// Reader state
pub struct ReaderState {
    /// All replica servers
    servers: Vec<ActorRef<ServerMessage>>,

    /// Majority threshold for quorum
    majority: usize,

    /// True if currently processing a read operation.
    status: ReaderStatus,

    /// Read counter
    read_counter: u64,

    /// Tracks received acknowledgements from servers.
    server_acks: HashSet<ActorId>,

    /// Maximum tag and value received from servers.
    max_tag: u64,
    value: String,

    /// A backlog of write requests.
    backlog: VecDeque<ABDMessage>,
}

/// [Reader] is an actor for handling read requests in the ABD algorithm.
pub struct Reader;

impl Reader {
    fn handle_internal(
        &self,
        myself: &ActorRef<ABDMessage>,
        message: ABDMessage,
        state: &mut ReaderState,
    ) -> Option<ABDMessage> {
        // println!("[READER {}] {:?}", myself.get_id(), message);

        match message {
            ABDMessage::Read => {
                if state.status != ReaderStatus::Idle {
                    return Some(ABDMessage::Read); // mid-read, backlog the message
                }

                state.status = ReaderStatus::Phase1;
                state.server_acks.clear();
                state.read_counter += 1;

                for server in state.servers.iter() {
                    let _ = cast!(
                        server,
                        ServerMessage::Read(myself.clone(), state.read_counter,)
                    );
                }
            }

            ABDMessage::ReadAck(who, tag, value, read_counter) => {
                if state.status != ReaderStatus::Phase1 || state.read_counter != read_counter {
                    return None; // drop message
                }

                state.server_acks.insert(who);
                if tag > state.max_tag {
                    state.max_tag = tag;
                    state.value = value;
                }

                if state.server_acks.len() < state.majority {
                    return None; // wait for more acks
                }

                state.status = ReaderStatus::Phase2;
                state.server_acks.clear();
                state.read_counter += 1;

                for server in state.servers.iter() {
                    let _ = cast!(
                        server,
                        ServerMessage::Write(
                            myself.clone(),
                            state.max_tag,
                            state.value.clone(),
                            state.read_counter,
                        )
                    );
                }
            }

            ABDMessage::WriteAck(who, read_counter) => {
                if state.status != ReaderStatus::Phase2 || state.read_counter != read_counter {
                    return None; // drop message
                }

                state.server_acks.insert(who);
                if state.server_acks.len() < state.majority {
                    return None; // wait for more acks
                }

                state.status = ReaderStatus::Idle;
                println!("[READER] Complete: {}", state.value.clone());
            }

            _ => {
                // drop message
                return None;
            }
        }

        None
    }
}

impl Actor for Reader {
    type Msg = ABDMessage;
    type State = ReaderState;
    type Arguments = Vec<ActorRef<ServerMessage>>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        servers: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let majority = servers.len() / 2 + 1;
        Ok(ReaderState {
            servers,
            majority,
            status: ReaderStatus::Idle,
            read_counter: 0,
            server_acks: HashSet::new(),
            max_tag: 0,
            value: String::new(),
            backlog: VecDeque::new(),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let mut maybe_unhandled = self.handle_internal(&myself, message, state);
        if let Some(message) = maybe_unhandled {
            state.backlog.push_back(message);
        } else {
            // we handled the message, check the queue for any work to dequeue and handle
            while !state.backlog.is_empty() && maybe_unhandled.is_none() {
                let head = state.backlog.pop_front().unwrap();
                maybe_unhandled = self.handle_internal(&myself, head, state);
            }
            // put the first unhandled msg back to the front of the queue
            if let Some(msg) = maybe_unhandled {
                state.backlog.push_front(msg);
            }
        }
        Ok(())
    }
}
