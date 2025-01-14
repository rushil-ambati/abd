use ractor::{cast, Actor, ActorId, ActorProcessingErr, ActorRef};
use std::collections::{HashSet, VecDeque};

use super::messages::{ABDMessage, ServerMessage};

/// Writer state
pub struct WriterState {
    /// All replica servers
    servers: Vec<ActorRef<ServerMessage>>,

    /// Majority threshold for quorum
    majority: usize,

    /// Flag to identify if a write operation is in progress.
    writing: bool,

    /// Local counter
    tag: u64,

    /// Write counter
    write_counter: u64,

    /// Servers that have acknowledged the write.
    server_acks: HashSet<ActorId>,

    /// A backlog of write requests.
    backlog: VecDeque<ABDMessage>,
}

/// [Writer] is an actor for the sole writer in the ABD algorithm.
pub struct Writer;

impl Writer {
    fn handle_internal(
        &self,
        myself: &ActorRef<ABDMessage>,
        message: ABDMessage,
        state: &mut WriterState,
    ) -> Option<ABDMessage> {
        // println!("[WRITER] {:?}", message);

        match message {
            ABDMessage::Write(value) => {
                if state.writing {
                    return Some(ABDMessage::Write(value)); // mid-write, backlog the message
                }

                state.writing = true;
                state.tag += 1;
                state.write_counter += 1;
                state.server_acks.clear();

                for server in state.servers.iter() {
                    let _ = cast!(
                        server,
                        ServerMessage::Write(
                            myself.clone(),
                            state.tag,
                            value.clone(),
                            state.write_counter,
                        )
                    );
                }
            }

            ABDMessage::WriteAck(who, wc) => {
                if !state.writing {
                    return None; // drop message
                }

                if state.write_counter == wc {
                    state.server_acks.insert(who);
                }

                if state.server_acks.len() >= state.majority {
                    state.writing = false;
                    println!("[WRITER] Write complete: {}", state.tag);
                }
            }

            _ => {
                return None; // drop message
            }
        }

        None
    }
}

impl Actor for Writer {
    type Msg = ABDMessage;
    type State = WriterState;
    type Arguments = Vec<ActorRef<ServerMessage>>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        servers: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let majority = (servers.len() + 1) / 2;
        Ok(Self::State {
            servers,
            majority,
            writing: false,
            tag: 0,
            write_counter: 0,
            server_acks: HashSet::new(),
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
