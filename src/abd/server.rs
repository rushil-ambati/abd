use std::collections::HashMap;

use ractor::{cast, Actor, ActorId, ActorProcessingErr, ActorRef};

use super::messages::{ABDMessage, ServerMessage};

/// [Server] is an actor for a replica server in the ABD algorithm.
pub struct Server;

pub struct ServerState {
    /// Shared object value
    value: String,

    /// Tag
    tag: u64,

    /// Counters for each writer/reader
    counters: HashMap<ActorId, u64>,
}

impl Actor for Server {
    type Msg = ServerMessage;
    type State = ServerState;
    type Arguments = ();

    /// Initializes the actor state.
    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        _: (),
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(ServerState {
            tag: 0,
            value: String::new(),
            counters: HashMap::new(),
        })
    }

    /// Handles incoming messages.
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // println!("[SRV {}] {:?}", myself.get_id(), message);

        match message {
            ServerMessage::Write(sender, tag, value, write_counter) => {
                if write_counter <= state.counters.get(&sender.get_id()).copied().unwrap_or(0) {
                    return Ok(());
                }

                state.counters.insert(sender.get_id(), write_counter);

                if tag > state.tag {
                    state.tag = tag;
                    state.value = value;
                }

                let _ = cast!(sender, ABDMessage::WriteAck(myself.get_id(), write_counter));
            }

            ServerMessage::Read(sender, read_counter) => {
                if read_counter < state.counters.get(&sender.get_id()).copied().unwrap_or(0) {
                    return Ok(());
                }

                state.counters.insert(sender.get_id(), read_counter);

                let _ = cast!(
                    sender,
                    ABDMessage::ReadAck(
                        myself.get_id(),
                        state.tag,
                        state.value.clone(),
                        read_counter
                    )
                );
            }
        }
        Ok(())
    }
}
