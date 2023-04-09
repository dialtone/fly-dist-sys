use std::collections::{HashMap, HashSet};
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Msg {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Clone, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,

    Generate,
    GenerateOk {
        id: String,
    },

    Broadcast {
        message: usize,
    },
    BroadcastOk,

    Read,
    ReadOk {
        messages: Vec<usize>,
    },

    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

#[derive(Serialize, Clone, Deserialize, Debug)]
struct Body {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,

    #[serde(flatten)]
    extra: Payload,
}

struct Node {
    output: io::Stdout,
    id: String,
    nodes: Vec<String>,
    msg_ids: u64,

    messages: HashSet<usize>,
}

impl Node {
    fn new(output: io::Stdout, id: String, nodes: Vec<String>) -> Self {
        Node {
            output,
            id,
            nodes,
            msg_ids: 0,
            messages: HashSet::new(),
        }
    }

    fn reply(&mut self, body: &Body) -> Option<Body> {
        match &body.extra {
            Payload::Init { .. } => Some(Body {
                in_reply_to: body.msg_id,
                extra: Payload::InitOk {},
                msg_id: None,
                // msg_id: Some(self.msg_ids),
            }),

            // problem 1
            Payload::Echo { echo } => Some(Body {
                // msg_id: Some(self.msg_ids),
                in_reply_to: body.msg_id,
                extra: Payload::EchoOk { echo: echo.clone() },
                msg_id: None,
            }),

            // problem 2, could also use ulid
            Payload::Generate => {
                let id = format!("{}-{}", self.id, self.msg_ids);
                Some(Body {
                    // msg_id: Some(self.msg_ids),
                    in_reply_to: body.msg_id,
                    extra: Payload::GenerateOk { id },
                    msg_id: None,
                })
            }

            // problem 3
            Payload::Broadcast { message } => {
                if self.messages.contains(message) {
                    return None;
                }
                self.messages.insert(*message);
                Some(Body {
                    // msg_id: Some(self.msg_ids),
                    in_reply_to: body.msg_id,
                    extra: Payload::BroadcastOk {},
                    msg_id: None,
                })
            }

            Payload::Read => Some(Body {
                in_reply_to: body.msg_id,
                msg_id: None,
                extra: Payload::ReadOk {
                    messages: self.messages.iter().map(|i| *i).collect::<Vec<usize>>(),
                },
            }),

            Payload::Topology { .. } => Some(Body {
                in_reply_to: body.msg_id,
                msg_id: None,
                extra: Payload::TopologyOk {},
            }),
            Payload::BroadcastOk => None,
            _ => todo!(),
            // Payload::EchoOk { echo } => {}
            // Payload::InitOk => {}
            // Payload::GenerateOk { id } => {}
            // Payload::ReadOk { messages } => {}
            // Payload::TopologyOk => {}
        }
    }

    async fn send(&mut self, dest: &str, mut body: Body) -> io::Result<()> {
        self.msg_ids += 1;
        body.msg_id = Some(self.msg_ids);
        let s = serde_json::to_string(&Msg {
            src: self.id.clone(),
            dest: dest.into(),
            body,
        })
        .unwrap();

        eprintln!("out: {}", s);
        self.output.write_all(s.as_bytes()).await?;
        self.output.write_all(b"\n").await?;
        self.output.flush().await?;
        Ok(())
    }

    async fn handle(&mut self, line: &str) -> io::Result<()> {
        let msg = serde_json::from_str::<Msg>(line)?;
        let next_msg = self.reply(&msg.body);
        if next_msg.is_none() {
            return Ok(());
        }
        let next_msg = next_msg.unwrap();
        if let Payload::Broadcast { .. } = msg.body.extra {
            // this really works for 2 reason:
            // we broadcast to all nodes, not just neighbors and we avoid loops
            // because we don't retransmit to ourselves or source and we don't
            // further re-broadcast if we have the message already
            // (because it means we've already broadcast it, which is cheating a bit)
            let iter_nodes = self.nodes.clone();
            for node in iter_nodes {
                if node == self.id || node == msg.src {
                    continue;
                }

                self.send(&node, msg.body.clone()).await?;
            }
        }
        self.send(&msg.src, next_msg).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let input = io::BufReader::new(io::stdin());
    let mut input_lines = input.lines();
    let output = io::stdout();

    // init
    let msg = serde_json::from_str::<Msg>(&input_lines.next_line().await.unwrap().unwrap())?;
    eprintln!("{:?}", msg);
    let mut n = if let Payload::Init {
        ref node_id,
        ref node_ids,
    } = msg.body.extra
    {
        Node::new(output, node_id.clone(), node_ids.clone())
    } else {
        panic!("abort first message should be init");
    };
    let next_msg = n.reply(&msg.body);
    n.send(&msg.src, next_msg.unwrap()).await?;

    // main loop
    while let Some(line) = input_lines.next_line().await.unwrap() {
        eprintln!("{}", line);
        n.handle(&line).await?;
    }
    Ok(())
}
