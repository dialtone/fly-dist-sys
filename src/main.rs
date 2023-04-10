use std::collections::{HashMap, HashSet};
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::time::{self, Duration};

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

impl Body {
    fn reply(&self, node: &Node) -> Option<Body> {
        match &self.extra {
            Payload::Init { .. } => Some(Body {
                in_reply_to: self.msg_id,
                extra: Payload::InitOk {},
                msg_id: None,
            }),

            // problem 1
            Payload::Echo { echo } => Some(Body {
                in_reply_to: self.msg_id,
                extra: Payload::EchoOk { echo: echo.clone() },
                msg_id: None,
            }),

            // problem 2, could also use ulid
            Payload::Generate => {
                let id = format!("{}-{}", node.id, node.msg_ids);
                Some(Body {
                    in_reply_to: self.msg_id,
                    extra: Payload::GenerateOk { id },
                    msg_id: None,
                })
            }

            // problem 3
            Payload::Broadcast { message } => Some(Body {
                in_reply_to: self.msg_id,
                extra: Payload::BroadcastOk {},
                msg_id: None,
            }),

            Payload::Read => Some(Body {
                in_reply_to: self.msg_id,
                msg_id: None,
                extra: Payload::ReadOk {
                    messages: node.messages.iter().map(|i| *i).collect::<Vec<usize>>(),
                },
            }),

            Payload::Topology { .. } => Some(Body {
                in_reply_to: self.msg_id,
                msg_id: None,
                extra: Payload::TopologyOk {},
            }),
            Payload::BroadcastOk => None,
            Payload::EchoOk { .. } => None,
            Payload::InitOk => None,
            Payload::GenerateOk { .. } => None,
            Payload::ReadOk { .. } => None,
            Payload::TopologyOk => None,
        }
    }
}

struct Node {
    output: io::Stdout,
    id: String,
    nodes: Vec<String>,
    msg_ids: u64,

    messages: HashSet<usize>,
    pending: HashMap<u64, (String, Body)>,
}

impl Node {
    fn new(output: io::Stdout, id: String, nodes: Vec<String>) -> Self {
        Node {
            output,
            id,
            nodes,
            msg_ids: 0,
            messages: HashSet::new(),
            pending: HashMap::new(),
        }
    }

    async fn rpc(&mut self, dest: &str, body: Body) -> io::Result<()> {
        self.pending
            .insert(body.msg_id.unwrap(), (dest.to_string(), body.clone()));
        self.send(dest, body).await
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

    async fn gossip(&mut self) -> io::Result<()> {
        // this is really problem 3b (broadcast with partitions)
        // not sure I like this too much with mem::replace but it works fine
        let drained = std::mem::replace(&mut self.pending, HashMap::new());
        for (_, (dest, msg)) in drained {
            self.rpc(&dest, msg).await?;
        }
        Ok(())
    }

    async fn handle(&mut self, line: &str) -> io::Result<()> {
        let msg = serde_json::from_str::<Msg>(line)?;
        match &msg.body.extra {
            Payload::Broadcast { message } => {
                // since we're guaranteed that messages are unique
                // and we broadcast to every node...
                // if I already have something in my memory it means I already broadcast it properly
                // so it works but it's horrible although simple
                if !self.messages.contains(message) {
                    self.messages.insert(*message);
                    for idx in 0..self.nodes.len() {
                        let node = &self.nodes[idx];
                        if *node == self.id || *node == msg.src {
                            continue;
                        }
                        self.rpc(&node.clone(), msg.body.clone()).await?;
                    }
                }
            }

            Payload::BroadcastOk => {
                self.pending.remove(&msg.body.in_reply_to.unwrap());
            }
            _ => {}
        }

        if let Some(next_msg) = msg.body.reply(self) {
            self.send(&msg.src, next_msg).await?;
        }
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
    let next_msg = msg.body.reply(&n);
    n.send(&msg.src, next_msg.unwrap()).await?;

    let mut interval = time::interval(Duration::from_millis(300));
    loop {
        tokio::select! {
            maybe_line = input_lines.next_line() => {
                if let Ok(Some(line)) = maybe_line {
                    eprintln!("{}", line);
                    n.handle(&line).await.unwrap();
                } else {
                    break;
                }
            }
            _ = interval.tick() => {
                n.gossip().await?;
            }
        }
    }

    Ok(())
}
