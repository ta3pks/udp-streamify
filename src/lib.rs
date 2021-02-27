use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Weak},
    time::{Duration, UNIX_EPOCH},
};

use tokio::{
    net::UdpSocket,
    sync::{
        mpsc::{channel, Receiver, Sender, UnboundedReceiver},
        Mutex,
    },
};
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        RwLock,
    },
};

type ClientMap = Arc<RwLock<HashMap<SocketAddr, UdpStream>>>;
type WeakClientMap = Weak<RwLock<HashMap<SocketAddr, UdpStream>>>;
pub struct UdpListener {
    clients: ClientMap,
    client_returner: UnboundedReceiver<UdpStream>,
    no_more_clients: Receiver<String>,
}
#[derive(Clone)]
pub struct UdpStream {
    pub addr: SocketAddr,
    to_main_sock: UnboundedSender<Vec<u8>>,
    from_main_sock_listener: Arc<Mutex<UnboundedReceiver<Vec<u8>>>>,
    from_main_sock_sender: UnboundedSender<Vec<u8>>,
    last_msg_time: u64,
    disconnect_notifier_main_tx: Sender<()>,
    disconnect_notifier_client_notifier: Arc<Mutex<Receiver<()>>>,
}
impl UdpStream {
    pub async fn read(&mut self) -> Option<Vec<u8>> {
        let mut data = self.from_main_sock_listener.lock().await;
        let mut disconnect = self.disconnect_notifier_client_notifier.lock().await;
        select! {
            d = data.recv()=>d,
            _ = disconnect.recv() => None
        }
    }
    pub async fn send(&mut self, data: &[u8]) -> Option<usize> {
        let len = data.len();
        self.to_main_sock.send(data.to_vec()).ok().map(|_| len)
    }
}
impl UdpListener {
    //{{{
    pub fn new(sock: UdpSocket, buf_len: usize, client_timeout: usize) -> Self {
        //{{{
        let clients: ClientMap = Default::default();
        let AcceptResult {
            new_client,
            no_more_accepts,
        } = internal_accept_start(clients.clone(), sock, buf_len, client_timeout);
        let l = UdpListener {
            clients,
            client_returner: new_client,
            no_more_clients: no_more_accepts,
        };
        l //}}}
    }
    pub async fn active_connections(&self) -> usize {
        self.clients.read().await.len()
    }
    pub async fn accept(&mut self) -> Result<UdpStream, String> {
        select! {
            client = self.client_returner.recv() =>{
                client.ok_or(String::from("client channel has been closed"))
            }
            err = self.no_more_clients.recv() => Err(err.unwrap_or(String::from("an error occured but for some reason the channel too died on the way")))
        }
    }
} //}}}
struct AcceptResult {
    new_client: UnboundedReceiver<UdpStream>,
    no_more_accepts: Receiver<String>,
}
fn internal_accept_start(
    clients: ClientMap,
    sock: UdpSocket,
    buf_len: usize,
    client_timeout: usize,
) -> AcceptResult {
    //{{{
    let clients = Arc::downgrade(&clients);
    let (client_notifier_tx, client_notifier_rx) = unbounded_channel();
    let (notx, norx) = channel(1);
    let sock = Arc::new(sock);
    // let c = clients.clone();
    // tokio::spawn(async move {
    //     let mut i = tokio::time::interval(Duration::from_secs(2));
    //     loop {
    //         i.tick().await;
    //         if let Some(c) = c.upgrade() {
    //             dbg!(c.read().await.len());
    //         } else {
    //             dbg!("socket dead");
    //             break;
    //         }
    //     }
    // });
    reader(
        sock.clone(),
        buf_len,
        clients.clone(),
        client_notifier_tx,
        notx.clone(),
        client_timeout,
    );
    AcceptResult {
        new_client: client_notifier_rx,
        no_more_accepts: norx,
    }
} //}}}
fn reader(
    sock: Arc<UdpSocket>,
    buf_len: usize,
    clients: WeakClientMap,
    client_notifier_tx: UnboundedSender<UdpStream>,
    notx: Sender<String>,
    client_timeout: usize,
) {
    //{{{
    tokio::task::spawn(async move {
        let mut buf = vec![0; buf_len];
        while let Ok((n, addr)) = sock.recv_from(&mut buf).await {
            if let Some(clients_strong) = clients.upgrade() {
                let mut clients_map = clients_strong.write().await;
                let main_sock = sock.clone();
                if let Some(sock) = clients_map.get_mut(&addr) {
                    sock.last_msg_time = UNIX_EPOCH.elapsed().unwrap().as_secs();
                    if let Err(_) = sock.from_main_sock_sender.send(buf[..n].to_vec()) {
                        clients_map.remove(&addr);
                    }
                } else {
                    //new Sock
                    let (from_self_tx, mut from_self_rx) = unbounded_channel();
                    let (dis_tx, mut dis_rx) = channel(1);
                    let (from_main_sock_tx, from_main_sock_rx) = unbounded_channel();
                    let (dis_client_tx, dis_client_rx) = channel(1);
                    let sock = UdpStream {
                        addr: addr.clone(),
                        to_main_sock: from_self_tx.clone(),
                        last_msg_time: UNIX_EPOCH.elapsed().unwrap().as_secs(),
                        disconnect_notifier_main_tx: dis_tx,
                        from_main_sock_listener: Arc::new(Mutex::new(from_main_sock_rx)),
                        from_main_sock_sender: from_main_sock_tx.clone(),
                        disconnect_notifier_client_notifier: Arc::new(Mutex::new(dis_client_rx)),
                    };
                    let _ = client_notifier_tx.send(sock.clone());
                    clients_map.insert(addr.clone(), sock);
                    let _ = from_main_sock_tx.send(buf[..n].to_vec());
                    drop(clients_map);
                    let clients = clients.clone();
                    tokio::spawn(async move {
                        //self to remote {{{

                        loop {
                            select! {
                                data = from_self_rx.recv() =>{
                                    if let Some(data) = data{
                                        if let Err(_) = main_sock.send_to(&data,addr).await{
                                            cleanup(clients,&addr,dis_client_tx.clone()).await;
                                            break;
                                        };
                                    }
                                    if let Some(clients) = clients.upgrade(){
                                        //increase sock time since it sock did something
                                        if let Some(sock)=clients.write().await.get_mut(&addr){
                                            sock.last_msg_time = UNIX_EPOCH.elapsed().unwrap().as_secs();
                                        }
                                    }

                                }
                                _ = dis_rx.recv() =>{
                                    from_self_rx.close();
                                    cleanup(clients,&addr,dis_client_tx).await;
                                    break
                                }
                                _ =  tokio::time::sleep(Duration::from_secs(30)) ,if client_timeout>0 =>{
                                    //no read for 30 seconds if no writes as well close the socket
                                    if let Some(clients) = clients.upgrade(){
                                        let  clients = clients.read().await;
                                        if let Some(sock)=clients.get(&addr){
                                            if  UNIX_EPOCH.elapsed().unwrap().as_secs() - sock.last_msg_time>=client_timeout as u64{
                                                let _ = sock.disconnect_notifier_main_tx.send(()).await;
                                                //dis_rx.recv above will handle the cleanup
                                            }

                                        }
                                    }else{
                                        break;
                                    }
                                }
                            }
                        }
                    }); //}}}
                }
            } else {
                let _ = notx.send("Socket Closed".into());
                break;
            }
        }
    });
} //}}}
async fn cleanup(clients: WeakClientMap, addr: &SocketAddr, notifier: Sender<()>) {
    if let Some(clients) = clients.upgrade() {
        if let Some(sock) = clients.write().await.remove(addr) {
            let _ = sock.disconnect_notifier_main_tx.send(()).await;
            let _ = notifier.send_timeout((), Duration::from_secs(5)).await;
        }
    }
}
