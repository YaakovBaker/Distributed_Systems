package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer{
    private final InetSocketAddress myAddress;
    private final int myPort;
    private final int tcpPort;
    private ServerState state;
    private AtomicBoolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<ClientRequest> tcpWorkQueue;
    private Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private ConcurrentMap<Long,InetSocketAddress> peerIDtoAddress;
    private Long gateWayID;
    private int numberOfObservers;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    private JavaRunnerFollower follow;
    private RoundRobinLeader leader;
    private TCPServer tcpServer;
    private Gossiper gossip;

    private Logger peerServerLogger;
    private ConcurrentMap<Long, BohemianRhapsody> monitering;
    private ConcurrentMap<InetSocketAddress, Long> addrToPeer;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers){
        this.myPort = myPort;
        this.tcpPort = this.myPort + 2;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress);
        this.myAddress = new InetSocketAddress("localhost", this.myPort);
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.tcpWorkQueue = new LinkedBlockingQueue<>();
        this.monitering = new ConcurrentHashMap<>();
        this.shutdown= new AtomicBoolean(false);
        this.state = ServerState.LOOKING;
        this.addrToPeer = new ConcurrentHashMap<>();
        
        this.currentLeader = null;
        this.gateWayID = gatewayID;
        this.numberOfObservers = numberOfObservers;

        this.follow = null;
        this.leader = null;
        this.tcpServer = null;
        this.gossip = null;
        
        try {
            this.peerServerLogger = initializeLogging(ZooKeeperPeerServerImpl.class.getCanonicalName() + "-on-serverID-" + this.id + "-on-tcpPort-" + this.tcpPort + "-Log.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.peerServerLogger.log(Level.FINE, "ZooKeeperPeerServerImpl Construction Finished!");
    }

    @Override
    public void shutdown(){
        this.shutdown.set(true);
        this.gossip.shutdown();
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        if( this.follow != null ){
            this.follow.shutdown();
        }else if( this.leader != null ){
            this.leader.shutdown();
        }
        if( this.tcpServer != null ){
            this.tcpServer.shutdown();
        }
        this.peerServerLogger.log(Level.WARNING, "ZooKeeperPeerServerImpl is shutting down");
    }

    @Override
    public void run(){
        try {
            //step 1: create and run thread that sends broadcast messages
            this.peerServerLogger.log(Level.FINE, "ZooKeeperPeerServerImpl is starting UDPMessageSender");
            startUDPMSGSender();
            //step 2: create and run thread that listens for messages sent to this server
            this.peerServerLogger.log(Level.FINE, "ZooKeeperPeerServerImpl is starting UDPMessageReceiver");
            startUDPMSGReceiver();
            //Create Gossip
            this.peerServerLogger.log(Level.FINE, "ZooKeeperPeerServerImpl is starting Gossip");
            startGossip();
        } catch (IOException e) {
            this.peerServerLogger.log(Level.WARNING, "IOException in UDPMSG class creation", e);
            return;
        }
        if( this.tcpServer == null ){
            //create our tcpServer connection
            startTCPServer();
        }
        //step 3: main server loop
        try{
            while (!this.shutdown.get()){
                switch (getPeerState()){
                    case LOOKING: case OBSERVER:
                        if( this.getCurrentLeader() == null ){
                            this.gossip.setPause();
                            beginElection();
                            this.gossip.applyTime();
                        }
                        break;
                    case FOLLOWING:
                        if( this.follow == null ){
                            this.peerServerLogger.log(Level.FINE, "ZooKeeperPeerServerImpl is starting JavaRunnerFollower");
                            startFollower();
                        }
                        break;
                    case LEADING:
                        if( this.leader == null ){
                            if( this.follow != null ){//I was previously a follower
                                this.follow.shutdown();
                                this.follow = null;
                            }
                            this.peerServerLogger.log(Level.FINE, "ZooKeeperPeerServerImpl is starting RoundRobinLeader");
                            startLeader();
                        }
                        break;
                }
            }
        }
        catch (Exception e) {
            this.peerServerLogger.log(Level.SEVERE, "Exception in ZooKeeperPeerServerImpl", e);
        }
        this.peerServerLogger.log(Level.WARNING, "Exiting ZooKeeperPeerServerImpl.run()");
    }

    private void startGossip() throws IOException{
        for( Map.Entry<Long, InetSocketAddress> peerEntry : this.peerIDtoAddress.entrySet()){
            this.monitering.put(peerEntry.getKey(), new BohemianRhapsody(peerEntry.getKey(), peerEntry.getValue()));
            this.addrToPeer.put(peerEntry.getValue(), peerEntry.getKey());
        }
        this.monitering.put(this.id, new BohemianRhapsody(this.id, this.myAddress));   
        this.gossip = new Gossiper(this, this.id, this.monitering, this.incomingMessages, this.addrToPeer);
        this.gossip.start();
    }

    private void startUDPMSGSender(){
        this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
        this.senderWorker.start();
    }

    private void startUDPMSGReceiver() throws IOException{
        this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
        this.receiverWorker.start();
    }

    private void startTCPServer(){
        try {
            this.peerServerLogger.log(Level.FINE, "ZooKeeperPeerServerImpl is starting TCPServer");
            this.tcpServer = new TCPServer(tcpWorkQueue, this.tcpPort, this.id);
            this.tcpServer.start();
        } catch (IOException e) {
            this.peerServerLogger.log(Level.WARNING, "IOException in ZooKeeperPeerServerImpl while running TCPServer");
        }
    }

    private void startFollower() throws IOException{
        this.follow = new JavaRunnerFollower(this.tcpWorkQueue, this.myAddress, this.tcpPort, this.id, this.peerIDtoAddress, this.currentLeader);
        this.follow.start();
    }
    private void startLeader() throws IOException{
        this.leader = new RoundRobinLeader(this.tcpWorkQueue, this.myAddress, this.tcpPort, this.id, this.peerIDtoAddress, this.gateWayID);
        this.leader.start();
    }

    private void beginElection(){
        this.peerServerLogger.log(Level.FINE, "ZooKeeperPeerServerImpl is starting Election");
        //start leader election, set leader to the election winner
        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this, this.incomingMessages);
        try {
            setCurrentLeader(election.lookForLeader());
            this.peerServerLogger.log(Level.INFO, "ZooKeeperPeerServerImpl set {0} as the leader", this.getCurrentLeader());
        } catch (IOException e) {
            this.peerServerLogger.log(Level.WARNING, "IOException in ZooKeeperPeerServerImpl while running Election");
        }
    }

    @Override
    public synchronized void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    @Override
    public synchronized Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        //send to target
        this.outgoingMessages.offer(
            new Message(type, 
                        messageContents, 
                        this.getAddress().getHostString(), 
                        this.myPort, 
                        target.getHostName(), 
                        target.getPort())
        );
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        // send to all peers
        for( Long peerID : this.peerIDtoAddress.keySet() ){
            this.sendMessage(type, messageContents, this.peerIDtoAddress.get(peerID));
        }
    }

    @Override
    public synchronized ServerState getPeerState() {
        return this.state;
    }

    @Override
    public synchronized void setPeerState(ServerState newState) {
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    public void incrementEpoch(){
        this.peerEpoch++;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    public int getTCPPort(){
        return this.tcpPort;
    }

    public String getHostName(){
        return this.myAddress.getHostName();
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    @Override
    public synchronized int getQuorumSize() {
        return ((this.peerIDtoAddress.size() / 2) + 1) - this.numberOfObservers;
    }

    @Override
    public synchronized void reportFailedPeer(long peerID) {
        InetSocketAddress peer = this.peerIDtoAddress.remove(peerID);
        if( peer == null ){
            this.peerServerLogger.log(Level.WARNING, "Attempting to remove a null peer");
            return;
        }
        this.addrToPeer.remove(peer);
        if( this.leader != null && peerID != this.gateWayID ){//if I'm the leader
            this.leader.removeWorker(peer);
            this.peerServerLogger.log(Level.WARNING, "Leader removed worker {0}", peerID);
        }else{//I'm not the leader
            this.peerServerLogger.log(Level.WARNING, "Follower removed peer {0}", peerID);
        }
    }

    @Override
    public synchronized boolean isPeerDead(long id){
        if( this.monitering.get(id) == null){//it was cleaned
            return false;
        }
        return this.monitering.get(id).isFailed();//is it failed?
    }
}