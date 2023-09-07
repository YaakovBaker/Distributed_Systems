package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.Util;

public class Gossiper extends Thread implements LoggingServer{
    private Logger gossipLogger;
    private Logger verboseLogger;
    private static final int GOSSIP = 2000;
    private static final int FAIL = GOSSIP * 15;
    private static final int CLEANUP = FAIL * 2;
    private BohemianRhapsody myRhapsody;
    private ConcurrentMap<Long, BohemianRhapsody> monitering;
    private ConcurrentMap<InetSocketAddress, Long> addrToPeer;
    private ZooKeeperPeerServerImpl myServer;
    private LinkedBlockingQueue<Message> incomingMessages;
    private AtomicBoolean paused;
    private Long myID;
    private GossipHttpServer gHTTP;

    public Gossiper(ZooKeeperPeerServerImpl myServer, Long myID, ConcurrentMap<Long, BohemianRhapsody> monitering, LinkedBlockingQueue<Message> incomingMessages, ConcurrentMap<InetSocketAddress, Long> addrToPeer) throws IOException{
        this.setDaemon(true);
        this.myID = myID;
        this.monitering = monitering;
        this.addrToPeer = addrToPeer;
        this.myServer = myServer;
        this.incomingMessages = incomingMessages;
        this.paused = new AtomicBoolean(false);
        this.myRhapsody = this.monitering.get(this.myID);
        this.gossipLogger = initializeLogging(Gossiper.class.getCanonicalName() + "-on-serverID-" + this.myID + "-on-udpPort-" + myServer.getUdpPort() + "-Log.txt");
        setName("Gossip-udpPort-" + myServer.getUdpPort());
        this.verboseLogger = initializeLogging(Gossiper.class.getCanonicalName() + "-VerboseLogger-on-serverID-" + this.myID + "-on-udpPort-" + myServer.getUdpPort() + "-Log.txt");
        // this.gHTTP = new GossipHttpServer(this.gossipLogger.getName(), this.verboseLogger.getName(), myServer);
        // this.gHTTP.start();
        this.gossipLogger.log(Level.FINE, "Finished Gossip construction");
    }

    public void setPause(){
        this.paused.set(true);
    }

    /*
     * After an election we need to reapply receive times to the current time becuase otherwise
     * it is possible that the election took long enough that we will mark nodes as failed
     */
    public void applyTime(){
        //wakeup after election
        long newTime = System.currentTimeMillis();
        for( Map.Entry<Long, BohemianRhapsody> peerBeat : this.monitering.entrySet() ){
            //go through map and replace timeRCVD so issue don't arrive
            peerBeat.getValue().replaceTimeReceived(newTime);
            
        }
        this.paused.set(false);
    }

    /*
     * Interrupt this thread to shutdown run
     */
    public void shutdown(){
        this.paused.set(true);
        this.interrupt();
        this.gossipLogger.log(Level.WARNING, "Gossiper-udpPort-{0} shutting down", this.myRhapsody.getAddr().getPort());
    }

    @Override
    public void run(){
        while( !this.isInterrupted() ){
            //while we are not paused for election
            while( !this.paused.get() ){
                long currentTime = System.currentTimeMillis();
                //increment my heartbeat
                this.myRhapsody.incrementBeat();
                this.myRhapsody.replaceTimeReceived(currentTime);
                //check incoming queue for messages
                this.gossipLogger.log(Level.FINE, "processing Incoming Messages");
                processIncomingMessages(this.incomingMessages.toArray(new Message[0]), currentTime);
                //fail and cleanup
                fail(currentTime);
                clean(currentTime);
                //notify a random peer
                sendHeartBeat();
                //wait GOSSIP time
                try {
                    Thread.sleep(GOSSIP);
                }catch (InterruptedException e){
                    this.verboseLogger.log(Level.SEVERE, "Exception caught while trying to sleep", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        this.verboseLogger.log(Level.SEVERE, "Exiting Gossiper.run()");
    }

    /**
     * @param - The Message[] to process
     * Process incoming messages that at this moment were found when
     * we called toArray() on incomingMessages
     */
    private void processIncomingMessages(Message[] incMsgs, long currentTime){
        this.verboseLogger.log(Level.FINE, "There are {0} incoming messages to proccess", incMsgs.length);
        for( int m = 0; m < incMsgs.length; m++ ){
            Message newMSG = incMsgs[m];
            
            if( Util.isGossip(newMSG.getMessageType()) ){
                this.verboseLogger.log(Level.FINE, "About to Process Gossip Message");
                processGossip(newMSG, currentTime);
                this.incomingMessages.remove(newMSG);
            }else{
                this.verboseLogger.log(Level.WARNING, "Received a message that was neither GOSSIP or ELECTION");
            }
        }
    }

    /**
     * @param - The GOSSIP message to process
     */
    private synchronized void processGossip(Message gossip, long currentTime){
        InetSocketAddress peerAddr = new InetSocketAddress(gossip.getSenderHost(), gossip.getSenderPort());
        //deserialize the map from the message contents
        ConcurrentMap<Long, BohemianRhapsody> deserializedMoniterMap = deserializeMap(gossip.getMessageContents());
        //go through peer map and see if we need to update
        for(Map.Entry<Long, BohemianRhapsody> peerEntry : deserializedMoniterMap.entrySet()){
            //peer version
            BohemianRhapsody peerRhapsody = peerEntry.getValue();
            //my version
            BohemianRhapsody myVersion = this.monitering.get(peerEntry.getKey());
            if( (this.monitering.containsKey(peerEntry.getKey())) && //if I have this peer and
                (newDataCheck(peerRhapsody, myVersion)) ){//it has new Data
                this.gossipLogger.log(Level.INFO, () -> this.myID + ": updated " + peerEntry.getKey() + "'s heartbeat count to " + peerRhapsody.getBeat() +
                " based on message from " + this.addrToPeer.get(peerAddr) + " at node time " + this.myRhapsody.getBeat());
                //replace
                this.monitering.get(peerEntry.getKey()).replaceBeat(peerRhapsody.getBeat());
                //set time rcvd to now
                this.monitering.get(peerEntry.getKey()).replaceTimeReceived(currentTime);
            }   
        }
        this.verboseLogger.log(Level.INFO, "After Fusion of conents:\n {0}", this.monitering);
    }

    /**
     * If we both think it hasn't failed 
     * AND
     * his data is newer
     * @param peerRhapsody
     * @param myVersion
     * @return
     */
    private boolean newDataCheck(BohemianRhapsody peerRhapsody, BohemianRhapsody myVersion){
        return (!myVersion.isFailed()) &&               //I say its not failed and
        (peerRhapsody.getBeat() > myVersion.getBeat()); //if his data is newer
    }

    /**
     * 
     * @param contents is the serialized HeartBeat data map from my peer
     * @return a deserialized map of my peer's HeartBeat data
     */
    private ConcurrentMap<Long, BohemianRhapsody> deserializeMap(byte[] contents){
        try(
            ByteArrayInputStream byteInStream = new ByteArrayInputStream(contents);
            ObjectInputStream objInStream = new ObjectInputStream(byteInStream);
        ){
            Object obj = objInStream.readObject();
            ConcurrentHashMap<Long, BohemianRhapsody> deserMap = (ConcurrentHashMap<Long, BohemianRhapsody>)(obj);
            this.verboseLogger.log(Level.WARNING, "Contents of deserialized map need to be checked for consistency:\n {0}", deserMap);
            return deserMap;
        }catch (IOException  e) {
            this.verboseLogger.log(Level.SEVERE, "IOException in InputStream creation or reading", e);
        }catch(ClassNotFoundException e){
            this.verboseLogger.log(Level.SEVERE, "ClassNotFoundException in InputStream reading", e);
        }
        return new ConcurrentHashMap<>();  
    }

    /*
     * Check the servers that are being monitrered to see
     * if any need to be set as failed
     * and if any need to be set as cleaned
     */
    private synchronized void fail(long currentTime){
        //mark peer as failed if they haven't updated in fail time
        for(Map.Entry<Long, BohemianRhapsody> peer : this.monitering.entrySet()){
            BohemianRhapsody peerRhapsody = peer.getValue();
            if( failable(peer.getKey(), peerRhapsody, currentTime) ){
                //Whenever a node discovers via missing heartbeats that another node has failed, log and System.out.println “[insert 
                //this node’s ID here]: no heartbeat from server [insert dead server ID here] – server failed
                String failedMessage = this.myID + ": no hearbeat from server " + peer.getKey() + " - server failed at time: " + currentTime + " with time difference " + (currentTime - peerRhapsody.getTimeRCVD()) ;
                this.gossipLogger.log(Level.WARNING, failedMessage);
                System.out.println(failedMessage);
                failedRhapsody(peer.getKey(), peerRhapsody);
            }
        }
    }

    private synchronized void clean(long currentTime){
        for(Map.Entry<Long, BohemianRhapsody> peer : this.monitering.entrySet()){
            BohemianRhapsody peerRhapsody = peer.getValue();
            if( cleanable(peer.getKey(), peerRhapsody, currentTime) ){
                //remove from monitering
                this.monitering.remove(peer.getKey());
            }
        }   
    }

    /**
     * @param peerRhapsody the peer to examine if it needs to be marked as failed
     * @return true if it has been FAIL time or more since receiving an update from this peer
     * OTHERWISE
     * false
     */
    private boolean failable(Long serverID, BohemianRhapsody peerRhapsody, long currentTime){
        return  (this.monitering.containsKey(serverID)) &&//the server exists in monitering
                (!peerRhapsody.isFailed()) &&//this server isn't already failed
                ((currentTime - peerRhapsody.getTimeRCVD()) >= FAIL);// and it can fail now
    }

    /**
     * @param peerRhapsody the peer to examine if it needs to be cleaned
     * @return true if it has been CLEANUP time or more since receiving an update from this peer
     * and only if it has failed
     * OTHERWISE
     * false
     */
    private boolean cleanable(Long serverID, BohemianRhapsody peerRhapsody, long currentTime){
        return  (this.monitering.containsKey(serverID)) &&//the server exists in monitering
                (peerRhapsody.isFailed()) &&//if its already failed
                (currentTime - peerRhapsody.getTimeRCVD() >= CLEANUP);//and it's cleanup time
    }

    /**
     * @param serverID of server that failed
     * @param peerRhapsody is the HeartBeat data of that failed server to examine
     */
    private void failedRhapsody(Long serverID, BohemianRhapsody peerRhapsody){
        //set as failed
        peerRhapsody.setFailed();
        
        //find my proposed leader
        Vote propLeader = this.myServer.getCurrentLeader();
        //if my proposed leader isn't null
        if( propLeader != null ){
            Long leaderID = propLeader.getProposedLeaderID();
            this.myServer.reportFailedPeer(serverID);
            if( leaderID.equals(serverID) ){//He was the leader
                deadLeaderLogic();
            }else{
                this.verboseLogger.log(Level.WARNING, "Fellow Follower, {0}, Died", serverID);
            }
        }else{
            this.verboseLogger.log(Level.WARNING, "Leader is null and is not able to change state");
        }
    }

    /**
     * Leader died so must do logic according to what I am
     */
    private void deadLeaderLogic(){
        //run election will happen automatically since case LOOKING or OBSERVER and leader null
        try {
            this.myServer.setCurrentLeader(null);
        } catch (IOException e) {
            this.verboseLogger.log(Level.WARNING, "IOException in setting current leader", e);
        }
        //increase epoch
        this.myServer.incrementEpoch();
        //change state if follower
        if( Util.isObserving(this.myServer.getPeerState()) ){
            this.gossipLogger.log(Level.INFO, this.myID + ": is re-electing at epoch " + this.myServer.getPeerEpoch());
        }else if( Util.isFollowing(this.myServer.getPeerState()) ){//I'm a JRF
            // Whenever a node switches its state (e.g. due to leader failure), it should log & System.out.println “[insert this node’s 
            // ID here]: switching from [old state] to [new state]”
            String stateChange = this.myID + ": switching from " + this.myServer.getPeerState() + " to " + ServerState.LOOKING;
            this.gossipLogger.log(Level.INFO, stateChange);
            System.out.println(stateChange);
            // b. change their sever state to LOOKING, increase their epoch by one, and run an election
            this.myServer.setPeerState(ServerState.LOOKING);
        }
    }
    
    /**
     * Send your HeartBeat data to a random peer
     */
    private void sendHeartBeat(){
        int ceiling = monitering.size();
        //choose a random number from 0 till ceiling
        long randomID = (long)(Math.random() * ceiling);
        //if it doesn't exist anymore or is failed retry
        while( (!this.monitering.containsKey(randomID)) || (this.monitering.get(randomID).isFailed()) ){
            randomID = (long)(Math.random() * ceiling);
        }
        //get the address
        InetSocketAddress recieverAddress = this.monitering.get(randomID).getAddr();
        //send via udp
        this.myServer.sendMessage(MessageType.GOSSIP, serializeMap(), recieverAddress);
    }

    /**
     * @return a byte[] that signifies the serialized moniter map
     * otherwise send an empty byte[]
     */
    private byte[] serializeMap(){
        ConcurrentMap<Long, BohemianRhapsody> nonFailed = new ConcurrentHashMap<>();
        for( Map.Entry<Long, BohemianRhapsody> entry: this.monitering.entrySet() ){
            if( !entry.getValue().isFailed() ){
                nonFailed.put(entry.getKey(), entry.getValue());
            }
        }
        try (
            ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
            ObjectOutputStream outStream = new ObjectOutputStream(byteOutStream);
        ){
            outStream.writeObject(nonFailed);
            outStream.flush();
            return byteOutStream.toByteArray();
        } catch (IOException e) {
            this.verboseLogger.log(Level.WARNING, "IOException during serialization", e);
        }
        this.verboseLogger.log(Level.WARNING, "Sending the empty byte[]");
        return new byte[0];  
    }
}