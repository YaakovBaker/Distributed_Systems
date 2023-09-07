package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Message.MessageType;
import java.util.logging.Logger;
import java.util.logging.Level;

public class RoundRobinLeader extends Thread implements LoggingServer{
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private LinkedBlockingQueue<InetSocketAddress> workers;
    private ConcurrentMap<InetSocketAddress, ArrayList<ClientRequest>> workerToWorkMap;
    private LinkedBlockingQueue<ClientRequest> tcpWorkQueue;
    private final InetSocketAddress myAddress;
    private final int tcpPort;
    private final Long gatewayID;
    private Logger rrlLogger;
    private ExecutorService executor;
    private ConcurrentMap<Long,InetSocketAddress> peerIDtoAddress;
    private ConcurrentMap<Long, BohemianRhapsody> monitering;
    private ConcurrentMap<Long, Message> reqIDMap;

    /**connects to the follower via TCP, sends requests to the follower, waits for a reply,
    * sends the reply back to the gateway, and removes the request from the leader's data structures*/
    class RequestCompletor implements Runnable{
        private ClientRequest request;
        RequestCompletor(ClientRequest request){
            this.request = request;
        }

        @Override
        public void run() {
            //The connection to the gatewayserver client
            Socket clientConnection = request.getClientConnection();
            //work from gatewayserver client
            Message workToDo = request.getMessage();
            //check map to see if this is a mabobler
            if( reqIDMap.containsKey(workToDo.getRequestID()) ){
                //send to gway
                try {
                    Message msgFromJRF = reqIDMap.remove(workToDo.getRequestID());
                    Message forGateway = new Message(MessageType.COMPLETED_WORK, msgFromJRF.getMessageContents(), myAddress.getHostName(), tcpPort, workToDo.getSenderHost(), workToDo.getSenderPort(), workToDo.getRequestID());
                    clientConnection.getOutputStream().write(forGateway.getNetworkPayload());//return to the gateway
                    rrlLogger.log(Level.FINE, "Message sent back to gateway:\n{0}", forGateway);
                } catch (IOException e) {
                    rrlLogger.log(Level.SEVERE, "IOException in RoundRobinLeader when writing to output stream of gateway client connection", e);
                }
            }else{
                if( Util.isWork(workToDo.getMessageType()) ){
                    job(clientConnection, workToDo);
                }else{
                    rrlLogger.log(Level.WARNING,"RRL received a non-Work Message from work queue");
                }
            }
            rrlLogger.log(Level.WARNING,"Exiting RoundRobinLeader.RequestCompletor.run()");
        }
        private synchronized InetSocketAddress getWorker(){
            InetSocketAddress worker = workers.remove();
            workers.add(worker);
            return worker;
        }
        private void job(Socket clientConnection, Message workToDo){
            // what if remove and add in between we remove again?
            //synchronize the block?
            //don't remove?
            //i think since its a linkedblocking queue then this remove will remove it
            //and add it but it won't be from a dead guy
            //need to do a check maybe of if failed?
            //get the worker to recieve the message
            InetSocketAddress worker = getWorker();

            //how do I specify that this worker will receive the work?
            //answer: use its tcpPort for clientsocket
            int jrfTCPPort = worker.getPort() + 2;
            String jrfHostName = worker.getHostName();  
            //construct the message to send to follower
            Message workForYou = new Message(MessageType.WORK, workToDo.getMessageContents(), myAddress.getHostString(), tcpPort, jrfHostName, jrfTCPPort, workToDo.getRequestID());//+2 for TCP port
            //send over TCP
            rrlLogger.log(Level.FINE, "Creating TCP client connection from RRL -> JRF");
            try (
                //tcp client connection RRL -> JRF
                Socket clientRRLSocket = new Socket(jrfHostName, jrfTCPPort); 
                OutputStream clientRRLSocketOutput = clientRRLSocket.getOutputStream();
                InputStream clientRRLSocketInput = clientRRLSocket.getInputStream();
            ){
                clientRRLSocketOutput.write(workForYou.getNetworkPayload());
                rrlLogger.log(Level.INFO, "Wrote message network payload to JRF to compile\n{0}", workForYou);
                //JRF returned work to RRL here instead of TCPServer
                //when we receive the completed work return the work to the clientConnection (gateway)
                byte[] contentsToSend = Util.readAllBytesFromNetwork(clientRRLSocketInput);
                sendMessageVIATCP(contentsToSend, clientConnection, workToDo, workForYou);
            }catch (Exception e){
                rrlLogger.log(Level.SEVERE, "Exception caught while trying to send TCP work message", e);
            }
        }

        private void sendMessageVIATCP(byte[] contentsToSend, Socket clientConnection, Message workDid, Message workForYou){
            Message msgFromJRF = new Message(contentsToSend);
            Message forGateway = new Message(MessageType.COMPLETED_WORK, msgFromJRF.getMessageContents(), myAddress.getHostName(), tcpPort, workDid.getSenderHost(), workDid.getSenderPort(), workForYou.getRequestID());
            try {    
                clientConnection.getOutputStream().write(forGateway.getNetworkPayload());//return to the gateway
                rrlLogger.log(Level.FINE, "Message sending back to gateway:\n{0}", forGateway);
            } catch (IOException e) {
                rrlLogger.log(Level.SEVERE, "IOException in RoundRobinLeader when writing to output stream of gateway client connection", e);
            }
        }
    }//RequestorCompletor

    public RoundRobinLeader(LinkedBlockingQueue<ClientRequest> tcpWorkQueue, InetSocketAddress myAddress, int tcpPort, Long serverID, ConcurrentMap<Long,InetSocketAddress> peerIDtoAddress, Long gatewayID) throws IOException{
        this.tcpWorkQueue = tcpWorkQueue;
        this.myAddress = myAddress;
        this.tcpPort = tcpPort;
        this.gatewayID = gatewayID;
        this.peerIDtoAddress = peerIDtoAddress;
        this.reqIDMap = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(N_THREADS,  r -> {
            Thread complete = new Thread(r);
            complete.setDaemon(true);
            return complete;});
        this.setDaemon(true);
        this.rrlLogger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-serverID-" + serverID + "-on-tcpPort-" + this.tcpPort + "-Log.txt");
        setName("RoundRobinLeader-tcpPort-" + this.tcpPort);
        this.workers = loadWorkerInetAddresses();
        this.workerToWorkMap = createWorkMap();
        this.rrlLogger.log(Level.FINE, "RoundRobinLeader Construction Finished!");
    }
    
    private LinkedBlockingQueue<InetSocketAddress> loadWorkerInetAddresses() {
        LinkedBlockingQueue<InetSocketAddress> addresses = new LinkedBlockingQueue<>();
        for( Long id : this.peerIDtoAddress.keySet() ){
            if( !id.equals(this.gatewayID) ){//ignore gateway for work
                addresses.add(this.peerIDtoAddress.get(id));
            }
        }
        return addresses;
    }

    private ConcurrentHashMap<InetSocketAddress, ArrayList<ClientRequest>> createWorkMap(){
        ConcurrentHashMap<InetSocketAddress, ArrayList<ClientRequest>> workMap = new ConcurrentHashMap<>();
        for( InetSocketAddress addr : this.workers ){
            workMap.put(addr, new ArrayList<>());
        }
        return workMap;
    }

    public void shutdown(){
        interrupt();
        executor.shutdownNow();
        this.rrlLogger.log(Level.WARNING, "RoundRobinLeader ShuttingDown");
    }
    
    @Override
    public void run() {
        //NEW_LEADER_GETTING_LAST_WORK message sent out
        newGuy();
        while( !this.isInterrupted() ){
            ClientRequest workToDo = null;
            try {//take from the queue of work provided by the tcp server
                workToDo = this.tcpWorkQueue.take();
            } catch (InterruptedException e) {
                this.rrlLogger.log(Level.SEVERE, "Exception caught while trying to receive work message", e);
                Thread.currentThread().interrupt();
                break;
            }
            executor.execute(new RequestCompletor(workToDo));
        }
        this.rrlLogger.log(Level.WARNING,"Exiting RoundRobinLeader.run()");
    }

    public synchronized void removeWorker(InetSocketAddress peer){
        
        if( this.workers.contains(peer) ){
            //remove that worker
            this.workers.remove(peer);
            //reassign any client request work it had given the dead node to a different node
            //get all work you gave it and delete from map
            ArrayList<ClientRequest> failureWork = this.workerToWorkMap.remove(peer);
            //reassign
            for( ClientRequest request : failureWork ){
                executor.execute(new RequestCompletor(request));
            }
        }else{
            this.rrlLogger.log(Level.SEVERE, "I'm getting that removeWorker error");
        }
    }

    private void newGuy(){
        byte[] cont = "I need compelted_work".getBytes();
        //go through each worker and send the NEW_LEADER_GETTING_LAST_WORK message
        InetSocketAddress[] followers = this.workers.toArray(new InetSocketAddress[0]);
        for( InetSocketAddress f : followers ){
            String fHost = f.getHostName();
            int fPort = f.getPort() + 2;
            //message for that guy
            Message nG = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, cont, myAddress.getHostName(), tcpPort, fHost, fPort);
            //send to that follower
            //send over TCP
            rrlLogger.log(Level.FINE, "Creating TCP client connection from RRL -> JRF");
            try (
                //tcp client connection RRL -> JRF
                Socket clientRRLSocket = new Socket(fHost, fPort); 
                OutputStream clientRRLSocketOutput = clientRRLSocket.getOutputStream();
                InputStream clientRRLSocketInput = clientRRLSocket.getInputStream();
            ){
                //send to him
                clientRRLSocketOutput.write(nG.getNetworkPayload());
                //returned
                while( true ){
                    byte[] contentsReceived = Util.readAllBytesFromNetwork(clientRRLSocketInput);
                    if( contentsReceived == null ){
                        break;
                    }
                    Message msgFromJRF = new Message(contentsReceived);
                    if( Util.isFinalWOrk(msgFromJRF.getMessageType()) ){
                        break;
                    }
                    rrlLogger.log(Level.INFO, "Placing Message:\n"+ msgFromJRF);
                    this.reqIDMap.put(msgFromJRF.getRequestID(), msgFromJRF);
                }
            }catch (Exception e){
                rrlLogger.log(Level.SEVERE, "Exception caught while trying to send or receive TCP work message", e);
            }
        }
    }
}