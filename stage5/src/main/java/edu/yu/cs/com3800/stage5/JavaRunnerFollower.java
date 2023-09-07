package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.Message.MessageType;
import java.util.logging.Level;
import java.util.logging.Logger;
public class JavaRunnerFollower extends Thread implements LoggingServer{
    private LinkedBlockingQueue<ClientRequest> tcpWorkQueue;
    private JavaRunner whyAreYouRunning;
    private final InetSocketAddress myAddress;
    private final int tcpPort;
    private Logger jrfLogger;
    private Queue<Message> newLWOrk;
    private ConcurrentMap<Long,InetSocketAddress> peerIDtoAddress;
    private volatile Vote currentLeader;

    public JavaRunnerFollower(LinkedBlockingQueue<ClientRequest> tcpWorkQueue, InetSocketAddress myAddress, int tcpPort, Long serverID, ConcurrentMap<Long,InetSocketAddress> peerIDtoAddress, Vote currentLeader) throws IOException{
        this.tcpWorkQueue = tcpWorkQueue;
        this.peerIDtoAddress = peerIDtoAddress;
        this.currentLeader = currentLeader;
        this.myAddress = myAddress;
        this.tcpPort = tcpPort;
        this.newLWOrk = new LinkedList<>();
        this.setDaemon(true);
        this.jrfLogger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-serverID-" + serverID + "-on-tcpPort-" + this.tcpPort + "-Log.txt");
        setName("JavaRunnerFollower-tcpPort-" + this.tcpPort);
        try {
            this.whyAreYouRunning = new JavaRunner();
        } catch (IOException e) {
            this.jrfLogger.log(Level.SEVERE, "failed to create Java Runner", e);
        }
        this.jrfLogger.log(Level.FINE, "JavaRunnerFollower Construction Finished!");
    }

    public void shutdown(){
        interrupt();
        this.jrfLogger.log(Level.WARNING, "JavaRunnerFollower Shutting down");
    }

    @Override
    public void run(){
        while( !this.isInterrupted() ){
            ClientRequest request = null;
            try{
                request = this.tcpWorkQueue.take();
            }catch( InterruptedException e ){
                this.jrfLogger.log(Level.WARNING, "Exception caught while trying to receive UDP packet", e);
                Thread.currentThread().interrupt();
                break;
            }
            //The connection to the RRL client
            Socket clientConnection = request.getClientConnection();
            //work from RRL client
            Message workToDo = request.getMessage();
            if( Util.isWork(workToDo.getMessageType()) ){//work from leader
                job(clientConnection, workToDo);
            }else if( Util.isNewWork(workToDo.getMessageType()) ){//new leader requesting completed work
                newLeaderRequest(clientConnection, workToDo);
            }else{
                //we can only get work types so...
                jrfLogger.log(Level.WARNING,"JRF received a non-Work Message from work queue");
            }
        }
        this.jrfLogger.log(Level.WARNING,"Exiting JavaRunnerFollower.run()");
    }


    private void job(Socket clientConnection, Message workToDo){
        //get the code to run
        InputStream code = new ByteArrayInputStream(workToDo.getMessageContents());
        //run the code
        String responseCode = null;
        boolean error = false;
        String leaderHost = workToDo.getSenderHost();
        int leaderPort = workToDo.getSenderPort();
        try {
            responseCode = whyAreYouRunning.compileAndRun(code);
            jrfLogger.log(Level.FINE, "compile and run was successful\n{0}", responseCode);
        } catch (Exception e) {
            responseCode = Util.getStackTrace(e);
            jrfLogger.log(Level.SEVERE, "Exception caught while trying to compile and run code\n{0}", responseCode);
            error = true;
        }
        if( responseCode != null ){
            //construct a return message to the master
            Message forMaster = new Message(MessageType.COMPLETED_WORK, responseCode.getBytes(), myAddress.getHostName(), tcpPort, leaderHost, leaderPort, workToDo.getRequestID(), error);
            //instead of being a client just be the server that returns to RRL client
            //JRF ServerSocket -> RRL ClientSocket
            //only send if the leader isn't null and isn't failed
            if( this.currentLeader != null && this.peerIDtoAddress.containsKey(this.currentLeader.getProposedLeaderID()) ){
                try {
                    clientConnection.getOutputStream().write(forMaster.getNetworkPayload());//return to master
                    jrfLogger.log(Level.FINE, "Sent network payload to master:\n {0}", forMaster);
                } catch (IOException e) {
                    jrfLogger.log(Level.SEVERE, "IOException caught while writing to output stream\n{0}", Util.getStackTrace(e));
                    //put work in a queue
                    //it died so save for later
                    //so if leader dies and requests for work we can give the works
                    newLWOrk.add(forMaster);
                }
            }else{
                //put work in a queue
                //it died so save for later
                //so if leader dies and requests for work we can give the works
                newLWOrk.add(forMaster);
            }
        }
    }

    private void newLeaderRequest(Socket clientConnection, Message workToDo){
        String leadHost = workToDo.getSenderHost();
        int leadPort = workToDo.getSenderPort(); 
        while( !newLWOrk.isEmpty() ){
            //remove the old message
            Message oldM = newLWOrk.remove();
            //construct message for the newLeader
            Message toSend = new Message(MessageType.COMPLETED_WORK, oldM.getMessageContents(), myAddress.getHostName(), tcpPort, leadHost, leadPort, oldM.getRequestID());
            //send him those messages with his connection
            try {
                clientConnection.getOutputStream().write(toSend.getNetworkPayload());//return to master
                jrfLogger.log(Level.FINE, "Sent network payload to master:\n {0}", toSend);
            } catch (IOException e) {
                jrfLogger.log(Level.SEVERE, "IOException caught while writing to output stream\n{0}", Util.getStackTrace(e));
            }
        }
        Message terminateM = new Message(MessageType.FINAL_WORK_FROM_FOLLOWER, "Final Work".getBytes(), myAddress.getHostName(), tcpPort, leadHost, leadPort);
        //send him those messages with his connection
        try {
            clientConnection.getOutputStream().write(terminateM.getNetworkPayload());
            jrfLogger.log(Level.INFO, "Sent terminating message");
        } catch (IOException e) {
            jrfLogger.log(Level.SEVERE, "IOException caught while writing to output stream\n{0}", Util.getStackTrace(e));
        }
    }
}
