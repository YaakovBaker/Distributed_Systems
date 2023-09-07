package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.Message.MessageType;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

public class GatewayServer extends Thread implements LoggingServer{
    private static final String LOCAL = "localhost";
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final String NO_LEADER = "No Leader";
    private int httpPort;
    private GatewayPeerServerImpl gwayServer;//used in handler, and methods called on it
    private HttpServer httpServer;
    private int tcpPort;//used in handler but only got
    private AtomicLong requestID;
    private ConcurrentMap<Long,InetSocketAddress> peerIDtoAddress;
    private ConcurrentMap<InetSocketAddress, Long> addrToPeer;
    private Long gatewayID;

    static ThreadLocal<Logger> handlerLoggerLocal = ThreadLocal.withInitial(() -> {
        try {
            String name = GatewayServer.class.getCanonicalName() + "-ClientRequestHandler-" + Thread.currentThread().getId() + "-Log.txt";
            return LoggingServer.createLogger(name, name, false);
        }catch(IOException e){
            return null;
        }});
    /*
     * This class functions as an HTTP Server that the Client (in the test code)
     * will communicate work messages to.
     * This class also functions as a TCP Client and
     * will communicate work messages to the TCPServer, 
     * which will then give the work message to the RRL.
     */
    class MyHandler implements HttpHandler {

        MyHandler(){
        }

        private void wrongContent(HttpExchange request) throws IOException{
            String response = "error 400";
            request.sendResponseHeaders(400, response.length());
            OutputStream os = request.getResponseBody();
            os.write(response.getBytes());
            os.close();
            request.close();
            handlerLoggerLocal.get().log(Level.SEVERE, () -> 400 + " " + response);
        }

        private void notPost(HttpExchange request) throws IOException{
            request.sendResponseHeaders(405, -1);
            request.close();
            handlerLoggerLocal.get().log(Level.SEVERE, 405 + " " + "not post or get method");
        }

        private void post(HttpExchange request){
            //request contents
            InputStream is = request.getRequestBody();
            byte[] code = Util.readAllBytes(is);
            
            //could change to jsut keep trying to submit just this request until it works
            //1)3) If Leader is null or failed/dead spin until that is resolved
            Vote currentLeader = null;
            while( (currentLeader = gwayServer.getCurrentLeader()) == null ){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    handlerLoggerLocal.get().log(Level.WARNING, "Interruped Exception while trying to find leader\n{0}", Util.getStackTrace(e));
                    return;
                }
            }
            //leader isn't null anymore
            InetSocketAddress leaderAddress = gwayServer.getPeerByID(currentLeader.getProposedLeaderID());
            int leaderTCPPort = leaderAddress.getPort() + 2;
            String leaderHostName = leaderAddress.getHostName();
            Message response = null;
            //increment requestID
            // 2) Any message sent needs to be put in a RequestID to Message map
            // *   2a) becuase if any failure happens need to resend work to new leader
            // *   2b) Need to incorperate requestIDs at this layer
            long myReqID = requestID.getAndIncrement();
            while( response == null ){
                try (
                    //use a tcp connection
                    Socket gatewaySocket = new Socket(leaderHostName, leaderTCPPort);
                    OutputStream socketOutput = gatewaySocket.getOutputStream();
                    InputStream socketInput = gatewaySocket.getInputStream();
                ){
                    //message for our specified leader
                    Message workMessage = new Message(MessageType.WORK, code, LOCAL, tcpPort, leaderHostName, leaderTCPPort, myReqID);
                    
                    socketOutput.write(workMessage.getNetworkPayload());
                    handlerLoggerLocal.get().log(Level.INFO, "Message sent to Leader:\n {0}", workMessage);
                    //recieve response from master
                    byte[] net = Util.readAllBytesFromNetwork(socketInput);
                    response = new Message(net);
                }catch (IllegalArgumentException e){
                    String except = Util.getStackTrace(e) + "\n";
                    try {
                        request.sendResponseHeaders(400, 0);
                    } catch (IOException e1) {
                        handlerLoggerLocal.get().log(Level.WARNING, Util.getStackTrace(e1));
                    }
                    OutputStream os = request.getResponseBody();
                    PrintStream ps = new PrintStream(os);
                    try {
                        os.write(except.getBytes());
                    } catch (IOException e1) {
                        handlerLoggerLocal.get().log(Level.WARNING, Util.getStackTrace(e1));
                    }
                    e.printStackTrace(ps);
                    ps.close();
                    try {
                        os.close();
                    } catch (IOException e1) {
                        handlerLoggerLocal.get().log(Level.WARNING,Util.getStackTrace(e1));
                    }
                    request.close();
                    handlerLoggerLocal.get().log(Level.SEVERE, () -> 400 + " " + except);
                    return;
                }catch( IOException e ){
                    //4) responses from a non-leader (dead/failed server) should be ignored and that work resent
                    //check leader death
                    try {
                        Thread.sleep(20000);
                    } catch (InterruptedException e1) {
                        handlerLoggerLocal.get().log(Level.WARNING, "Interruped Exception while trying to find leader\n{0}", Util.getStackTrace(e1));
                        return;
                    }
                    if( gwayServer.isPeerDead(currentLeader.getProposedLeaderID()) ){//if leader is dead
                        while( (currentLeader = gwayServer.getCurrentLeader()) == null ){//wait for new leader
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException e1) {
                                handlerLoggerLocal.get().log(Level.WARNING, "Interruped Exception while trying to find leader\n{0}", Util.getStackTrace(e1));
                                return;
                            }
                        }
                        //now that we have a new leader need to redo the data
                        leaderAddress = gwayServer.getPeerByID(currentLeader.getProposedLeaderID());
                        leaderTCPPort = leaderAddress.getPort() + 2;
                        leaderHostName = leaderAddress.getHostName();
                    }
                }
            }
            //send back to client
            handlerLoggerLocal.get().log(Level.INFO, "Response Message from Leader:\n{0}", response);
            try {
                request.sendResponseHeaders(200, response.getMessageContents().length);
                OutputStream os = request.getResponseBody();
                os.write(response.getMessageContents());
                os.close();
                request.close();
                handlerLoggerLocal.get().log(Level.INFO, 200 + " {0}", response);
            } catch (IOException e) {
                String except = Util.getStackTrace(e) + "\n";
                try {
                    request.sendResponseHeaders(400, 0);
                } catch (IOException e1) {
                    handlerLoggerLocal.get().log(Level.WARNING,Util.getStackTrace(e1));
                }
                OutputStream os = request.getResponseBody();
                PrintStream ps = new PrintStream(os);
                try {
                    os.write(except.getBytes());
                } catch (IOException e1) {
                    handlerLoggerLocal.get().log(Level.WARNING,Util.getStackTrace(e1));
                }
                e.printStackTrace(ps);
                ps.close();
                try {
                    os.close();
                } catch (IOException e1) {
                    handlerLoggerLocal.get().log(Level.WARNING,Util.getStackTrace(e1));
                }
                request.close();
                handlerLoggerLocal.get().log(Level.SEVERE, () -> 400 + " " + except);
            }
        }

        public void handle(HttpExchange request) throws IOException {
            String reqMethod = request.getRequestMethod();
            Headers headerContent = request.getRequestHeaders();
            //if this is a POST method
            if( reqMethod.equals("POST") ){
                //If header content isn't that string
                if( !headerContent.get("Content-Type").contains("text/x-java-source") ){
                    wrongContent(request);
                    return;
                }
                post(request);
            }else if( reqMethod.equals("GET") ){
                if( !headerContent.get("Content-Type").contains("get-leader") ){
                    wrongContent(request);
                    return;
                }
                getLeader(request);
            }else{//not post or get
                notPost(request);
            }
        }

        private void getLeader(HttpExchange request){
            Vote currentVote = gwayServer.getCurrentLeader();
            try {
                if( currentVote == null ){
                    //send back no leader
                    request.sendResponseHeaders(200, NO_LEADER.length());
                    OutputStream os = request.getResponseBody();
                    os.write(NO_LEADER.getBytes());
                    os.close();
                    request.close();
                    handlerLoggerLocal.get().log(Level.INFO, 200 + " {0}", NO_LEADER);
                }else{
                    //send back leader list things
                    long leaderID = currentVote.getProposedLeaderID();
                    StringBuilder buildResponse = new StringBuilder();
                    buildResponse.append(leaderID + " is in ServerState: LEADING\n");
                    for( Long serverID : peerIDtoAddress.keySet() ){
                        if( (!gwayServer.isPeerDead(leaderID)) && (serverID != leaderID) ){
                            buildResponse.append(serverID + " is in ServerState: FOLLOWING\n");
                        }
                    }
                    buildResponse.append(gatewayID + " is in ServerState: OBSERVER\n");
                    request.sendResponseHeaders(200, buildResponse.length());
                    OutputStream os = request.getResponseBody();
                    os.write(buildResponse.toString().getBytes());
                    os.close();
                    request.close();
                    handlerLoggerLocal.get().log(Level.INFO, 200 + " {0}", buildResponse);
                }
            }catch( IOException e ){
                e.printStackTrace();
            }
        }
    }

    public GatewayServer(int httpPort, int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers){
        this.httpPort = httpPort;
        this.tcpPort = myPort+2;
        this.gatewayID = gatewayID;
        this.requestID = new AtomicLong();
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress);
        this.addrToPeer = new ConcurrentHashMap<>();
        for( Map.Entry<Long, InetSocketAddress> peerEntry : this.peerIDtoAddress.entrySet()){
            this.addrToPeer.put(peerEntry.getValue(), peerEntry.getKey());
        }
        //create GatewayPeerServerImpl
        this.gwayServer = new GatewayPeerServerImpl(myPort, peerEpoch, id, peerIDtoAddress, gatewayID, numberOfObservers);
        try {
            //create httpserver
            this.httpServer = HttpServer.create(new InetSocketAddress(this.httpPort), 0);
        } catch (IOException e) {
            handlerLoggerLocal.get().log(Level.SEVERE, "IOException occured when trying to create httpServer\n{0}", Util.getStackTrace(e));
        }
        handlerLoggerLocal.get().log(Level.FINE, "GatewayServer Construction Finished!");
    }

    @Override
    public void run(){
        this.httpServer.createContext("/compileandrun", new MyHandler());
        this.httpServer.createContext("/gettheleader", new MyHandler());
        this.httpServer.setExecutor(Executors.newFixedThreadPool(N_THREADS));
        this.gwayServer.start();
        handlerLoggerLocal.get().log(Level.FINE, "GatewayServerImpl Started!");
        this.httpServer.start();
        handlerLoggerLocal.get().log(Level.FINE, "HttpServer Started!");
    }

    public void shutdown(){
        handlerLoggerLocal.get().log(Level.WARNING, "GatewayServer Shutting Down!");
        handlerLoggerLocal.remove();
        this.httpServer.stop(0);
        this.gwayServer.shutdown();
    }

    public ZooKeeperPeerServerImpl getServer(){
        return this.gwayServer;
    }
}
