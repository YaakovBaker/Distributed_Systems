package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPServer extends Thread implements LoggingServer{
    private LinkedBlockingQueue<ClientRequest> tcpWorkQueue;
    private ServerSocket tcpServerSocket;
    private int tcpPort;
    private Logger tcpServerLogger;

    public TCPServer(LinkedBlockingQueue<ClientRequest> tcpWorkQueue, int tcpPort, Long serverID) throws IOException{// myPort+2 - add 2 for TCp ports UDP + 2
        this.tcpWorkQueue = tcpWorkQueue;
        this.tcpPort = tcpPort;
        this.tcpServerLogger = initializeLogging(TCPServer.class.getCanonicalName() + "-on-serverID-" + serverID + "-on-tcpPort-" + this.tcpPort + "-Log.txt");
        setName("TCPServer-tcpPort-" + this.tcpPort);
        setDaemon(true);
        try {
            this.tcpServerSocket = new ServerSocket(this.tcpPort);
            this.tcpServerSocket.setReuseAddress(true);
        } catch (IOException e) {
            this.tcpServerLogger.log(Level.SEVERE, "failed to create TCP Socket", e);
        }
        this.tcpServerLogger.log(Level.FINE, "TCPServer Construction Finished!");
    }

    public void shutdown(){
        interrupt();
        try {
            this.tcpServerSocket.close();
        } catch (IOException e) {
            this.tcpServerLogger.log(Level.WARNING, "TCPServerSocket faield to Shutdown:\n{0}", e);
        }
        this.tcpServerLogger.log(Level.WARNING, "TCPServer Shutting Down");
    }

    @Override
    public void run() {
        while( !this.isInterrupted() ){
            Socket clientSocket = null;
            try {
                clientSocket = this.tcpServerSocket.accept();
            } catch (IOException e) {
                this.tcpServerLogger.log(Level.SEVERE, "IOException in TCPServer while trying to accept client connection");
            }
            if( clientSocket != null ){
                tcpServerJob(clientSocket);
            }else{
                this.tcpServerLogger.log(Level.WARNING, "The client connection was null in TCPServer");
            }
        }
        this.tcpServerLogger.log(Level.SEVERE, "TCPServer Exiting!");
    }
    
    private Message getMessageVIATCP(Socket clientSocket){
        try {
            InputStream clientSocketInputStream = clientSocket.getInputStream();
            byte[] bytesFromIS = Util.readAllBytesFromNetwork(clientSocketInputStream);
            return new Message(bytesFromIS);
        } catch (IOException e) {
            this.tcpServerLogger.log(Level.SEVERE, "IOException in TCPServer when reading bytes from socket", e);
        }
        return null;
    }

    private void offerWork(Message work, Socket clientSocket){
        boolean worked = this.tcpWorkQueue.offer(new ClientRequest(clientSocket, work));
        if( !worked ){
            this.tcpServerLogger.log(Level.SEVERE, "Offering the message to the work queue did not work");
        }
    }

    private void tcpServerJob(Socket clientSocket){
        Message incomingMail = getMessageVIATCP(clientSocket);
        if( incomingMail != null ){
            MessageType msgType = incomingMail.getMessageType();
            if( Util.isWork(msgType) || Util.isNewWork(msgType) ){//work or new work
                offerWork(incomingMail, clientSocket);
            }else{
                this.tcpServerLogger.log(Level.SEVERE, "Incorrect Message appeared in TCPServer client connection");
            }
        }
    }
}