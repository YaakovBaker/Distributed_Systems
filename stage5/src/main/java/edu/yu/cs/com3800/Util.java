package edu.yu.cs.com3800;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

public class Util {

    public static byte[] readAllBytesFromNetwork(InputStream in)  {
        try {
            int tries = 0;
            while (in.available() == 0 && tries < 10) {
                try {
                    tries++;
                    Thread.currentThread().sleep(500);
                }
                catch (InterruptedException e) {
                }
            }
        }
        catch(IOException e){}
        return readAllBytes(in);
    }

    public static byte[] readAllBytes(InputStream in) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        try {
            while (in.available() > 0 && (numberRead = in.read(data, 0, data.length)) != -1   ) {
                buffer.write(data, 0, numberRead);
            }
        }catch(IOException e){}
        return buffer.toByteArray();
    }

    public static Thread startAsDaemon(Runnable run, String name) {
        Thread thread = new Thread(run, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    public static String getStackTrace(Exception e){
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        PrintStream myErr = new PrintStream(bas,true);
        e.printStackTrace(myErr);
        myErr.flush();
        myErr.close();
        return bas.toString();
    }

    /**
     * 
     * @param notification to turn into message contents
     * @return byte[] message contents
     */
    public static byte[] buildMsgContent(ElectionNotification notification) {
        /*
        size of buffer =
        1 long (proposedLeaderID) = 8 bytes
        1 long (senderID) = 8 bytes
        1 long (peerEpoch) = 8 byte
        1 char (State) (getChar) = 2 bytes
        */
        int bufferSize = 26;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        buffer.flip();
        return buffer.array();
    }

    /**
     * 
     * @param received message to turn into a election notification
     * @return an ElectionNotification
     */
    public static ElectionNotification getNotificationFromMessage(Message received) {
        if (received == null) {
            throw new IllegalArgumentException();
        }
        ByteBuffer msgBytes = ByteBuffer.wrap(received.getMessageContents());
        long propL = msgBytes.getLong();
        ServerState sevst = ServerState.getServerState(msgBytes.getChar());
        long sndrID = msgBytes.getLong();
        long prEpoch = msgBytes.getLong();
        return new ElectionNotification(propL, sevst, sndrID, prEpoch);
    }

    /**
     * @return true if this is a Gossip MessageType
     * otherwise
     * @return false
     */
    public static boolean isGossip(MessageType msgType) {
        return msgType.equals(MessageType.GOSSIP);
    }

    /**
     * @return true if this is an ELECTION MessageType
     * otherwise
     * @return false
     */
    public static boolean isElection(MessageType msgType) {
        return msgType.equals(MessageType.ELECTION);
    }

    /**
     * @return true if this is an WORK MessageType
     * otherwise
     * @return false
     */
    public static boolean isWork(MessageType msgType) {
        return msgType.equals(MessageType.WORK);
    }

    /**
     * @return true if this is an COMPLETED_WORK MessageType
     * otherwise
     * @return false
     */
    public static boolean isCompletedWork(MessageType msgType) {
        return msgType.equals(MessageType.COMPLETED_WORK);
    }

    /**
     * @return true if this is an NEW_LEADER_GETTING_LAST_WORK MessageType
     * otherwise
     * @return false
     */
    public static boolean isNewWork(MessageType msgType) {
        return msgType.equals(MessageType.NEW_LEADER_GETTING_LAST_WORK);
    }

    public static boolean isFinalWOrk(MessageType msgType){
        return msgType.equals(MessageType.FINAL_WORK_FROM_FOLLOWER);
    }

    /**
     * @param state
     * @return
     */
    public static boolean isFollowing(ServerState state){
        return state.equals(ServerState.FOLLOWING);
    }

    /**
     * 
     * @param state
     * @return
     */
    public static boolean isLeading(ServerState state){
        return state.equals(ServerState.LEADING);
    }

    /**
     * 
     * @param state
     * @return
     */
    public static boolean isLooking(ServerState state){
        return state.equals(ServerState.LOOKING);
    }

    /**
     * 
     * @param state
     * @return
     */
    public static boolean isObserving(ServerState state){
        return state.equals(ServerState.OBSERVER);
    }    
}
