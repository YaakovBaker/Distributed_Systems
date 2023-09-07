package edu.yu.cs.com3800.stage5.demo;

import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
//This was a partnership between the Max Friedman and Yaakov Baker foundations
public class ServerCreation {
    private static final int GATEWAY_UDP = 8090;
    private static final int GATEWAY_HTTP = 9000;
    private static final Long GATEWAY_ID = 0L;
    private static final String HOST = "localhost";
    private static final int[] clusterPorts = {8010, 8020, 8030, 8040, 8050, 8060, 8070};
    private static final int NUM_OBSERVERS = 1;
    private static final int CLUSTER_NUM = clusterPorts.length + NUM_OBSERVERS;
    

   public static void main(String[] args) {
        Long serverID = Long.parseLong(args[0]);
        
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
        for (int i = 0; i < CLUSTER_NUM; i++) {
            if( i != 0 ){
                System.out.println(serverID +" is creating server: " + i);
                peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress(HOST, clusterPorts[i-1]));
            }
        }
        peerIDtoAddress.put(0L, new InetSocketAddress(HOST, GATEWAY_UDP));
        if( GATEWAY_ID.equals(serverID) ){
            peerIDtoAddress.remove(serverID);
            GatewayServer gServer  = new GatewayServer(GATEWAY_HTTP, GATEWAY_UDP, 0L, GATEWAY_ID, peerIDtoAddress, GATEWAY_ID, NUM_OBSERVERS);
            gServer.start();
        }else{
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>)peerIDtoAddress.clone();
            InetSocketAddress myAddress = map.remove(serverID);
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(myAddress.getPort(), 0L, serverID, map, GATEWAY_ID, NUM_OBSERVERS);
            server.start();
        }
   }
}
