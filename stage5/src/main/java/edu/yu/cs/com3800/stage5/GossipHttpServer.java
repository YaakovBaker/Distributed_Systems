package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;

public class GossipHttpServer extends Thread{

    private String sumPath;
    private String verbosePath;
    private HttpServer gHttpServer;
    private ZooKeeperPeerServerImpl me;
    private InetSocketAddress httpAddr;
    public GossipHttpServer(String sumPath, String verbosePath, ZooKeeperPeerServerImpl me){
        this.sumPath = "stage5Logs/" + sumPath;
        this.verbosePath = "stage5Logs/" + verbosePath;
        this.me = me;
        this.httpAddr = new InetSocketAddress(me.getAddress().getHostName(), me.getAddress().getPort() + 3);
        try {
            this.gHttpServer = HttpServer.create(this.httpAddr, 0);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
