package edu.yu.cs.com3800.stage5;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class TestMethods {

    private static String validClass = "package edu.yu.cs.fall2019.com3800.stage5;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    private ArrayList<ZooKeeperPeerServerImpl> servers;
    private static final int GATEWAY_UDP = 8090;
    private static final int GATEWAY_HTTP = 9000;
    private static final String GATEWAY_HOST = "localhost";
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private GatewayServer gServer;
    private URI uri;
    private HttpClient httpClientel;
    private LinkedBlockingQueue<Future<Response>> responses;

    private ExecutorService clients;

    public TestMethods(){
        this.servers = new ArrayList<>();
        this.httpClientel = HttpClient.newHttpClient();
        this.uri = URI.create("http://" + GATEWAY_HOST + ":" + GATEWAY_HTTP + "/compileandrun");
        this.gServer = null;
        this.responses = new LinkedBlockingQueue<>();
        this.clients = Executors.newFixedThreadPool(N_THREADS, r -> {
            Thread complete = new Thread(r);
            complete.setDaemon(true);
            return complete;});
    }

    public URI getURI(){
        return this.uri;
    }

    public HttpClient getClient(){
        return this.httpClientel;
    }

    public LinkedBlockingQueue<Future<Response>> getResponses(){
        return this.responses;
    }

    public ArrayList<ZooKeeperPeerServerImpl> getServers(){
        return this.servers;
    }
    /**
     * create the servers
     * @param ports
     * @throws IOException
     */
    public void createServers(int[] ports) throws IOException {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
        for (int i = 0; i < ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue() + 1, new InetSocketAddress("localhost", ports[i]));
        }
        peerIDtoAddress.put(0L, new InetSocketAddress(GATEWAY_HOST, GATEWAY_UDP));
        this.gServer = new GatewayServer(GATEWAY_HTTP, GATEWAY_UDP, 0L, 0L, peerIDtoAddress, 0L, 1);
        //create servers
        this.servers = new ArrayList<>(3);
        this.servers.add(gServer.getServer());
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if( !entry.getKey().equals(0L) ){
                HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>)peerIDtoAddress.clone();
                map.remove(entry.getKey());
                ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 0L, 1);
                this.servers.add(server);
            }
        }
        //start servers
        gServer.start();
        for( ZooKeeperPeerServerImpl zpsi : this.servers ){
            if( zpsi.getServerId() != 0 ){
                zpsi.start();
            }
        }
        ZooKeeperPeerServerImpl g = gServer.getServer();
        while( g.getCurrentLeader() == null ){
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Print out the leaders
     */
    public void printLeaders() {
        for (ZooKeeperPeerServer server : this.servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }else{
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: null and its state is " + server.getPeerState().name());
            }
        }
    }

    /**
     * Print Responses from Gateway
     * @param ports
     * @throws Exception
     */
    public void printResponses(int[] ports) throws Exception {
        String completeResponse = "";
        //do I construct the message again in HttpClient here or Gateway just sends the contents?
        for (int i = 0; i < ports.length; i++) {
            String response = this.responses.take().get().getBody();
            System.out.println("I took msg: " + i);
            completeResponse += "Response to request " + i + ":\n" + response + "\n\n";
        }
        System.out.println(completeResponse);
    }

    /**
     * Print Responses from Gateway
     * @param more
     * @param ports
     * @throws Exception
     */
    public void printResponses(int more, int[] ports) throws Exception {
        String completeResponse = "";
        for (int i = 0; i < ports.length + more; i++) {
            String response = this.responses.take().get().getBody();
            System.out.println("I took msg: " + i);
            completeResponse += "Response to request " + i + ":\n" + response + "\n\n";
        }
        System.out.println(completeResponse);
    }

    public int printResponses(int more) throws Exception {
        int messageCount = 0;
        for (int i = 0; i <  more; i++) {
            String response = this.responses.take().get().getBody();
            if (response != null) {
                messageCount++;
            }else{
                System.out.println("I took msg: " + i);
                System.out.println("Response to request " + i + ":\n" + response + "\n\n");
            }
            
        }
        return messageCount;
    }

    /**
     * Send the HttpRequests
     * @param code
     * @throws InterruptedException
     */
    public Response sendMessage(String code) throws InterruptedException{
        HttpRequest request = (HttpRequest) HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(code))
                .uri(uri)
                .setHeader("User-Agent", "Java 17 HttpClient")
                .header("Content-Type", "text/x-java-source")
                .build();
        HttpResponse<String> response = null;
        try {
            response = httpClientel.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Response(response.statusCode(), (String)response.body());
    }

    public void sendMessagesParallel(){
        for (int i = 0; i < 15; i++) {
            responses.add(clients.submit(new MultiClient(i, uri, validClass)));
        }
    }

    public void sendMessagesParallel(int num){
        for (int i = 0; i < num; i++) {
            responses.add(clients.submit(new MultiClient(i, uri, validClass)));
        }
    }

    /**
     * Stop The Servers
     */
    public void stopServers() {
        for (ZooKeeperPeerServer server : this.servers) {
            if( Util.isObserving(server.getPeerState()) ){
                continue;
            }
            server.shutdown();
        }
        gServer.shutdown();
    }

    public void removeServer(ZooKeeperPeerServerImpl leader) {
        this.servers.remove(leader);
    }
}
