package edu.yu.cs.com3800.stage5.demo;
//This was a partnership between the Max Friedman and Yaakov Baker foundations
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;



public class Stage5DemoScript {
    private static String validClass = "package edu.yu.cs.fall2019.com3800.stage5;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int GATEWAY_UDP = 8090;
    private static final int GATEWAY_HTTP = 9000;
    private static final Long GATEWAY_ID = 0L;
    private static final String HOST = "localhost";
    private static final int[] clusterPorts = {8010, 8020, 8030, 8040, 8050, 8060, 8070};
    private static final int NUM_OBSERVERS = 1;
    private static final int CLUSTER_NUM = clusterPorts.length + NUM_OBSERVERS;
    
    public static void main(String[] args) {
        URI uri = URI.create("http://" + HOST + ":" + GATEWAY_HTTP + "/gettheleader");
        URI uri2 = URI.create("http://" + HOST + ":" + GATEWAY_HTTP + "/compileandrun");
        ExecutorService arceus = Executors.newFixedThreadPool(N_THREADS, mew -> {
            Thread girantina = new Thread(mew);
            girantina.setDaemon(true);
            return girantina;});
        
        //2. Create a cluster of 7 nodes and one gateway, 
        //create IDs and addresses
        System.out.println("Creating The Servers");
        //hardcoded id maps
        //starting each in their own JVM
        System.out.println("Start Each Server in own JVM");
        Map<Long, Long> pokeball = new ProcessCreation(CLUSTER_NUM).startServersInOwnJVM();
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        /*
        * 3. Wait until the election has completed before sending any requests to the Gateway. 
        * In order to do this, you must add another http based service to the Gateway 
        * which can be called to ask if it has a leader or not. 
        * If the Gateway has a leader, it should respond with the full list of nodes 
        * and their roles (follower vs leader). 
        * Script should print out the list of server IDs and their roles.
        */
        System.out.println("Step 3");
        HttpClient httpClientel = HttpClient.newHttpClient();
        
        HttpRequest request = (HttpRequest) HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .header("Content-Type", "get-leader")
                .build();
        HttpResponse<String> response = null;
        while( (response == null) || (response.body().equals("No Leader")) ){
            try {
                response = httpClientel.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        String responseBody = response.body();
        Long leaderID = Long.parseLong(responseBody.substring(0,1));
        System.out.println(response.body());
        
        
        /*
        * 4. Once the gateway has a leader, send 9 client requests.
        * Script should print out both the request and the response from the cluster.
        * In other words, you wait to get all the responses and print them out.
        * You can either write a client in java or use cURL.
        */
        System.out.println("Sending Messages in step 4");
        List<Future<Response>> celebi = new ArrayList<>();
        for(int i = 0; i < 9; i++){
            Future<Response> dialga = arceus.submit(new MultiClient(i, uri2, validClass));
            celebi.add(dialga);
        }
        for( Future<Response> palkia : celebi ){
            try {
                Response gardevoir = palkia.get();
                System.out.println(gardevoir);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        /*
        * 5. kill -9 a follower JVM, printing out which one you are killing.
        * Wait heartbeat interval * 10 time, and then retrieve and
        * display the list of nodes from the Gateway.
        * The dead node should not be on the list
        */
        System.out.println("Killing a follower in step 5");
        try {
            Runtime.getRuntime().exec("kill -9 " + pokeball.get(3L));//kill follwer server 3
            pokeball.remove(3L);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        HttpRequest request2 = (HttpRequest) HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .header("Content-Type", "get-leader")
                .build();
        HttpResponse<String> response2 = null;
        while( (response2 == null) || (response2.body() == "No Leader") ){
            try {
                response2 = httpClientel.send(request2, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(response2.body());

        /*
        * 6. kill -9 the leader JVM and then pause 1000 milliseconds.
        * Send/display 9 more client requests to the gateway, in the background
        */
        System.out.println("Killing a leader in step 6");
        try {
            Runtime.getRuntime().exec("kill -9 " + pokeball.get(leaderID));//kill leader 6
            pokeball.remove(leaderID);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<Future<Response>> jirachi = new ArrayList<>();
        for(int i = 9; i < 18; i++){
            Future<Response> dialga = arceus.submit(new MultiClient(i, uri2, validClass));
            jirachi.add(dialga);
        }
        

        /*
        * 7. Wait for the Gateway to have a new leader, and then print out the node ID of the leader. 
        * Print out the responses the client receives from the Gateway for the 9 requests sent in step 6. 
        * Do not proceed to step 8 until all 9 requests have received responses.
        */
        System.out.println("Step 7");
        HttpRequest request3 = (HttpRequest) HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .header("Content-Type", "get-leader")
                .build();
        HttpResponse<String> response3 = null;
        while( (response3 == null) || (response3.body() == "No Leader") ){
            try {
                response3 = httpClientel.send(request3, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        leaderID = Long.parseLong(response3.body().substring(0,1));
        System.out.println("The New Leader: " + leaderID);

        for( Future<Response> palkia : jirachi ){
            try {
                Response gardevoir = palkia.get();
                System.out.println(gardevoir);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        // # 8. Send/display 1 more client request (in the foreground), print the response
        System.out.println("Step 8");
        Future<Response> dialga = arceus.submit(new MultiClient(18, uri2, validClass));
        try {
            Response mewtwo = dialga.get();
            System.out.println(mewtwo);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        
        // # 9. List the paths to files containing the Gossip messages received by each node.
        System.out.println("Step 9");
        // # 10. Shut down all the node
        System.out.println("Step 10");
        for( Long serverID : pokeball.keySet() ){
            try {
                Runtime.getRuntime().exec("kill -9 " + pokeball.get(serverID));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
