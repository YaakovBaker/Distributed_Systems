package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;
//Inspired by Judah's earlier demo tests from previous stages
public class Stage5Test {

    private int[] smallP = {8010, 8020, 8030, 8040, 8050};//int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
    private int[] manyP = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
    @Test
    public void regularRunTest(){
        System.out.println("REGULAR RUN TEST\n");
        //step 1: create client and other test stuff
        TestMethods tm = new TestMethods();
            
        //step 2: create servers
        try {
            tm.createServers(smallP);
        } catch (IOException e1) {
            System.out.println("IOException while creating servers");
            e1.printStackTrace();
        }
        //step2.1: wait for servers to get started
        try {
            Thread.sleep(10000);
        }
        catch (InterruptedException e) {
            System.out.println("Interrupted Exception");
        }
        //print leaders
        tm.printLeaders();

        //step 3: send requests to the GatewayServer over HTTP
        tm.sendMessagesParallel(5);
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        //step 4: validate responses from leader
        System.out.println("about to print responses:");
        try {
            tm.printResponses(smallP);
        } catch (Exception e) {
            System.out.println("Exception in printing respones :\n");
            e.printStackTrace();
        }
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //step 5: stop servers
        System.out.println("Shutting Down");
        tm.stopServers();
        System.out.println("Shut down");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void killLeaderTest(){
        System.out.println("Kill Leader TEST\n");
        //step 1: create client and other test stuff
        TestMethods tm = new TestMethods();

        //step 2: create servers
        try {
            tm.createServers(manyP);
        } catch (IOException e1) {
            System.out.println("IOException while creating servers");
            e1.printStackTrace();
        }
        
        //step2.1: wait for servers to get started
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //print leaders
        tm.printLeaders();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //kill leader
        ArrayList<ZooKeeperPeerServerImpl> servers = tm.getServers();
        ZooKeeperPeerServerImpl zero = servers.get(0);
        System.out.println("zero id: " + zero.getServerId());
        int lead = (int) zero.getCurrentLeader().getProposedLeaderID();
        System.out.println("lead to kill: " + lead);
        ZooKeeperPeerServerImpl leader = servers.get(lead);
        leader.shutdown();
        tm.removeServer(leader);
        
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("After Re-election?");
        tm.printLeaders();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Shutting Down");
        tm.stopServers();
        System.out.println("Shut down");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void reassignWorkTest(){
        System.out.println("REASSIGN WORK TEST\n");
        //step 1: create client and other test stuff
        TestMethods tm = new TestMethods();

        //step 2: create servers
        try {
            tm.createServers(manyP);
        } catch (IOException e1) {
            System.out.println("IOException while creating servers");
            e1.printStackTrace();
        }
        
        //step2.1: wait for servers to get started
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //print leaders
        tm.printLeaders();
        int messagesReturned = 0;
        int numMessages = 5;
        tm.sendMessagesParallel(numMessages);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //kill leader
        ArrayList<ZooKeeperPeerServerImpl> servers = tm.getServers();
        ZooKeeperPeerServerImpl zero = servers.get(0);
        System.out.println("zero id: " + zero.getServerId());
        int lead = (int) zero.getCurrentLeader().getProposedLeaderID();
        System.out.println("lead to kill: " + lead);
        ZooKeeperPeerServerImpl leader = servers.get(lead);
        leader.shutdown();
        tm.removeServer(leader);
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("After Re-election?");
        tm.printLeaders();
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            messagesReturned = tm.printResponses(numMessages);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Expected: " + numMessages + "\nAnd got: " + messagesReturned);
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Shutting Down");
        tm.stopServers();
        System.out.println("Shut down");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}