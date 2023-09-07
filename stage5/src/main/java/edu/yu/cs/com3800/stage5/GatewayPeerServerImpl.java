package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.util.Map;
public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl{
    
    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) {
        super(myPort, peerEpoch, id, peerIDtoAddress, gatewayID, numberOfObservers);
        super.setPeerState(ServerState.OBSERVER);
    }

    @Override
    public void setPeerState(ServerState newState) {
        super.setPeerState(ServerState.OBSERVER);
    }
}
