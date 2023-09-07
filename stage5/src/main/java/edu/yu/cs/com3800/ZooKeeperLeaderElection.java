package edu.yu.cs.com3800;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class ZooKeeperLeaderElection
{
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int FINALIZEWAIT = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int MAXNOTIFIFICATIONINTERVAL = 60000;

    private long proposedEpoch;
    private long proposedLeader;
    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServerImpl myPeerServer;
    private boolean accepted;
    private List<Message> ignoredMessages;

    public ZooKeeperLeaderElection(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages)
    {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proposedEpoch = this.myPeerServer.getPeerEpoch();
        this.proposedLeader = this.myPeerServer.getServerId();
        this.accepted = false;
        this.ignoredMessages = new ArrayList<>();
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader()
    {
        Map<Long, ElectionNotification> votes = new HashMap<>();
        int timeOut = FINALIZEWAIT;
        //send initial notifications to other peers to get things started
        sendNotifications();
        //Loop, exchanging notifications with other servers until we find a leader
        while( !this.accepted ) {//(this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING)
            //Remove next notification from queue, timing out after 2 times the termination time
            Message nextNotification = null;
            try {
                nextNotification = this.incomingMessages.poll((2 * timeOut), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
            ElectionNotification notif = null;
            if( nextNotification != null ){
                if( Util.isGossip(nextNotification.getMessageType())) {//ignore gossip
                    this.ignoredMessages.add(nextNotification);
                    continue;
                }
                notif = getNotificationFromMessage(nextNotification);
                
            }else{//if no notifications received..
                //..resend notifications to prompt a reply from others..
                sendNotifications();
                //.and implement exponential back-off when notifications not received..
                if( timeOut < MAXNOTIFIFICATIONINTERVAL ){
                    timeOut *= 2;
                    if( timeOut > MAXNOTIFIFICATIONINTERVAL ){
                        timeOut = MAXNOTIFIFICATIONINTERVAL;
                    }  
                }
            }
            
            //if/when we get a message and it's from a valid server and for a valid server..
            //if the notif is not null AND the sender isn't failed AND the Vote Isn;t for a failure
            if( isValid(notif) ){
                //switch on the state of the sender:
                switch( notif.getState() ){
                    case LOOKING: //if the sender is also looking
                        //if the received message has a vote for a leader which supersedes mine, 
                        if( supersedesCurrentVote( notif.getProposedLeaderID(), notif.getPeerEpoch()) ){
                            //change my vote and tell all my peers what my new vote is.
                            this.proposedLeader = notif.getProposedLeaderID();
                            this.proposedEpoch = notif.getPeerEpoch();
                            sendNotifications();
                        }
                        //keep track of the votes I received and who I received them from.
                        votes.put(notif.getSenderID(), notif);
                        //if I have enough votes to declare my currently proposed leader as the leader:
                        if( haveEnoughVotes(votes, getCurrentVote()) ){
                            //first check if there are any new votes for a higher ranked possible leader before I declare a leader.
                            //need to iterate through votes to find if true
                            boolean existsGreater = false;
                            while( (!incomingMessages.isEmpty()) ){
                                Message mes = this.incomingMessages.poll();
                                if( Util.isGossip(mes.getMessageType()) ){
                                    this.ignoredMessages.add(mes);
                                    continue;
                                }
                                ElectionNotification incoming = getNotificationFromMessage(mes);
                                if( this.myPeerServer.isPeerDead(incoming.getSenderID()) && 
                                    this.myPeerServer.isPeerDead(incoming.getProposedLeaderID())&&
                                    supersedesCurrentVote(incoming.getProposedLeaderID(), incoming.getPeerEpoch()) 
                                ){
                                    existsGreater = true;
                                    this.incomingMessages.add(mes);
                                }
                            }
                            if( !existsGreater ){
                                //If not,
                                //set my own state to either LEADING (if I won the election)
                                //or FOLLOWING (if someone lese won the election) and exit the election
                                acceptElectionWinner(notif);
                                finalSleep();
                            }
                        }
                        break;
                    case FOLLOWING: case LEADING: //if the sender is following a leader already or thinks it is the leader
                        //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in,
                        // i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                        // Vote newLead = new Vote(notif.getProposedLeaderID(), notif.getPeerEpoch());
                        // if( (notif.getPeerEpoch() == this.proposedEpoch) && haveEnoughVotes(votes, newLead) ){
                        //     //if so, accept the election winner.
                        //     acceptElectionWinner(notif);
                        //     finalSleep();
                        //     //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                        // }
                        // //ELSE: if n is from a LATER election epoch
                        // //IF a quorum from that epoch are voting for the same peer as 
                        // //the vote of the FOLLOWING or 
                        // //LEADING peer whose vote I just received.
                        // else if((notif.getPeerEpoch() > this.proposedEpoch) &&
                        //         (notif.getProposedLeaderID() == this.proposedLeader)
                        //     ){
                        //         //THEN accept their leader, and update my epoch to be their epoch
                        //         acceptElectionWinner(notif);
                        //         finalSleep();
                        // }
                        if( notif.getPeerEpoch() >= this.proposedEpoch ){
                            acceptElectionWinner(notif);
                            finalSleep();
                        }
                        break;
                    case OBSERVER:
                        //ignore message
                        break;
                    } 
                }
            }
        finalSleep();
        return getCurrentVote();
    }

    private boolean isValid(ElectionNotification notif){
        return notif != null && 
        !this.myPeerServer.isPeerDead(notif.getSenderID()) && 
        !this.myPeerServer.isPeerDead(notif.getProposedLeaderID());
    }

    private void finalSleep(){
        try {
            Thread.sleep(FINALIZEWAIT);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private void sendNotifications() {
        this.myPeerServer.sendBroadcast(MessageType.ELECTION, buildMsgContent(new ElectionNotification
        (this.proposedLeader,
        this.myPeerServer.getPeerState(), 
        this.myPeerServer.getServerId(), 
        this.proposedEpoch)));
    }

    //need to understand this more
    private Vote acceptElectionWinner(ElectionNotification n)
    {
        //is it by stuff from ElectionNotification or the class
        this.proposedLeader = n.getProposedLeaderID();
        this.proposedEpoch = n.getPeerEpoch();
        //played around where to put this and this gave no problems compared to after clear
        sendNotifications();
        //set my state to either LEADING or FOLLOWING
        if( this.myPeerServer.getServerId() != this.proposedLeader ){
            this.myPeerServer.setPeerState(ServerState.FOLLOWING);
        }else{
            this.myPeerServer.setPeerState(ServerState.LEADING);
        }
        //clear out the incoming queue before returning
        this.incomingMessages.clear();
        this.accepted = true;
        this.incomingMessages.addAll(this.ignoredMessages);
        return getCurrentVote();
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
     protected boolean supersedesCurrentVote(long newId, long newEpoch) {
         return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
     }
    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification > votes, Vote proposal)
    {
       //is the number of votes for the proposal > the size of my peer server’s quorum?
       int numVotes = 0;
       for(Long v : votes.keySet()){
        if( votes.get(v).getProposedLeaderID() == proposal.getProposedLeaderID() ){
            numVotes++;
        }
       }
       return (numVotes >= this.myPeerServer.getQuorumSize());
    }
    
    public static byte[] buildMsgContent(ElectionNotification notification) {
        return Util.buildMsgContent(notification);
    }
    
    public static ElectionNotification getNotificationFromMessage(Message received) {
        return Util.getNotificationFromMessage(received);
    }
}
