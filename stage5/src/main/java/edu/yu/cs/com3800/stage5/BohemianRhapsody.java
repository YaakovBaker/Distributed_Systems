package edu.yu.cs.com3800.stage5;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class BohemianRhapsody implements Serializable{
    private long timeRCVD;
    private long heartBeat;
    private boolean failed;
    private InetSocketAddress addr;
    private Long serverID;

    public BohemianRhapsody(Long serverID, InetSocketAddress addr){
        this.timeRCVD = System.currentTimeMillis();
        this.heartBeat = 0L;
        this.failed = false;
        this.addr = addr;
        this.serverID = serverID;
    }

    public void setTime(){
        this.timeRCVD = System.currentTimeMillis();
    }

    public void replaceTimeReceived(long newTime){
        this.timeRCVD = newTime;
    }

    public void incrementBeat(){
        this.heartBeat++;
    }

    public void setFailed(){
        this.failed = true;
    }

    public long getTimeRCVD(){
        return this.timeRCVD;
    }

    public long getBeat(){
        return this.heartBeat;
    }

    public boolean isFailed(){
        return this.failed;
    }

    public void replaceBeat(long newBeat){
        this.heartBeat = newBeat;
    }

    public InetSocketAddress getAddr(){
        return this.addr;
    }

    public Long getServerID(){
        return this.serverID;
    }

    @Override
    public String toString() {
        return "BohemianRhapsody(" + "timeRCVD:" + timeRCVD + ", heartBeat:" + heartBeat + ", failed:" + failed + ", addr:" + addr + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if( obj == null ){
            return false;
        }
        BohemianRhapsody other = null;
        if( !(obj instanceof BohemianRhapsody) ){
            return false;
        }
        other = (BohemianRhapsody)obj;
        return( this.getTimeRCVD() == other.getTimeRCVD() &&
                this.getBeat() == other.getBeat() &&
                this.isFailed() == other.isFailed() &&
                this.getAddr().equals(other.getAddr())
            );
    }

    @Override
    public int hashCode() {
        return Math.abs((int)((this.heartBeat + this.timeRCVD) * 31));
    }
}