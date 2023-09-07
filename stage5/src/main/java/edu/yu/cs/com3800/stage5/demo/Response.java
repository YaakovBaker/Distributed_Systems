package edu.yu.cs.com3800.stage5.demo;
//This was a partnership between the Max Friedman and Yaakov Baker foundations
public class Response {
    private int code;
    private String body;
    private int msgNum;

    public Response(int code, String body) {
        this.code = code;
        this.body = body;
        this.msgNum = 0;
    }

    public Response(int code, String body, int msgNum) {
        this.code = code;
        this.body = body;
        this.msgNum = msgNum;
    }

    public int getCode() {
        return this.code;
    }

    public String getBody() {
        return this.body;
    }

    public int getMSGNum(){
        return this.msgNum;
    }

    @Override
    public String toString() {
        return "Response " + this.msgNum + " has status code: " + this.code + 
        "\n With the response body: \n" + this.body + "\n";
    }
}