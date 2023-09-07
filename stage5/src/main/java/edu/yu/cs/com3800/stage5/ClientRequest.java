package edu.yu.cs.com3800.stage5;

import java.net.Socket;

import edu.yu.cs.com3800.Message;

public class ClientRequest {
    private Socket clientConnection;
    private Message message;

    public ClientRequest(Socket clientConnection, Message message) {
        this.clientConnection = clientConnection;
        this.message = message;
    }

    public Message getMessage() {
        return this.message;
    }

    public Socket getClientConnection() {
        return this.clientConnection;
    }
}
