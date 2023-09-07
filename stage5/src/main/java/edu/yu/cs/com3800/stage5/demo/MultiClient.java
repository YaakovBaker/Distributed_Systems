package edu.yu.cs.com3800.stage5.demo;
//This was a partnership between the Max Friedman and Yaakov Baker foundations
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Callable;

public class MultiClient implements Callable<Response> {

  private int num;
  private HttpClient clienteler;
  private int clientID;
  private URI uri;
  private String validClass;

  public MultiClient(int num, URI uri, String validClass) {
    this.clientID = num;
    this.num = num;
    this.uri = uri;
    this.validClass = validClass;
    clienteler = HttpClient.newHttpClient();
  }

  @Override
  public Response call(){
    System.out.println(this.clientID + " sent message: " + this.num);
    String code = validClass.replace("world!", "world! from code version " + this.num);
    HttpRequest request = (HttpRequest) HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(code))
        .uri(uri)
        .header("Content-Type", "text/x-java-source")
        .build();
    HttpResponse<String> response = null;
    try {
      response = clienteler.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      System.out.println("Exception in sending " + this.num + "\n" + e);
    }
    String bodyR = response.body();
    return new Response(response.statusCode(), bodyR, this.num);
  }
}
