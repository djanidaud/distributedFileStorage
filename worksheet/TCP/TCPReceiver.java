package TCP;

import java.io.*;
import java.net.*;
class TCPReceiver{
  public static void main(String [] args){

    try {
        ServerSocket ss = new ServerSocket(4322);
        for(;;){
            try { 
                Socket client = ss.accept();

                // Single Threaded Client Handling:
                // BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                // String line;

                // while((line = in.readLine()) != null) System.out.println(line+ " received");
                // client.close();

                // Multi-Threaded Client Handling:
                new Thread(new Runnable(){
                    public void run(){
                      try {
                          BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                          String line;

                          while ((line = in.readLine()) != null) {
                            System.out.println(line + " received" + " from port " + client.getPort());


                            PrintWriter ack = new PrintWriter(client.getOutputStream());
                            ack.println("ACK"); 
                            ack.flush();


                          }
                          client.close(); 
                      } catch(Exception e){}
                  }
                }).start();
            } catch(Exception e){
              System.out.println("error " + e);
            }
        }
    } catch(Exception e) {
      System.out.println("error "+e);
    }
  }
}