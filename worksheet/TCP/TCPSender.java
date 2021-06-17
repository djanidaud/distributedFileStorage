package TCP;

import java.io.*;
import java.net.*;

/*
    The new concept here is the class Socket which can be used to set up stream communication between 
    two applications. Here the client is trying to establish communication with the server 192.168.1.205 
    on port 4322. If a con- nection is achieved, then the method socket.getOutputStream is used to get a 
    stream which can be used to communicate with a server listening on the same port. 
    Messages can now be sent to that stream. We use a PrintWriter to be able to send text messages 
    a line at a time. Sockets have two-way communication, so we could have also got an input stream 
    from this socket, but it wasn’t required for this simple client.
*/
class TCPSender {
  public static void main(String [] args){
        try { 
            Socket socket = new Socket("192.168.1.205", 4322);
            PrintWriter out = new PrintWriter(socket.getOutputStream());

            for(int i = 0; i < 10; i++) {
                out.println("TCP message " + i); 
                out.flush();
                System.out.println("TCP message " + i + " sent");
                Thread.sleep(1000);
                
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String line;
                
                if ((line = in.readLine()) != null) System.out.println(line + " received");
            }
        } catch(Exception e) {
            System.out.println("error" + e);
        }
    } 
}
