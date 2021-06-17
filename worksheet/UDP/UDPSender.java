package UDP;

import java.io.*;
import java.net.*;


/*
    UDP uses a combination of sockets and packets in the program to send short messages over the internet
    to machines addressed by a combination of their machine name (or IP address) and a port number. 
    
    This is a connectionless protocol. 
    
    Messages are sent off containing the address of their recipient and the underlying IP protocol arranges that 
    they are delivered to their destination. Messages can be lost.

    This application is a client which is constructing and sending messages as UDP packets. 
    Various classes from the java.net package have been used. Most significantly we have constructed a DatagramSocket 
    and a DatagramPacket and then called socket.send(packet) to put our message out on the network. The DatagramPacket 
    contains, as well as the message, its destination in the form of an InetAddress and a port number (here 4321).
    The application sends 10 numbered messages and prints a copy of what it has sent. The application sleeps for 2 seconds 
    between each message so that a human user can observe its behaviour.
*/
class UDPSender {
  public static void main(String[] args) {

    try {
      InetAddress address = InetAddress.getByName("192.168.1.205");
      DatagramSocket socket = new DatagramSocket();

      for (int i = 0; i < 10; i++) {
        byte[] buf = String.valueOf(i).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, address, 4321);
        socket.send(packet);

        System.out.println("send DatagramPacket " + new String(packet.getData()) + " " + packet.getAddress() + ":" + packet.getPort());
        Thread.sleep(2000);
        
        DatagramPacket ack = new DatagramPacket(buf, buf.length);
        socket.receive(ack);
        
        System.out.println("Ack " + new String(packet.getData()) + " " + packet.getAddress() + ":" + packet.getPort());


        
      }

    } catch (Exception e) {
      System.out.println("error");
    }

  }

}