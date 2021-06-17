package UDP;

import java.io.*;
import java.net.*;

/*
    Again we need a DatagramSocket and a DatagramPacket, this time so we can call socket.receive(packet). 
    Note that the application listens on port 4321 and must be running on machine koestler.ecs.soton.ac.uk 
    in order to collect the packets sent by our earlier application. As each packet is received its details 
    are printed so we can see what we have.

*/
class UDPReceiver {
    public static void main(String[] args) {
        try {
        DatagramSocket socket = new DatagramSocket(4321);
        listen(socket);
        } catch (Exception e) {
            System.out.println("error " + e);
        }
    }


    private static void listen(DatagramSocket socket) {
        try {
        byte[] buf = new byte[256];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        socket.receive(packet);
        socket.send(new DatagramPacket(buf, buf.length,packet.getAddress(), packet.getPort()));
        System.out.println("receive DatagramPacket " + (new String(packet.getData())).trim() + " " + packet.getAddress() + ":" + packet.getPort());
        //Thread.sleep(2000);
        listen(socket); 
        } catch (Exception e) {
            System.out.println("error " + e);
        }
    }
}