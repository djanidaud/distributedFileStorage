import java.io.*;
import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Dstore {
    private final int port;
    private final int cport;
    private final long timeout;
    private final String fileFolder;
    private Socket controllerSocket;
    private final DstoreLogger logger;

    public Dstore(int port, int cport, long timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;

        File file = new File(fileFolder);
        file.mkdir();
        for (File f : file.listFiles()) f.delete();

        try {
            DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
        } catch (IOException e) {
            System.err.println("Error, while trying to initialise the DStoreLogger: " + e);
        }
        this.logger = DstoreLogger.getInstance();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4)  {
            System.err.println("Invalid Arguments");
            return;
        }
            
        boolean isInputValid = isInteger(args[0]) && isInteger(args[1]) && isLong(args[2]);
        if (!isInputValid) {
            System.err.println("Invalid Arguments");
            return;
        }
    
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        long timeout = Long.parseLong(args[2]);
        String fileFolder = args[3];
        if (port < 0 || cport < 0 || timeout < 0) {
            System.err.println("Invalid Arguments");
            return;
        }
    
        Dstore store = new Dstore(port, cport, timeout, fileFolder);
        store.connectToController();
    }
 
    private void connectToController() {
        try { 
            controllerSocket = new Socket("localhost", cport);
            sendMessage(controllerSocket,  Protocol.JOIN_TOKEN + " " + port);
        
            new Thread(this::listenForController).start();
            listenForClients();
        } catch(Exception e) {
            System.err.println("Error, while trying to join the controller: " + e);
        }
    }

    private void listenForController() {
        while(true) {
            try { 
                BufferedReader br = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
                String line;
               
                while ((line = br.readLine()) != null) {
                    logger.messageReceived(controllerSocket, line);
                    String[] args = line.split(" ");
                    switch (args[0]) {
                        case Protocol.REMOVE_TOKEN: onRemoveQuery(args); break;
                        case Protocol.LIST_TOKEN: onListQuery(); break;
                        case Protocol.REBALANCE_TOKEN: onRebalance(args); break;
                        default: System.err.println("Unrecognised Action: " + line + " received from controller");
                    }
                }
            }
            catch(Exception e) {
                System.err.println("Error, while listening to the controller: " + e);
            }
        }
    }

    private void onRebalance(String[] args) {
        try {
            int filesToSend = Integer.parseInt(args[1]);
            int offset = 1;
            for (int i = 0; i < filesToSend; i++) {
                offset++;
                String file = args[offset];
                offset++;
                int count = Integer.parseInt(args[offset]);
                for (int j = 0; j < count; j++) {
                    offset++;
                    int port = Integer.parseInt(args[offset]);
                    sendFile(port, file);
                }
            }
            offset++;
            int filesToRemove = Integer.parseInt(args[offset]);
            for (int i = 0; i < filesToRemove; i++) {
                offset++;
                String file = args[offset];
                File f = new File(fileFolder + "/" + file);
                f.delete();
            }
            sendMessage(controllerSocket, Protocol.REBALANCE_COMPLETE_TOKEN);
        } catch (Exception e) {
            System.out.println("Malformed Rebalance Message");
        }
    }

    private void sendFile(int port, String file) {
        try {
            Socket dstore = new Socket("localhost", port);
            long fileSize = getFileSize(file);
            sendMessage(dstore, Protocol.REBALANCE_STORE_TOKEN + " " + file + " " + fileSize);
            
            BufferedReader br = new BufferedReader(new InputStreamReader(dstore.getInputStream()));
            String line;

            AtomicBoolean didTimeout = new AtomicBoolean(false);
            new Thread(() -> {
                try {
                    Thread.sleep(timeout);
                    didTimeout.set(true);
                } catch (InterruptedException ignored) {}
            }).start();

            if ((line = br.readLine()) != null && !didTimeout.get()) {
                if(line.equals(Protocol.ACK_TOKEN)) {
                    try {
                        File inputFile = new File(fileFolder + "/" + file);

                        FileInputStream inf = new FileInputStream(inputFile);
                        OutputStream out = dstore.getOutputStream();

                        byte[] buf = new byte[1024];
                        int buflen;

                        while ((buflen = inf.read(buf)) != -1) {
                            out.write(buf,0, buflen);
                        }
                    } catch(IOException e) { System.err.println("Failed to load file " + file);}
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private long getFileSize(String fileName) {
        final File file = new File(fileFolder + "/" + fileName);
        return file.isFile() ? file.length() : 0;
    }

    private void onRemoveQuery(String[] args) {
        if (args.length != 2) {
            System.err.println("Invalid REMOVE query");
            return;
        }
        String fileName = args[1];
        File file = new File(fileFolder + "/" + fileName);
    
        if(file.exists()) {
            if(file.delete()) sendMessage(controllerSocket, Protocol.REMOVE_ACK_TOKEN + " " + fileName);
        } else {
            sendMessage(controllerSocket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
        }
    }

    private void onListQuery() {
        final File folder = new File(fileFolder);
        File[] files = folder.listFiles();
        StringBuilder output = new StringBuilder(Protocol.LIST_TOKEN);
        for (File fileEntry : files) if (fileEntry.isFile()) output.append(" ").append(fileEntry.getName());
        sendMessage(controllerSocket, output.toString());
    }

    private void listenForClients() {
        try {
            ServerSocket ss = new ServerSocket(port);
            while(true) {
                Socket client = ss.accept();
                new Thread(() -> proccessClientQuery(client)).start();
            }
        } catch(Exception e) { System.err.println("Error, while listening for clients: " + e); }
    }

    private void proccessClientQuery(Socket client) {
        try {
            InputStream inputStream = client.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;

            while ((line = br.readLine()) != null) {
                logger.messageReceived(client, line);
                String[] args = line.split(" ");
                String action = args[0];

                switch (action) {
                    case Protocol.STORE_TOKEN: onStoreQuery(args, client); break;
                    case Protocol.LOAD_DATA_TOKEN: onLoadQuery(args, client); break;
                    case Protocol.REBALANCE_STORE_TOKEN: onRebalanceStore(args, client); break;
                    default: System.err.println("Unrecognised Action " + action);
                }
            }
        } catch (IOException ignored) {}
    }

    private void onRebalanceStore(String[] args, Socket client) {
        if (args.length != 3 || !isLong(args[2])) {
            System.err.println("Invalid STORE query");
            return;
        }

        try {
            sendMessage(client, Protocol.ACK_TOKEN);
            put(client, args[1], Long.parseLong(args[2]));
            client.close();
        } catch (Exception ignored) {}
    }

    private void onStoreQuery(String[] args, Socket client) {
        if (args.length != 3 || !isLong(args[2])) {
            System.err.println("Invalid STORE query");
            return;
        }
        String fileName = args[1];
        sendMessage(client, Protocol.ACK_TOKEN);

        boolean wasSucc = put(client, fileName, Long.parseLong(args[2]));
        if(wasSucc) sendMessage(controllerSocket, Protocol.STORE_ACK_TOKEN + " " + fileName);
    }

    private void onLoadQuery(String[] args, Socket client) {
        if (args.length != 2) {
            System.err.println("Invalid LOAD query");
            return;
        }
        String fileName = args[1];
        long fileSize = getFileSize(fileName);

        get(client, fileName, fileSize);
    }

    private void get(Socket client, String fileName, long fileSize) {
        try {
            File inputFile = new File(fileFolder + "/" + fileName);

            if (!inputFile.exists()) {
                try {
                    client.close();
                } catch (IOException ignored) { }
                return;
            }

            FileInputStream inf = new FileInputStream(inputFile);
            OutputStream out = client.getOutputStream();

            byte[] buf = new byte[1024];
            int buflen;

            while ((buflen = inf.read(buf)) != -1) {
                out.write(buf,0,buflen);
            }
        } catch(IOException e) { System.err.println("Failed to load file " + fileName);}
    }

    private boolean put(Socket client, String fileName, long fileSize) {
        try {
            InputStream inputStream = client.getInputStream();
            byte[] buf = new byte[1000];
            int buflen;

            File outputFile = new File(fileFolder + "/" + fileName);
            FileOutputStream out = new FileOutputStream(outputFile);

            while ((buflen = inputStream.read(buf)) != -1) {
                out.write(buf, 0, buflen);
            }
            return true;
        } catch(IOException e) {
            System.err.println("Failed to store file " + fileName);
            return false;
        }
    }

    private void sendMessage(Socket socket, String message) {
        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream());
            out.println(message);
            out.flush();
            logger.messageSent(socket, message);
        } catch (Exception e) {
            System.err.println("Failed to send message " + message + " to socket on port " + socket.getPort());
        }
    }

    private static boolean isLong(String s){
        try {
            Long.parseLong(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isInteger(String s){
        try {
            Integer.parseInt(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
