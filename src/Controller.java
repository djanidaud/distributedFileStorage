import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Controller {
    static int lowerBound = 0;
    final Object rebalanceLock = new Object();
    private final int cport;
    private final int replicationFactor;
    private final long timeout;

    private final List<IndexEntry> index;
    private final List<Map.Entry<Integer, StoredFiles>> storedFiles;
    private final List<Integer> rebalanceCompleteACKs;

    private final Map<String, Integer> fileCounter;
    private final Map<Integer, Socket> dStoresList;
    private final Map<Integer, Socket> rebalancingDStores;

    private final AtomicInteger listResponseCounter;
    private final AtomicBoolean hasRebalanceTimeouted;
    private final AtomicBoolean rebalanceCompleteTimeouted;
    private final AtomicBoolean isRebalancing;
    private final long rebalancePeriod;
    private final ControllerLogger logger;
    private Thread rebalanceTimerThread;
    private Thread rebalanceListTimerThread;
    private Thread rebalanceThread;

    public Controller(int cport, int replicationFactor, long timeout, long rebalancePeriod) {
        this.cport = cport;
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;

        storedFiles = Collections.synchronizedList(new ArrayList<>());
        index = Collections.synchronizedList(new ArrayList<>());
        rebalanceCompleteACKs = Collections.synchronizedList(new ArrayList<>());

        fileCounter = new Hashtable<>();
        dStoresList = new Hashtable<>();
        rebalancingDStores = new Hashtable<>();

        listResponseCounter = new AtomicInteger(0);
        hasRebalanceTimeouted = new AtomicBoolean(false);
        rebalanceCompleteTimeouted = new AtomicBoolean(false);
        isRebalancing = new AtomicBoolean(false);

        try {
            ControllerLogger.init(Logger.LoggingType.ON_FILE_ONLY);
        } catch (IOException e) {
            System.err.println("Error, while trying to initialise the ControllerLogger Singleton: " + e);
        }

        this.logger = ControllerLogger.getInstance();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Invalid Arguments");
            return;
        }

        boolean isInputValid = isInteger(args[0]) && isInteger(args[1]) && isLong(args[2]) && isLong(args[3]);
        if (!isInputValid) {
            System.err.println("Invalid Arguments");
            return;
        }

        int cport = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        long timeout = Long.parseLong(args[2]);
        long rebalancePeriod = Long.parseLong(args[3]);
        if (cport < 0 || replicationFactor < 0 || timeout < 0 || rebalancePeriod < 0) {
            System.err.println("Invalid Arguments");
            return;
        }

        Controller c = new Controller(cport, replicationFactor, timeout, rebalancePeriod);
        c.startRebalanceThread();
        c.listen();
    }

    private static boolean isLong(String s) {
        try {
            Long.parseLong(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean notEnoughStores() {
        return replicationFactor > dStoresList.size();
    }

    private void listen() {
        try {
            ServerSocket ss = new ServerSocket(cport);
            while (true) {
                try {
                    System.out.println("Waiting for Connection");
                    Socket client = ss.accept();
                    System.out.println("Established a connection with client on port " + client.getPort());

                    new Thread(() -> proccessQuery(client)).start();
                } catch (Exception e) {
                    System.err.println("Error, while listening for clients: " + e);
                }
            }
        } catch (Exception e) {
            System.err.println("Error, while trying to create the controller's server socket: " + e);
        }
    }

    private void proccessQuery(Socket client) {
        boolean isDstore = false;
        try {
            InputStream inputStream = client.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;

            while ((line = br.readLine()) != null) {
                logger.messageReceived(client, line);

                String[] args = line.split(" ");
                String action = args[0];

                switch (action) {
                    case Protocol.JOIN_TOKEN:
                        isDstore = true;
                        onJoinQuery(args, client);
                        break;
                    case Protocol.LIST_TOKEN:
                        if (isDstore) onRebalanceListQuery(args, client);
                        else onListQuery(client);
                        break;
                    case Protocol.STORE_TOKEN:
                        onStoreQuery(args, client);
                        break;
                    case Protocol.LOAD_TOKEN:
                        onLoadQuery(args, client);
                        break;
                    case Protocol.REMOVE_TOKEN:
                        onRemoveQuery(args, client);
                        break;
                    case Protocol.STORE_ACK_TOKEN:
                        onStoreACK(args);
                        break;
                    case Protocol.REMOVE_ACK_TOKEN:
                        onRemoveACK(args, client);
                        break;
                    case Protocol.RELOAD_TOKEN:
                        onReload(args, client);
                        break;
                    case Protocol.REBALANCE_COMPLETE_TOKEN:
                        onRebalanceComplete(client);
                        break;
                    case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN:
                        onRemoveACK(args, client);
                        break;
                    default:
                        System.err.println("Unrecognised Action");
                }
            }

        } catch (Exception e) {
            System.err.println("Error, while trying to parse the client's query: " + e);
        }
        System.out.println("The client on port " + client.getPort() + " disconnected");
        if (isDstore) onDstoreDrop(client);
    }

    private void onDstoreDrop(Socket client) {
        int clientPort = client.getPort();
        Integer dstoreEntry = -1;
        for (Map.Entry<Integer,Socket> entry : dStoresList.entrySet())
            if (entry.getValue().getPort() == clientPort) {
                dstoreEntry = entry.getKey();
                break;
            }

        if (dstoreEntry != -1) {
            dStoresList.remove(dstoreEntry);
            for (IndexEntry e : index) e.ports.remove(dstoreEntry);
            System.out.println("Removed Dstore on port " + dstoreEntry);
        }
    }

    private void onJoinQuery(String[] args, Socket client) {
        if (args.length != 2) {
            System.err.println("Invalid JOIN query");
            return;
        }

        if (!isInteger(args[1])) {
            System.err.println("Invalid JOIN query");
            return;
        }

        int port = Integer.parseInt(args[1]);
        if (port <= 0) {
            System.err.println("Invalid JOIN query");
            return;
        }

        logger.dstoreJoined(client, port);
        dStoresList.put(port, client);
        new Thread(this::startRebalance).start();
    }

    private void onStoreQuery(String[] args, Socket client) {
        while (isRebalancing()) ;
        if (notEnoughStores()) {
            sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        if (args.length != 3 || !isLong(args[2])) {
            System.err.println("Invalid STORE query");
            return;
        }

        synchronized (this) {
            String fileName = args[1];
            if (getFileEntry(fileName) != null) {
                sendMessage(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                return;
            }

            long fileSize = Long.parseLong(args[2]);
            IndexEntry entry = new IndexEntry(fileName, fileSize, client);
            index.add(entry);

            Map<Integer, Integer> portWorkload = new HashMap<>();
            dStoresList.keySet().forEach(k -> portWorkload.put(k, 0));

            index.forEach(i -> i.ports.forEach(p -> portWorkload.put(p, portWorkload.get(p) + 1)));
            List<Map.Entry<Integer, Integer>> sortedEntries = new ArrayList<>(portWorkload.entrySet());
            sortedEntries.sort(Map.Entry.comparingByValue());

            StringBuilder message = new StringBuilder(Protocol.STORE_TO_TOKEN);
            for (int r = 0; r < replicationFactor; r++) {
                int p = sortedEntries.get(r).getKey();
                entry.ports.add(p);
                message.append(" ").append(p);
            }

            sendMessage(client, message.toString());
            new Thread(() -> {
                try {
                    Thread.sleep(timeout);
                    entry.didStoreTimeout.set(true);
                    if (replicationFactor != entry.storeCounter.get()) index.remove(entry);
                } catch (InterruptedException ignored) {
                }
            }).start();
        }
    }

    private void onStoreACK(String[] args) {
        if (args.length != 2) {
            System.err.println("Invalid STORE_ACK");
            return;
        }
        IndexEntry entry = getFileEntry(args[1]);
        if (entry == null) return;

        if (entry.storeCounter.incrementAndGet() == replicationFactor && !entry.didStoreTimeout.get()) {
            entry.state = FileState.STORE_COMPLETE;
            sendMessage(entry.client, Protocol.STORE_COMPLETE_TOKEN);
        }
    }

    private void onLoadQuery(String[] args, Socket client) {
        while (isRebalancing()) ;
        if (notEnoughStores()) {
            sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        if (args.length != 2) {
            System.err.println("Invalid LOAD query");
            return;
        }

        synchronized (this) {
            String fileName = args[1];
            IndexEntry entry = getFileEntry(fileName);

            if (entry == null || entry.ports.size() == 0 || entry.state != FileState.STORE_COMPLETE) {
                sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            entry.loads.put(client.getPort(), 0);
            sendMessage(client, Protocol.LOAD_FROM_TOKEN + " " + entry.ports.get(0) + " " + entry.fileSize);
        }
    }

    private void onReload(String[] args, Socket client) {
        while (isRebalancing()) ;
        if (notEnoughStores()) {
            sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        if (args.length != 2) {
            System.err.println("Invalid RELOAD query");
            return;
        }

        String fileName = args[1];
        IndexEntry entry = getFileEntry(fileName);

        if (entry == null || entry.ports.size() == 0 || entry.state != FileState.STORE_COMPLETE) {
            sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }

        int loadToDStoreIndex = entry.loads.getOrDefault(client.getPort(), -1);
        if (loadToDStoreIndex == -1) return;

        loadToDStoreIndex += 1;
        if (loadToDStoreIndex == entry.ports.size()) {
            sendMessage(client, Protocol.ERROR_LOAD_TOKEN);
            return;
        }

        entry.loads.put(client.getPort(), loadToDStoreIndex);
        sendMessage(client, Protocol.LOAD_FROM_TOKEN + " " + entry.ports.get(loadToDStoreIndex) + " " + entry.fileSize);
    }

    private void onRemoveQuery(String[] args, Socket client) {
        while (isRebalancing()) ;
        if (notEnoughStores()) {
            sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        if (args.length != 2) {
            System.err.println("Invalid REMOVE query");
            return;
        }

        synchronized (this) {
            String fileName = args[1];
            IndexEntry entry = getFileEntry(fileName);

            if (entry == null || entry.ports.size() == 0 || entry.state != FileState.STORE_COMPLETE) {
                sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }

            entry.state = FileState.REMOVE_IN_PROGRESS;
            entry.removeClient = client;

            for (int port : entry.ports) {
                Socket s = dStoresList.get(port);
                sendMessage(s, Protocol.REMOVE_TOKEN + " " + fileName);
            }

            new Thread(() -> {
                try {
                    Thread.sleep(timeout);
                    entry.didRemoveTimeout.set(true);
                    if (replicationFactor != entry.removeCounter.get()) {
                        System.err.println("REMOVE ERROR: failed to remove file " + fileName + " on time");
                    }
                } catch (InterruptedException ignored) {
                }
            }).start();
        }
    }

    private void onRemoveACK(String[] args, Socket client) {
        if (args.length != 2) {
            System.err.println("Invalid REMOVE_ACK");
            return;
        }

        IndexEntry entry = getFileEntry(args[1]);
        if (entry == null) return;

        if (entry.removeCounter.incrementAndGet() == replicationFactor && !entry.didRemoveTimeout.get()) {
            entry.state = FileState.REMOVE_COMPLETE;
            index.remove(entry);
            sendMessage(entry.removeClient, Protocol.REMOVE_COMPLETE_TOKEN);
        }
    }

    private void onListQuery(Socket client) {
        while (isRebalancing());

        if (notEnoughStores()) {
            sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        StringBuilder fileList = new StringBuilder(Protocol.LIST_TOKEN);
        for (IndexEntry entry : index) {
            if (entry.state == FileState.STORE_COMPLETE) fileList.append(" ").append(entry);
        }
        sendMessage(client, fileList.toString());
    }

    private void onRebalanceListQuery(String[] args, Socket client) {
        Set<String> files = new HashSet<>();
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("") || hasRebalanceTimeouted.get()) continue;
            files.add(args[i]);
            fileCounter.put(args[i], fileCounter.getOrDefault(args[i], 0) + 1);
        }
        if (hasRebalanceTimeouted.get()) return;
        storedFiles.add(Map.entry(getDStorePort(client.getPort()), new StoredFiles(files)));

        if (listResponseCounter.incrementAndGet() == rebalancingDStores.size()) {
            filterFiles();
            lowerBound = Math.floorDiv(replicationFactor * fileCounter.size(), rebalancingDStores.size());

            ensureFilesAreReplicated();
            storedFiles.sort(Comparator.comparingInt(o -> o.getValue().size()));
            distributeFiles();
            sendRebalanceMessages();
            rebalanceCompleteTimer();
        }
    }

    private void onRebalanceComplete(Socket client) {
        if (rebalanceCompleteTimeouted.get()) return;

        rebalanceCompleteACKs.add(getDStorePort(client.getPort()));
        if (rebalanceCompleteACKs.size() == rebalancingDStores.size()) {
            System.out.println("FINISHED REBALANCING");
            updateIndex();
        }
    }

    synchronized private void startRebalance() {
        if (rebalanceThread != null) {
            rebalanceThread.interrupt();
        }

        System.out.print("Tries to Start a Rebalance..");
        if (!notEnoughStores())
            synchronized (rebalanceLock) {
                System.out.println("OK");
                if (rebalanceTimerThread != null) rebalanceTimerThread.interrupt();
                if (rebalanceListTimerThread != null) rebalanceListTimerThread.interrupt();

                while (areThereAnyStoresOrRemoves());

                isRebalancing.set(true);
                fileCounter.clear();
                storedFiles.clear();

                listResponseCounter.set(0);
                rebalanceCompleteACKs.clear();
                hasRebalanceTimeouted.set(false);
                rebalanceCompleteTimeouted.set(false);
                rebalancingDStores.clear();
                rebalancingDStores.putAll(dStoresList);

                for (Socket dstore : rebalancingDStores.values()) sendMessage(dstore, "LIST");

                rebalanceListTimerThread = new Thread(() -> {
                    try {
                        Thread.sleep(timeout);
                        hasRebalanceTimeouted.set(true);
                        if (listResponseCounter.get() != rebalancingDStores.size()) {
                            removeDeadDstores();

                            if (rebalancingDStores.size() >= replicationFactor) {
                                filterFiles();
                                lowerBound = Math.floorDiv(replicationFactor * fileCounter.size(), rebalancingDStores.size());

                                ensureFilesAreReplicated();
                                storedFiles.sort(Comparator.comparingInt(o -> o.getValue().size()));
                                distributeFiles();
                                sendRebalanceMessages();
                                rebalanceCompleteTimer();
                            } else {
                                isRebalancing.set(false);
                                System.err.println("Failed to rebalance, because there aren't enough DStores");
                            }
                        }
                    } catch (InterruptedException ignored) {
                    }
                });
                rebalanceListTimerThread.start();
                while (isRebalancing()) ;
        } else System.out.println("not enough Dstores");

        if (rebalanceThread != null) {
            rebalanceThread.interrupt();
        }
        startRebalanceThread();
    }

    private void startRebalanceThread() {
        rebalanceThread = new Thread(() -> {
            try {
                Thread.sleep(rebalancePeriod);
                startRebalance();
            } catch (InterruptedException ignored) {
            }
        });
        rebalanceThread.start();
    }

    private void rebalanceCompleteTimer() {
        if (rebalanceTimerThread != null) rebalanceTimerThread.interrupt();
        rebalanceTimerThread = new Thread(() -> {
            try {
                Thread.sleep(timeout);
                rebalanceCompleteTimeouted.set(true);
                if (rebalanceCompleteACKs.size() != rebalancingDStores.size()) {
                    for (Map.Entry<Integer, Socket> rs : rebalancingDStores.entrySet())
                        if (!rebalanceCompleteACKs.contains(rs.getKey()))
                            System.err.println(rs.getKey() + " failed to send REBALANCE_COMPLETE on time");

                    updateIndex();
                }
            } catch (InterruptedException ignored) {
            }
        });

        rebalanceTimerThread.start();
    }

    private void ensureFilesAreReplicated() {
        for (Map.Entry<String, Integer> e : fileCounter.entrySet()) {
            String fileName = e.getKey();
            Integer replicas = e.getValue();
            if (replicas == replicationFactor) continue;

            int n = Math.abs(replicas - replicationFactor);
            storedFiles.sort(Comparator.comparingInt(o -> o.getValue().size()));

            if (replicas < replicationFactor) {
                for (Map.Entry<Integer, StoredFiles> currentEntry : storedFiles) {
                    if (currentEntry.getValue().contains(fileName) || n == 0) continue;
                    currentEntry.getValue().add(fileName, getStoreEntry(fileName));
                    n--;
                }
                continue;
            }
            for (int i = storedFiles.size() - 1; i >= 0; i--) {
                StoredFiles container = storedFiles.get(i).getValue();
                if (!container.contains(fileName) || n == 0) continue;
                container.remove(fileName, true);
                n--;
            }
        }
    }

    private void distributeFiles() {
        for (Map.Entry<Integer, StoredFiles> currentEntry : storedFiles) {
            StoredFiles currentContainer = currentEntry.getValue();

            while (currentContainer.isHungry() || currentContainer.isOverflown()) {
                for (Map.Entry<Integer, StoredFiles> otherEntry : storedFiles) {
                    if ((currentEntry.equals(otherEntry)) || (!currentContainer.isOverflown() && !currentContainer.isHungry()))
                        continue;

                    boolean isHungry = currentContainer.isHungry();
                    Map.Entry<Integer, StoredFiles> sender = isHungry ? otherEntry : currentEntry;
                    Map.Entry<Integer, StoredFiles> receiver = isHungry ? currentEntry : otherEntry;
                    if ((isHungry && sender.getValue().hasRoom()) || (!isHungry && receiver.getValue().isFull()))
                        continue;

                    for (StoredFiles.StoredFile storedFile : sender.getValue()) {
                        String fileToTransfer = storedFile.file;
                        if (!storedFile.isAvailable() || receiver.getValue().contains(fileToTransfer)) continue;

                        boolean shouldClone = storedFile.status == StoredFiles.StoredFile.StoredFileStatus.ADDED;
                        if (shouldClone) {
                            sender = storedFile.origin;
                            storedFile.status = StoredFiles.StoredFile.StoredFileStatus.REMOVED;
                        }
                        receiver.getValue().add(fileToTransfer, sender);
                        if (!shouldClone) sender.getValue().remove(fileToTransfer, true);
                        break;
                    }
                }
            }
        }
    }

    private void sendRebalanceMessages() {
        storedFiles.forEach(e -> {
            int port = e.getKey();

            Map<String, Set<Integer>> files = new HashMap<>();
            storedFiles.forEach(e1 -> {
                if (!e1.equals(e)) {
                    e1.getValue().forEach(f -> {
                        if (f.isAdded() && f.origin.getKey() == port) {
                            Set<Integer> set = files.getOrDefault(f.file, new HashSet<>());
                            set.add(e1.getKey());
                            files.put(f.file, set);
                        }
                    });
                }
            });

            StringBuilder filesToSend = new StringBuilder().append(files.size());
            for (Map.Entry<String, Set<Integer>> f : files.entrySet()) {
                filesToSend.append(" ").append(f.getKey()).append(" ").append(f.getValue().size());
                for (int p : f.getValue()) filesToSend.append(" ").append(p);
            }

            int deleted = 0;
            StringBuilder filesToRemove = new StringBuilder();
            for (StoredFiles.StoredFile r : e.getValue()) {
                if (r.status == StoredFiles.StoredFile.StoredFileStatus.DELETED) {
                    filesToRemove.append(" ").append(r.file);
                    deleted++;
                }
            }

            String rebalanceMessage = Protocol.REBALANCE_TOKEN + " " + filesToSend.toString() + " " + deleted + filesToRemove.toString();

            if (rebalanceMessage.equals("REBALANCE 0 0")) {
                System.out.println("The DStore on port " + port + " is already rebalanced");
                rebalanceCompleteACKs.add(port);
                if (rebalanceCompleteACKs.size() == rebalancingDStores.size()) {
                    System.out.println("FINISHED REBALANCING");
                    updateIndex();
                }
            } else sendMessage(rebalancingDStores.get(port), rebalanceMessage);
        });
    }

    private void filterFiles() {
        List<String> itemsToRemove = new ArrayList<>();
        for (Map.Entry<String, Integer> file : fileCounter.entrySet()) {
            String fileName = file.getKey();
            IndexEntry entry = getFileEntry(fileName);

            if (entry == null || entry.shouldBeDeleted()) {
                itemsToRemove.add(fileName);

                storedFiles.forEach(files -> {
                    StoredFiles container = files.getValue();
                    if (container.contains(fileName)) container.remove(fileName, true);
                });
            }
            if (entry != null && entry.shouldBeDeleted()) index.remove(entry);
        }
        itemsToRemove.forEach(fileCounter::remove);
    }

    private boolean areThereAnyStoresOrRemoves() {
        for (IndexEntry entry : index)
            if (entry.state == FileState.STORE_IN_PROGRESS || (entry.state == FileState.REMOVE_IN_PROGRESS && !entry.didRemoveTimeout.get()))
                return true;

        return false;
    }

    private int getDStorePort(int connectionPort) {
        for (Map.Entry<Integer, Socket> e : rebalancingDStores.entrySet())
            if (e.getValue().getPort() == connectionPort) return e.getKey();
        return 0;
    }

    private Map.Entry<Integer, StoredFiles> getStoreEntry(String fileName) {
        for (Map.Entry<Integer, StoredFiles> sender : storedFiles)
            if (sender.getValue().containsOriginal(fileName))
                return sender;
        return null;
    }

    private void updateIndex() {
        for (IndexEntry e : index) e.ports.clear();

        List<IndexEntry> entriesToRemove = new ArrayList<>();
        Set<String> totalFiles = fileCounter.keySet();
        index.forEach(indexEntry -> {
            if (!totalFiles.contains(indexEntry.fileName) || indexEntry.shouldBeDeleted()) entriesToRemove.add(indexEntry);
        });
        entriesToRemove.forEach(index::remove);

        storedFiles.forEach(e -> {
            int port = e.getKey();
            for (StoredFiles.StoredFile file : e.getValue()) {
                IndexEntry entry = getFileEntry(file.file);
                if (entry != null) {
                    entry.loads.clear();
                    entry.state = FileState.STORE_COMPLETE;
                    entry.ports.add(port);
                }
            }
        });

        isRebalancing.set(false);
    }

    private void removeDeadDstores() {
        List<Map.Entry<Integer, Socket>> dstoresToRemove = new ArrayList<>();
        for (Map.Entry<Integer, Socket> connection : dStoresList.entrySet()) {
            boolean wasReceived = false;
            for (Map.Entry<Integer, StoredFiles> store : storedFiles)
                if (store.getKey().equals(connection.getKey())) {
                    wasReceived = true;
                    break;
                }

            if (!wasReceived) {
                Socket s = connection.getValue();
                if (s.isConnected()) {
                    try {
                        s.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                dstoresToRemove.add(connection);
                System.out.println("Dropped the Connection with DStore " + connection.getKey());
            }
        }
        for (Map.Entry<Integer, Socket> r : dstoresToRemove) {
            dStoresList.remove(r.getKey());
            rebalancingDStores.remove(r.getKey());
        }
    }

    private boolean isRebalancing() {
        return isRebalancing.get();
    }

    private IndexEntry getFileEntry(String fileName) {
        for (IndexEntry entry : index) if (entry.fileName.equals(fileName)) return entry;
        return null;
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

    private enum FileState {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    private static class IndexEntry {
        private final String fileName;
        private final long fileSize;
        private final List<Integer> ports;
        private final AtomicInteger storeCounter;
        private final AtomicBoolean didStoreTimeout;
        private final AtomicInteger removeCounter;
        private final AtomicBoolean didRemoveTimeout;
        private final Socket client;
        private final Map<Integer, Integer> loads;
        private FileState state;
        private Socket removeClient;

        private IndexEntry(String fileName, long fileSize, Socket client) {
            this.state = FileState.STORE_IN_PROGRESS;
            this.fileName = fileName;
            this.fileSize = fileSize;
            this.client = client;
            this.ports = Collections.synchronizedList(new ArrayList<>());
            ;

            this.storeCounter = new AtomicInteger(0);
            this.didStoreTimeout = new AtomicBoolean(false);

            this.removeCounter = new AtomicInteger(0);
            this.didRemoveTimeout = new AtomicBoolean(false);
            this.loads = new Hashtable<>();
        }

        private boolean shouldBeDeleted() {
            return state == FileState.REMOVE_COMPLETE || state == FileState.REMOVE_IN_PROGRESS;
        }

        @Override
        public String toString() {
            return fileName;
        }
    }

    private static class StoredFiles extends HashSet<StoredFiles.StoredFile> {

        public StoredFiles(Set<String> value) {
            value.forEach(v -> add(new StoredFile(v)));
        }

        public void add(String fileName, Map.Entry<Integer, StoredFiles> origin) {
            StoredFile file = new StoredFile(fileName);
            file.status = StoredFile.StoredFileStatus.ADDED;
            file.origin = origin;
            add(file);
        }

        @Override
        public int size() {
            int size = 0;
            for (StoredFile f : this) if (f.isAvailable()) size++;
            return size;
        }

        public boolean contains(String fileName) {
            for (StoredFiles.StoredFile file : this) if (file.file.equals(fileName) && file.isAvailable()) return true;
            return false;
        }

        public boolean containsOriginal(String fileName) {
            for (StoredFiles.StoredFile file : this)
                if (file.file.equals(fileName) && file.status == StoredFile.StoredFileStatus.ORIGINAL) return true;
            return false;
        }

        public void remove(String fileName, boolean wasDeleted) {
            StoredFiles.StoredFile file = null;
            for (StoredFiles.StoredFile f : this) if (f.file.equals(fileName)) file = f;
            if (file == null) return;
            file.status = wasDeleted ? StoredFile.StoredFileStatus.DELETED : StoredFile.StoredFileStatus.REMOVED;
        }

        private boolean isOverflown() {
            return size() > lowerBound + 1;
        }

        private boolean isHungry() {
            return size() < lowerBound;
        }

        private boolean isFull() {
            return size() == lowerBound + 1;
        }

        private boolean hasRoom() {
            return size() <= lowerBound;
        }

        private static class StoredFile {
            private final String file;
            private StoredFileStatus status;
            private Map.Entry<Integer, StoredFiles> origin;

            public StoredFile(String file) {
                this.file = file;
                this.status = StoredFileStatus.ORIGINAL;
            }

            private boolean isAvailable() {
                return isAdded() || status == StoredFileStatus.ORIGINAL;
            }

            private boolean isAdded() {
                return status == StoredFileStatus.ADDED;
            }

            private enum StoredFileStatus {
                ORIGINAL,
                REMOVED,
                DELETED,
                ADDED
            }
        }
    }
}