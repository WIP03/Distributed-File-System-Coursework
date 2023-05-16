import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Main brains of the system, controls file allocation and how said files should be stored after a rebalance.
 */
public class Controller {

    /**
     * The server socket which the controller is using to communicate with different clients and Dstores.
     */
    private static ServerSocket controllerSocket;

    /**
     * The number of Dstores which should be used to store a file.
     */
    private static Integer replicationFactor;

    /**
     * The time between process and the response its waiting for.
     */
    private static Integer timeoutMilliseconds;

    /**
     * The amount of seconds between rebalance periods.
     */
    private static Integer rebalancePeriod;

    /**
     * Contains all the current files in the system and the size of the given file.
     * HashMap paring goes as follows [FILE, SIZE].
     */
    private static HashMap<String,String> fileSize;

    /**
     * Contains all the current files in the system and the current operations they are going under.
     * HashMap paring goes as follows [FILE, CONTEXT].
     */
    private static HashMap<String,String> indexes;

    /**
     * Contains all the current files which are undergoing an operation and there CountDownLatch
     * HashMap paring goes as follows [FILE, LATCH].
     */
    private static HashMap<String,CountDownLatch> fileLatches;

    /**
     * Contains all the ports for the connected Dstore's and the files each Dstore has.
     * HashMap paring goes as follows [DSTORE_PORT, FILES].
     */
    private static HashMap<Integer,ArrayList<String>> dstores;

    /**
     * Main setup of the controller, setups up its main values then stats the programs main loop.
     * @param args Values which are used in setting up the controller.
     */
    public static void main(String[] args) {
        // Defines base values which are required.
        Integer controllerPort;

        // Sets up the main values inputted from the command line.
        try {
            controllerPort = Integer.parseInt(args[0]);
            System.out.println("Port is: " + args[0]);
            replicationFactor = Integer.parseInt(args[1]);
            timeoutMilliseconds = Integer.parseInt(args[2]);
            rebalancePeriod = Integer.parseInt(args[3]);
            fileSize = new HashMap<>();
            indexes = new HashMap<>();
            fileLatches = new HashMap<>();
            dstores = new HashMap<>();
        } catch (Exception exception) {
            System.err.println("Error: (" + exception + "), arguments are either of wrong type or not inputted at all.");
            return;
        }

        // Trys binding the server socket to the port before starting the controllers main loop.
        try {
            controllerSocket = new ServerSocket(controllerPort);
            while(true) { socketLoop(); }
        }

        // Returns an error if a problem happens trying to bind the port before the loop.
        catch (Exception exception){
            System.err.println("Error: (" + exception + "), unable to bind the port.");
        }

        // Clean up code which runs after the final try catch to close the port.
        finally{
            if (!controllerSocket.isClosed()) {
                try {controllerSocket.close();}
                catch(IOException exception) {System.err.println("Error: (" + exception + "), couldn't close port.");}
            }
        }
    }

    /**
     * Used to rebalance the storage system. (IMPROVE DESCRIPTION LATER)
     */
    private static void storageRebalanceOperation() {}

    /**
     * Function which is used to send a particular message to a given socket.
     * @param protocol The type of message which is being sent.
     * @param parameters The values which are contained in the message.
     * @param socket The socket we are trying to send said message on.
     * @throws IOException Occours when an error occours with the {@link PrintWriter}.
     */
    private static void sendMessage(String protocol, Object parameters, Socket socket) throws IOException {
        // Creates a new print writer for the given socket which auto flushes its inputs.
        PrintWriter socketOut = new PrintWriter(socket.getOutputStream(), true);

        // Creates the standard output, if its parameter is not null then its modified.
        String output = protocol;
        if (parameters != null) {output = protocol + " " + parameters;}

        // Sends the message then terminates the line before automatically flushing it so it gets to its destination.
        socketOut.println(output);
    }

    /**
     * Main loop for the controller, trys to connect new sockets to the system then starts there own thread.
     */
    private static void socketLoop(){
        System.out.println("Socket loop begins here");
        // Trys accepting the new socket before running its own thread.
        try {
            Socket newConnection = controllerSocket.accept();
            new Thread(new ControllerThread(newConnection)).start();
        }

        // Catches any errors that occour with the IO during the connection.
        catch (IOException exception){
            System.err.println("Error: (" + exception + "), happend on the current thread with its IO.");
        }
    }

    /**
     * Thread for the controller, handles a socket until connection is lost.
     */
    static class ControllerThread implements Runnable {

        /**
         * Stores the socket which is being managed by this current thread.
         */
        private Socket connectedSocket;

        /**
         * A value which lets the rest of the thread know if its a Dstore (by default set to false).
         */
        private Boolean isDstore = false;

        /**
         * Stores a list of ports which the client has already loaded from (resets on new normal load).
         */
        private ArrayList<Integer> loadedFromPorts = new ArrayList<>();

        /**
         * Used when initilising the thread, sets the socket before the threads main loop starts in run.
         * @param inputtedSocket The socket which the thread is connected to.
         */
        ControllerThread(Socket inputtedSocket) {
            connectedSocket = inputtedSocket;
        }

        /**
         * Main loop which is ran until the connection to the port is lost or the controller crashes.
         */
        public void run(){
            // Trys to create a reader for the input stream and then parse the messages its recieves from the socket.
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connectedSocket.getInputStream()));
                String currentMessage;
                while((currentMessage = reader.readLine()) != null){
                    System.out.println(currentMessage+" received");
                    //ADD CODE FOR CALLING PARSER HERE
                    //messageParser(currentMessage);
                }
                connectedSocket.close();
            }

            // If the program encounters an excpetion an error is flagged.
            catch(Exception e) { System.err.println("Error: " + e); }

            // If the thread is for a Dstore then it removes it from the list on disconnect to help with all operations (including rebalance).
            finally { if (isDstore) {dstores.remove(connectedSocket.getPort());} }
        }

        /**
         * Function which is used to parse the messages sent by a Client or Dstore.
         * @param message The message which is being sent by the Client or Dstore.
         */
        private void messageParser(String message) {
            // Splits the inputted message into an array.
            String messageArgs[] = message.split(" ");

            // Uses switch to check which message the port sent and run the required function.
            switch(messageArgs[0]) {
                case Protocol.STORE_TOKEN -> clientStore(messageArgs[1], messageArgs[2]);               // When a client wants a files to be store in the system.
                case Protocol.LOAD_TOKEN -> clientLoad(messageArgs[1]);                                 // When a client wants to get a file from the system.
                case Protocol.RELOAD_TOKEN -> clientReload(messageArgs[1]);                             // Whem a client wants a file from the system but the given Dstore doesn't work.
                case Protocol.REMOVE_TOKEN -> clientRemove(messageArgs[1]);                             // When a client wants a file to be removed from the system.
                case Protocol.LIST_TOKEN -> clientList();                                               // When a client wants a list of all files in the system.
                case Protocol.JOIN_TOKEN -> dstoreJoin(messageArgs[1]);                                 // When a Dstore joins the controller.
                case Protocol.STORE_ACK_TOKEN -> dstoreStoreAck(messageArgs[1]);                        // When a Dstore acknowledges storing a specific file.
                case Protocol.REMOVE_ACK_TOKEN -> dstoreRemoveAck(messageArgs[1]);                      // When a Dstore acknowledges removing a specific file.
                case Protocol.ERROR_FILE_DOES_NOT_EXISTS_TOKEN -> dstoreFileNotExist(messageArgs[1]);   // When a Dstore finds out it doesn't contain a given file during a remove process.
                default -> System.err.println("Error: malformed message [" + messageArgs + "] recieved from [Port:" + connectedSocket.getPort() + "]."); // Malformed message is recieved.
            }
        }

        /**
         * Function which handles storage of new files into the distributed system.
         * @param filename The name of the file the client wants to store.
         * @param filesize The size of the file the client wants to store.
         */
        private void clientStore(String filename, String filesize) {
            // Checks if the file that wants to be stored is already in the system (and not completed it's removal), if so it sends an error and stops processing.
            if (indexes.containsKey(filename) && (indexes.get(filename) != Index.REMOVE_COMPLETE_TOKEN)) {
                try { sendMessage(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN, null, connectedSocket); }
                catch (IOException exception) { System.err.println("Error: unable to send file already exists error to port: " + connectedSocket.getPort()); }
                finally{ return; }
            }

            // Checks if there isn't enough Dstores for the operation to occour, if so it sends an error and stops processing.
            if (indexes.size() < replicationFactor) {
                try { sendMessage(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, null, connectedSocket); }
                catch (IOException exception) { System.err.println("Error: unable to send not enough dstores error to port: " + connectedSocket.getPort()); }
                finally{ return; }
            }

            // Adds the file to the HashMap of indexes with the state of "store in progress" (plus to a filesize HashMap)
            indexes.put(filename, Index.STORE_PROGRESS_TOKEN);
            fileSize.put(filename, filesize);

            // Creates a latch for the current file so we can wait for its completion.
            CountDownLatch currentLatch = new CountDownLatch(replicationFactor);
            fileLatches.put(filename, currentLatch);

            // Extracts all the Dstores into an array then creates a message argument containing the first R ports to save to.
            Set<Integer> setOfDstores = dstores.keySet();
            Integer[] currentDstores = setOfDstores.stream().toArray(n -> new Integer[n]);
            String messageArguments = "";
            for (int i = 0; i < replicationFactor; i++) { messageArguments += (currentDstores[i] + " "); }

            // Sends the Dstores to the client where we want the data to be stored.
            try { sendMessage(Protocol.STORE_TO_TOKEN, messageArguments, connectedSocket); }

            // Catches issues that occour when the message cant be received by the client (ends operation and removes index/latch).
            catch (IOException exception) {
                System.err.println("Error: (" + exception + "), unable to join controller.");
                indexes.remove(filename);
                fileLatches.remove(filename);
                return;
            }

            // Trys checking if all Dstores have recieved the message
            try {
                // If the files are stored in all Dstores in time then store complete is sent and the index is updated to reflect this.
                if (fileLatches.get(filename).await(timeoutMilliseconds, TimeUnit.MILLISECONDS)) {
                    indexes.put(filename, Index.STORE_COMPLETE_TOKEN);
                    sendMessage(Protocol.STORE_COMPLETE_TOKEN, null, connectedSocket);
                }

                // As file is though to have not properly been saved it is removed from the system.
                else { indexes.remove(filename); }
            }

            // Sends error if an error occurs during either the latching or sending the message to the client.
            catch (Exception exception) {
                System.err.println("Error: Unable to makesure files are saved (exception: " + exception + ").");
                indexes.remove(filename);
            }

            // Removes the latch as its no longer needed.
            finally { fileLatches.remove(filename); }

            // MAYBE MAKE DSTORES INFO STORED IN THERE OWN OBJECT? (PLUS MAKE INDEX ENUMS).

            // SEND DSTORES TO CLIENT, (RECORD FILE NAME, CLIENT PORT AND DSTORES PORTS IN A HASH MAP, REMOVE PORT AFTER GETTING AN ACK TOKEN)
            // CHECK FOR ALL RECIEVED UPDATE INDEX FOR FILE AND SEND MESSAGE TO INITIAL CLIENT IF GOTTEN WITHIN TIMEOUT (IF NOT THEN JUST EXIT THIS).
        }

        /**
         * Function which handles the loading of files from the distributed system.
         * @param filename The name of the file the client wants to load.
         */
        private void clientLoad(String filename) {
            // Resets the loaded from ports as its a new load.
            loadedFromPorts = new ArrayList<>();

            // Calls clientReload as the rest of the code is the same.
            clientReload(filename);
        }

        /**
         * Function which handles loading the same file from the distributed system but with a different Dstore (as the last lot failed).
         * @param filename The name of the file the client wants to load from a new Dstore.
         */
        private void clientReload(String filename) {
            // Checks if the file that the client wants to load doesn't exists (or hasn't completed its store) in the system, if so it sends an error and stops processing.
            if (!indexes.containsKey(filename) || (indexes.get(filename) != Index.STORE_COMPLETE_TOKEN)) {
                try { sendMessage(Protocol.ERROR_FILE_DOES_NOT_EXISTS_TOKEN, null, connectedSocket); }
                catch (IOException exception) { System.err.println("Error: unable to send file doesn't exists error to port: " + connectedSocket.getPort()); }
                finally{ return; }
            }

            // Checks if there isn't enough Dstores for the operation to occour, if so it sends an error and stops processing.
            if (indexes.size() < replicationFactor) {
                try { sendMessage(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, null, connectedSocket); }
                catch (IOException exception) { System.err.println("Error: unable to send not enough dstores error to port: " + connectedSocket.getPort()); }
                finally{ return; }
            }

            // Creates an ArrayList containing all current Dstores which could contain the file
            ArrayList<Integer> possibleDstores = new ArrayList<>();
            dstores.forEach((store,files) -> {
                if (files.contains(filename) && !loadedFromPorts.contains(store)) {
                    possibleDstores.add(store);
                }
            });

            // Tries to send the client the correct response for loading the file, if not possible an error is given.
            try {
                // Checks if there arn't any Dstores left to load files from, if so an error is sent to the client.
                if (possibleDstores.isEmpty()) {
                    sendMessage(Protocol.ERROR_LOAD_TOKEN, null, connectedSocket);
                    System.out.println("Error: Unable to load file (with name '" + filename + "') from any Dstore");
                }

                // Else it sends a random avalible Dstrore for the client to load the file from (and adds it to loaded from ports).
                else {
                    int argument = possibleDstores.get((int) Math.random() * possibleDstores.size());
                    sendMessage(Protocol.LOAD_FROM_TOKEN, argument, connectedSocket);
                    loadedFromPorts.add(connectedSocket.getPort());
                }

            }

            // Occours when an exception happens in when sending a message to the client.
            catch (IOException exception) { System.err.println("Error: unable to let the client know the current state of getting the file from the Dstore."); }
        }

        /**
         * Function which handles the removal of a file from the distributed system.
         * @param filename The name of the file the client wants to remove.
         */
        private void clientRemove(String filename) {
            // Checks if the file that the client wants to load doesn't exists (or hasn't completed its store) in the system, if so it sends an error and stops processing.
            if (!indexes.containsKey(filename) || (indexes.get(filename) != Index.STORE_COMPLETE_TOKEN)) {
                try { sendMessage(Protocol.ERROR_FILE_DOES_NOT_EXISTS_TOKEN, null, connectedSocket); }
                catch (IOException exception) { System.err.println("Error: unable to send file doesn't exists error to port: " + connectedSocket.getPort()); }
                finally{ return; }
            }

            // Checks if there isn't enough Dstores for the operation to occour, if so it sends an error and stops processing.
            if (indexes.size() < replicationFactor) {
                try { sendMessage(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, null, connectedSocket); }
                catch (IOException exception) { System.err.println("Error: unable to send not enough dstores error to port: " + connectedSocket.getPort()); }
                finally{ return; }
            }

            // Adds the file to the HashMap of indexes with the state of "store in progress" (plus to a filesize HashMap).
            indexes.put(filename, Index.REMOVE_PROGRESS_TOKEN);

            // Creates a latch for the current file so we can wait for its completion.
            CountDownLatch currentLatch = new CountDownLatch(replicationFactor);
            fileLatches.put(filename, currentLatch);

            // Goes through all Dstores that contain the file and sends them a remove command for that file.
            ArrayList<Integer> possibleDstores = new ArrayList<>();
            dstores.forEach((store,files) -> {
                if (files.contains(filename)) {
                    // Creates the socket for the Dstore which has the file then sends a message to it letting it know that it should remove said file
                    try {
                        Socket dstoreSocket = new Socket(InetAddress.getLoopbackAddress(), store);
                        sendMessage(Protocol.REMOVE_TOKEN, filename, dstoreSocket);
                    }

                    // Catches any issue that could occour when connecting to the Dstore.
                    catch (IOException exception) {
                        System.err.println("Error: (" + exception + "), unable to join controller.");
                        return;
                    }
                }
            });

            // Trys checking if all Dstores have recieved the message
            try {
                // If the files are removed from all Dstores in time then remove complete is sent and the index is updated to reflect this.
                if (fileLatches.get(filename).await(timeoutMilliseconds, TimeUnit.MILLISECONDS)) {
                    indexes.put(filename, Index.REMOVE_COMPLETE_TOKEN);
                    fileSize.remove(filename);
                    sendMessage(Protocol.REMOVE_COMPLETE_TOKEN, null, connectedSocket);
                }
            }

            // Sends error if an error occurs during either the latching or sending the message to the client.
            catch (Exception exception) {
                System.err.println("Error: Unable to makesure files are removed (exception: " + exception + ").");
            }

            // Removes the latch as its no longer needed.
            finally { fileLatches.remove(filename); }
        }

        /**
         * Function which handles the listing of files in the distributed system.
         */
        private void clientList() {
            // Checks if there isn't enough Dstores for the operation to occour, if so it sends an error and stops processing.
            if (indexes.size() < replicationFactor) {
                try { sendMessage(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, null, connectedSocket); }
                catch (IOException exception) { System.err.println("Error: unable to send not enough dstores error to port: " + connectedSocket.getPort()); }
                finally{ return; }
            }

            // Extracts all the files that exist in the indexes Hashmap that are fully stored in the system.
            ArrayList<String> allFiles = new ArrayList<>();
            indexes.forEach((file,context) -> { if(context == Index.STORE_COMPLETE_TOKEN) {allFiles.add(file);} });

            // Creates the arguement which includes all the files previously extracted.
            String argument = "";
            for (String file : allFiles) { argument += file + " "; }

            // Trys to send the client the list of all files in the system.
            try { sendMessage(Protocol.LIST_TOKEN, argument, connectedSocket); }
            catch (IOException exception) { System.err.println("Error: unable to send client list of avalible files in the system."); }
        }

        /**
         * Function which handles the joining of a new Dstore to the distributed system.
         */
        private void dstoreJoin(String port) {
            // Lets the Thread know that it is a Dstore for later use.
            isDstore = true;

            // Adds it to the HashMap of Dstores ready to be updated when files are added.
            dstores.put(Integer.parseInt(port), new ArrayList<String>());

            // Rebalances the storage system as a new Dstore has joined.
            storageRebalanceOperation();
        }

        /**
         * Function which handles when a particular Dstore is storing a new file sent by a client.
         * @param filename The name of the file the particular Dstore want's stored.
         */
        private void dstoreStoreAck(String filename) {
            // Check if the file is supposed to be getting stored (if not, it exits, giving us an error message in the console).
            if (!indexes.get(filename).equals(Index.STORE_PROGRESS_TOKEN)) {
                System.err.println("Error: acknowledging storage of file which has the incorrect index (its '" + indexes.get(filename) + "').");
                return;
            }

            // Counts down the latch to show are client thread that this Dstore has the file.
            fileLatches.get(filename).countDown();

            // As the file is now known to be stored at this Dstore it is added to the Controllers HashMap logging such fact.
            dstores.get(connectedSocket.getPort()).add(filename);
        }

        /**
         * Function which handles when a particular Dstore is removing the file a client asked it to.
         * @param filename The name of the file the particular Dstore want's removed.
         */
        private void dstoreRemoveAck(String filename) {
            // Check if the file is supposed to be getting removed (if not, it exits, giving us an error message in the console).
            if (!indexes.get(filename).equals(Index.REMOVE_PROGRESS_TOKEN)) {
                System.err.println("Error: acknowledging removal of file which has the incorrect index (its '" + indexes.get(filename) + "').");
                return;
            }

            // Counts down the latch to show are client thread that this Dstore has removed the file.
            fileLatches.get(filename).countDown();

            // As the file is now known to be removed at this Dstore it is removed from the Controllers HashMap logging such fact.
            dstores.get(connectedSocket.getPort()).remove(filename);
        }

        /**
         * Function which deals with the problem when we try to remove a file that doesn't exists.
         * @param filename The name of the file we tried to remove.
         */
        private void dstoreFileNotExist(String filename) {
            //Lets the sysyem know the file was never there before acknowledging it like if the file needed to be removed.
            System.err.println("Error: tried to remove file '" + filename + "' from Dstore with port '" + connectedSocket.getPort() +"' while file doesn't exists there.");
            dstoreRemoveAck(filename);
        }
    }
}