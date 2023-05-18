import java.io.*;
import java.lang.reflect.Array;import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;import java.util.HashMap;import java.util.List;
import java.util.concurrent.CountDownLatch;import java.util.concurrent.TimeUnit;import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.ArrayList;import static java.util.stream.Collectors.toCollection;

/**
 * Used for storing data in the distributed system, works with the main controller to make sure files are balanced around the given replication factor.
 */
public class Dstore {

    /**
     * The server socket which the dstore is using to communicate with different clients and the controller.
     */
    private static ServerSocket dstoreSocket;

    /**
     * The socket which the dstore uses to send information to the controller.
     */
    private static Socket controllerSocket;

    /**
     * The time between process and the response its waiting for.
     */
    private static Integer timeoutMilliseconds;

    /**
     * Where data should be stored locally.
     */
    private static String fileFolder;

    /**
     * Stores all the latches used in rebalance operations when waiting for an ack from another Dstore.
     */
    private static CountDownLatch rebalanceLatch;

    /**
     * Main setup of the Dstore, setups up its main values then stats the programs main loop.
     * @param args Values which are used in setting up the Dstore.
     */
    public static void main(String[] args) {
        // Defines base values which are required.
        Integer dstorePort;
        Integer controllerPort;

        // Sets up the main values inputted from the command line.
        try {
            dstorePort = Integer.parseInt(args[0]);
            controllerPort = Integer.parseInt(args[1]);
            timeoutMilliseconds = Integer.parseInt(args[2]);
            fileFolder = args[3];
        }

        // Returns when incorrect arguements are inputted on the command line.
        catch (Exception exception) {
            System.err.println("Error: (" + exception + "), arguments are either of wrong type or not inputted at all.");
            return;
        }

        // If succesful in generating the values for the Dstore then all its old data is removed (if the folder exists).
        File folder = new File(fileFolder);
        if (!folder.exists()) { folder.mkdirs(); } //MAYBE CHECK FOR IF WE CAN'T MAKE PATH???
        else { clearFileFolder(folder); }

        // Creates the socket for the controller then connects the Dstore to the controller via said socket.
        try {
            controllerSocket = new Socket(InetAddress.getLoopbackAddress(), controllerPort);
            sendMessage("JOIN", String.valueOf(dstorePort), controllerSocket);
        }

        // Catches any issue that could occour when connecting to the controller. (MAYBE PUT ME INSIDE OF LOOP TRY CATCH TO CLOSE SOCKET AT END).
        catch (IOException exception) {
            System.err.println("Error: (" + exception + "), unable to join controller.");
            return;
        }

        // Trys binding the server socket to the port before starting the dstoress main loop.
        try {
            dstoreSocket = new ServerSocket(dstorePort);
            while(true) { socketLoop(); }
        }

        // Returns an error if a problem happens trying to bind the port before the loop.
        catch (IOException exception) {
            System.err.println("Error: (" + exception + "), unable to bind the port.");
        }

        // Clean up code which runs after the final try catch to close the port.
        finally{
            if (!dstoreSocket.isClosed()) {
                try {dstoreSocket.close();}
                catch(IOException exception) {System.err.println("Error: (" + exception + "), couldn't close port.");}
            }
        }
    }

    /**
     * Clears the file folder removing all of its current contents.
     * @param folder The folder the contents we want to remove are in.
     */
    private static void clearFileFolder(File folder) {
        // Gets all the files and folders that exits in the current directory.
        File[] files = folder.listFiles();

        // Check if the folder contains files, if so loops through them removing them.
        if(files != null) {
            for(File f: files) {
                if(f.isDirectory()) {clearFileFolder(f);} // Checks for folders (probably not needed but is good to have).
                f.delete(); // Removes that file/folder from the folder.
            }
        }
    }

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
     * Main loop for the dstore, trys to connect new sockets to the system then starts there own thread.
     */
    private static void socketLoop() {
        // Trys accepting the new socket before running its own thread.
        try {
            Socket newConnection = dstoreSocket.accept();
            new Thread(new DstoreThread(newConnection)).start();
        }

        // Catches any errors that occour with the IO during the connection.
        catch (IOException exception){
            System.err.println("Error: (" + exception + "), happend on the current thread with its IO.");
        }
    }

    /**
     * Thread for the dstore, handles a socket until connection is lost.
     */
    static class DstoreThread implements Runnable {

        /**
         * Stores the socket which is being managed by this current thread.
         */
        private Socket connectedSocket;

        /**
         * Used when initilising the thread, sets the socket before the threads main loop starts in run.
         * @param inputtedSocket The socket which the thread is connected to.
         */
        DstoreThread(Socket inputtedSocket) {
            connectedSocket = inputtedSocket;
        }

        /**
         * Main loop which is ran until the connection to the port is lost or the dstore fails.
         */
        public void run(){
            // Trys to create a reader for the input stream and then parse the messages its recieves from the socket.
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connectedSocket.getInputStream()));
                String currentMessage;
                while((currentMessage = reader.readLine()) != null){ messageParser(currentMessage); }
                connectedSocket.close();
            }
            // If the program encounters an excpetion an error is flagged.
            catch(Exception e) { System.err.println("Error: -----" + e); }
            System.out.println(dstoreSocket.getLocalPort() + " PARSE DONE");
        }

        /**
         * Function which is used to parse the messages sent by a Client or the Controller.
         * @param message The message which is being sent by the Client or Controller.
         */
        private void messageParser(String message) {
            // Splits the inputted message into an array.
            String messageArgs[] = message.split(" ");
            System.out.println(dstoreSocket.getLocalPort() + " " + connectedSocket.getLocalPort() + ") " + String.join(" ", messageArgs));

            // Uses switch to check which message the port sent and run the required function.
            switch(messageArgs[0]) {
                case Protocol.STORE_TOKEN -> clientStore(messageArgs[1], messageArgs[2]);                    // When the client wants to store a file at the particular Dstore.
                case Protocol.LOAD_DATA_TOKEN -> clientLoadData(messageArgs[1]);                             // When the client wants particular data from the Dstore.
                case Protocol.REMOVE_TOKEN -> clientRemove(messageArgs[1]);                                  // When the controller wants the Dstore to remove a particular file.
                case Protocol.LIST_TOKEN -> controllerList();                                                // When the controller wants to get all the files stored in the current Dstore.
                case Protocol.REBALANCE_TOKEN -> controllerRebalance(messageArgs);                           // When the Dstore is to be changed by sending file to other Dstores and removing its own files.
                case Protocol.REBALANCE_STORE_TOKEN -> dstoreRebalanceStore(messageArgs[1], messageArgs[2]); // When another Dstore is sending a file to the current Dstore.
                case Protocol.ACK_TOKEN -> dstoreRebalanceAck();                                             // When another Dstore has acknowledged its connection with us.
                default -> System.err.println("Error: malformed message [" + String.join(" ", messageArgs) + "] recieved from [Port:" + connectedSocket.getPort() + "]."); // Malformed message is recieved.
            }
            System.out.println(dstoreSocket.getLocalPort() + " IS DONE");
        }

        /**
         * Function which handles storage of new files into the particular Dstore.
         * @param filename The name of the file the client wants to store.
         * @param filesize The size of the file the client wants to store.
         */
        private void clientStore(String filename, String filesize) {
            // Try's sending an acknowledgement message to the client, if not possible it ends the operation.
            try{ sendMessage(Protocol.ACK_TOKEN, null, connectedSocket); }
            catch (IOException exception) { System.err.println("Error: unable to tell client it got the message"); return; }

            // Try's to setup a timeout for reading information from the clients stream.
            try { connectedSocket.setSoTimeout(timeoutMilliseconds); }
            catch (SocketException exception) { System.err.println("Error: unable to setup timeout for client"); return; }

            // Used for getting the file from the client and storing it in the system before letting the controller know that it worked.
            try {
                // Gets the input and output for the file.
                InputStream reader = connectedSocket.getInputStream();
                OutputStream fileWriter = new BufferedOutputStream(new FileOutputStream(fileFolder + File.separator + filename));

                // Transfers the file from the input stream to the file.
                byte[] buf = new byte[2048]; // Does this need to be file size??
                int bytesRead;
                while((bytesRead = reader.read(buf)) != -1) {
                    fileWriter.write(buf, 0, bytesRead);
                }

                //Closes the internal connections before values removed by garbage collection.
                reader.close();
                fileWriter.close();

                // Try's sending acknowledgement message to controller that we stored a file, if not possible it ends the operation.
                try{ sendMessage(Protocol.STORE_ACK_TOKEN, filename, controllerSocket); }
                catch (IOException exception) { System.err.println("Error: unable to tell controller that we stored the file"); }
            }

            //Lets the user know if the Dstore can't save the file.
            catch (IOException exception) {
                System.err.println("Error: unable to load and or save file (exception: " + exception + ").");
            }

            // Try's to reset timeout for reading information from the clients stream as its no longer needed.
            finally{
                try { connectedSocket.setSoTimeout(0); }
                catch (SocketException exception) { System.err.println("Error: unable to remove timeout for client"); }
            }
        }

        /**
         * Function which handles loading of a file from the particular Dstore.
         * @param filename The name of the file the client wants to load.
         */
        private void clientLoadData(String filename) {
            // Try's to setup a timeout for sending information to the client.
            try { connectedSocket.setSoTimeout(timeoutMilliseconds); }
            catch (SocketException exception) { System.err.println("Error: unable to setup timeout for client"); return; }

            // Try's to load data from the given file and send it to the client.
            try {
                // Get the file from the input and gets the connection to send it on.
                InputStream fileReader = new FileInputStream(fileFolder + File.separator + filename);
                OutputStream writer = connectedSocket.getOutputStream();

                // Transfers the file to the client via the output stream.
                byte[] buf = new byte[2048]; // Does this need to be file size??
                int bytesRead;
                while((bytesRead = fileReader.read(buf)) != -1) {
                    writer.write(buf, 0, bytesRead);
                }

                //Closes the internal connections before values removed by garbage collection.
                fileReader.close();
                writer.close();
            }

            // Try's to remove the connection of the socket as the file doesn't exitst/cant be loaded from the Dstore.
            catch (IOException exception) {
                System.err.println("Error: unable to load file from Dstore with exception '" + exception + "'.");
                try { connectedSocket.close(); }
                catch (IOException exception1) { System.err.println("Error: cant close the threads socket."); }
            }

            // Try's to reset timeout for sending information to the clients stream as its no longer needed.
            finally{
                try { connectedSocket.setSoTimeout(0); }
                catch (SocketException exception) { System.err.println("Error: unable to remove timeout for sending data to client."); }
            }
        }

        /**
         * Function which handles removing of a file from the particular Dstore.
         * @param filename The name of the file the client wants to remove.
         */
        private void clientRemove(String filename) {
            // Creates a new File object in reference to the file we are trying to remove from the Dstore.
            File newFile = new File(fileFolder + "/" + filename);
            System.out.println(fileFolder + "/" + filename);

            //Checks if the file exits in the system, if so it trys to delete it.
            if (newFile.exists()) {
                // File tries to get deleted, if so acknoledgement is sent to the Controller.
                if (newFile.delete()) {
                    try{ sendMessage(Protocol.REMOVE_ACK_TOKEN, filename, controllerSocket); }
                    catch (IOException exception) { System.err.println("Error: unable to tell controller that we removed the file."); }
                }

                // If the file can't be deleted it logs an error as the file could still exist in the Dstore.
                else { System.err.println("Error: unable to remove the file from the system."); }
            }

            // Try's to tell the controller that the Dstore doesn't have the file.
            else {
                try{ sendMessage(Protocol.ERROR_FILE_DOES_NOT_EXISTS_TOKEN, filename, controllerSocket); }
                catch (IOException exception) { System.err.println("Error: unable to tell controller that the file doesn't exists at the Dstore."); }
            }
        }

        /**
         * Function which is used when the controller wants to find out all the files that are stored at this given dstore.
         */
        private void controllerList(){
            // Trys to get all the files in the dstore and send them to the controller to help in rebalance.
            try {
                // Gets all the files in the dstores directory relative to it (should allow sub directories if the files name includes them).
                File[] filesList = new File(fileFolder).listFiles();
                ArrayList<String> files = getFiles(filesList, null);

                // Converts the ArrayList of all the files to an easy to send single String.
                String argument = "";
                for (String file : files) { argument += file + " "; }

                // Sends the files the dstore has stored (with paths if they have them.
                sendMessage(Protocol.LIST_TOKEN, argument, controllerSocket);
            }

            // When either the Dstore can't get the files which its trying to store or can't send a message to the controller.
            catch (IOException exception) {
                System.err.println("Error: unable to send controller list of avalible files in the dstore (Exception: " + exception + " ).");
            }
        }

        /**
         * Gets all the files in the dstores folders.
         * @param filesList The current file list for the directory we are reading.
         * @param foldername The name of the folder.
         * @return All the files the Dstore has.
         */
        private ArrayList<String> getFiles(File[] filesList, String foldername) {
            // Gets and returns all the files in the folder.
            ArrayList<String> files = new ArrayList<>();
            if(filesList != null) {
                for(File f: filesList) {
                    if(f.isDirectory()) {files.addAll(getFiles(f.listFiles(), (f.getName() + "/")));}
                    if(foldername == null) { files.add(f.getName()); }
                    else { files.add(foldername + f.getName()); }
                }
            }
            return files;
        }

        /**
         * Function which is used when the controller calls for a rebalance of the files stored in the distributed system.
         * @param message The unaltered orginal message so it can be read properly for future function.
         */
        private void controllerRebalance(String[] message) {
            // Turns the inital message into an easier to manipulate ArrayList (removing from it the unneeded protocol string).
            ArrayList<String> messageArgs = new ArrayList<>(Arrays.asList(message));
            messageArgs.remove(0);

            // Creates an empty HashMap for storing all the files that need to move and where they should go.
            HashMap<String, ArrayList<Integer>> moveMap = new HashMap<>();

            // Gets the number of files that need to be moved and uses that in a loop to add files and there new dstores into the HashMap.
            Integer moveCount = Integer.parseInt(messageArgs.remove(0));
            for (int i = 0; i < moveCount; i++){
                String file = messageArgs.remove(0);
                moveMap.put(file,new ArrayList<>());
                for (int j = 0; j < Integer.parseInt(messageArgs.remove(0)); j++){moveMap.get(file).add(Integer.parseInt(messageArgs.remove(0)));}
            }

            // Creates an ArrayList storing all the files that need to be removed from the system.
            Integer removeCount = Integer.parseInt(messageArgs.remove(0));
            if (removeCount != messageArgs.size()) { System.err.println("Error: remove count '"+ removeCount +"' and number files to remove '"+ messageArgs.size() +"' are not equal (Store may not be correct)."); }
            ArrayList<String> removeList = new ArrayList<>(messageArgs);

            // Goes through each file sending said file to all the Dstores that need it.
            moveMap.keySet().forEach(filename -> {
                // Gets the file size of this given file.
                File thisFile = new File(fileFolder + "/" + filename);
                String fileSize = Integer.toString(Math.toIntExact(thisFile.length()));

                // Trys to load the files from this Dstore to all the ones that require said file.
                moveMap.get(filename).forEach(storePort -> {
                    try {
                        Socket socket = new Socket(InetAddress.getLocalHost(),storePort);;
                        dstoreRebalanceLoad(filename,fileSize,socket);
                    }
                    catch (Exception exception) {
                        System.err.println("Error: unable to create socket for sending files during rebalance.");
                    }
                });
            });

            // Removes all the files from the Dstore.
            removeList.forEach(filename -> dstoreRebalanceRemove(filename));

            // Tells the controller that the rebalance is complete.
            try{ sendMessage(Protocol.REBALANCE_COMPLETE_TOKEN, null, controllerSocket); System.out.println("REBALANCE TOKEN IS SENT");}
            catch (IOException exception) { System.err.println("Error: unable to tell controller that we completed the rebalance."); }
        }

        /**
         * Used during a rebalance to send files to another Dstore.
         * @param filename The name of the file we want to send.
         * @param filesize The size of the file we want to send.
         * @param socket The socket we are trying to send the file on.
         */
        private void dstoreRebalanceLoad(String filename, String filesize, Socket socket) {
            // Adds a new latch for the current instance of the file at this Dstore.
            rebalanceLatch = new CountDownLatch(1);

            // Sets up the socket and sends the store command to the other Dstore
            try { sendMessage(Protocol.REBALANCE_STORE_TOKEN, (filename + " " + filesize), socket); }

            // Lets the user know if a rebalance store isn't possible.
            catch (Exception exception) {System.err.println("Error: unable to send ack for store of rebalance at port '" + socket.getPort() +"'.");}

            // Trys checking if all Dstores have recieved the message, if so it log's it (If not the rebalance store process still continues but an error is logged).
            try { if (rebalanceLatch.await(timeoutMilliseconds, TimeUnit.MILLISECONDS)) { System.out.println("Successfully got storaage confirmation at port '" + socket.getPort() +"'.");} }

            // Sends error if not all dstore hasn't acknowledged its store to this dstore
            catch (Exception exception) { System.err.println("Error: Unable to get storage ack from dstore at port '" + socket.getPort() +"', resume regardless (exception: " + exception + ")."); }

            // Try's to setup a timeout for sending information to the the other dstore.
            try { socket.setSoTimeout(timeoutMilliseconds); }
            catch (SocketException exception) { System.err.println("Error: unable to setup timeout for other dstore"); return; }

            // Try's to load data from the given file and send it to the other dstore.
            try {
                // Get the file from the input and gets the connection to send it on.
                InputStream fileReader = new FileInputStream(fileFolder + File.separator + filename);
                OutputStream writer = socket.getOutputStream();

                // Transfers the file to the other dstore via the output stream.
                byte[] buf = new byte[2048]; // Does this need to be file size??
                int bytesRead;
                while((bytesRead = fileReader.read(buf)) != -1) {
                    writer.write(buf, 0, bytesRead);
                }

                //Closes the internal connections before values removed by garbage collection.
                fileReader.close();
                writer.close();
            }

            // Try's to remove the connection of the socket as the file doesn't exitst/cant be loaded from the Dstore.
            catch (IOException exception) {
                System.err.println("Error: unable to load file from Dstore with exception '" + exception + "'.");
            }

            // Try's to reset timeout for sending information to the dstore then closes its socket.
            finally{
                try { socket.setSoTimeout(0); socket.close();}
                catch (Exception exception) { System.err.println("Error: unable to remove timeout from/close socket with other Dstore."); }
            }

        }

        /**
         * Function which handles removing of a file from the particular Dstore during a rebalance.
         * @param filename The name of the file the controller wants to remove.
         */
        private void dstoreRebalanceRemove(String filename) {
            // Creates a new File object in reference to the file we are trying to remove from the Dstore.
            File newFile = new File(fileFolder + "/" + filename);
            System.out.println(fileFolder + "/" + filename);

            //Checks if the file exits in the system, if so it trys to delete it (if it can't delete it an error is returned).
            if (newFile.exists()) {
                if (!newFile.delete()) {System.err.println("Error: unable to remove the file '" + filename + "' from the system.");}
            }

            // Logs the the file didn't exist to begin with.
            else {System.err.println("Error: the file '" + filename + "' didn't exists at this Dstore."); }
        }

        /**
         * Function which is used when another Dstore want to send this specific Dstore a file.
         * @param filename The name of the file the Dstore wants to send.
         * @param filesize The size of the file the Dstore wants to send
         */
        private void dstoreRebalanceStore(String filename, String filesize) {
            // Try's sending an acknowledgement message to the other dstore.
            try{ sendMessage(Protocol.ACK_TOKEN, null, connectedSocket); }
            catch (IOException exception) { System.err.println("Error: unable to tell other dstore it got the message.");}

            // Try's to setup a timeout for reading information from the controller stream.
            try { connectedSocket.setSoTimeout(timeoutMilliseconds); }
            catch (SocketException exception) { System.err.println("Error: unable to setup timeout for other dstore."); return; }

            // Used for getting the file from the other dstore and stores it in the system.
            try {
                // Gets the input and output for the file.
                InputStream reader = connectedSocket.getInputStream();
                OutputStream fileWriter = new BufferedOutputStream(new FileOutputStream(fileFolder + File.separator + filename));

                // Transfers the file from the input stream to the file.
                byte[] buf = new byte[2048]; // Does this need to be file size??
                int bytesRead;
                while((bytesRead = reader.read(buf)) != -1) {
                    fileWriter.write(buf, 0, bytesRead);
                }

                // Closes the internal connections before values removed by garbage collection.
                reader.close();
                fileWriter.close();
            }

            // Lets the user know if the Dstore can't save the file.
            catch (IOException exception) { System.err.println("Error: unable to load and or save file (exception: " + exception + ")."); }

            // Try's to reset timeout for reading information from the other dstores stream as its no longer needed.
            finally{
                try { connectedSocket.setSoTimeout(0); }
                catch (SocketException exception) { System.err.println("Error: unable to remove timeout for client"); }
            }
        }

        /**
         * Function which handles when a particular Dstore has acknowledged its connection to the current one.
         */
        private void dstoreRebalanceAck() {
            // Counts down the latch to show the other dstore that a it sees the connection.
            rebalanceLatch.countDown();
        }
    }
}