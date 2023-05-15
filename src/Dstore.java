import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

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
     * Main setup of the Dstore, setups up its main values then stats the programs main loop.
     * @param args Values which are used in setting up the Dstore.
     */
    public static void main(String[] args) {
        // Defines base values which are required.
        Integer dstorePort;
        Integer controllerPort;

        // Sets up the main values inputted from the command line.
        try {
            dstorePort = Integer.getInteger(args[0]);
            controllerPort = Integer.getInteger(args[1]);
            timeoutMilliseconds = Integer.getInteger(args[2]);
            fileFolder = args[3];
        } catch (Exception exception) {
            System.err.println("Not all arguments inputted: " + exception);
            return;
        }

        // If succesful in generating the values for the Dstore then all its old data is removed (if the folder exists).
        File folder = new File(fileFolder);
        if (!folder.exists()) {folder.mkdir();}
        else {clearFileFolder(folder);}

        // Creates the socket for the controller then connects the Dstore to the controller via said socket.
        try {
            controllerSocket = new Socket(InetAddress.getLoopbackAddress(), controllerPort);
            sendMessage(Protocol.JOIN_TOKEN, String.valueOf(dstorePort), controllerSocket);
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
                while((currentMessage = reader.readLine()) != null){
                    System.out.println(currentMessage+" received");
                    //ADD CODE FOR CALLING PARSER HERE
                    //messageParser(currentMessage);
                }
                connectedSocket.close();
            }
            // If the program encounters an excpetion an error is flagged.
            catch(Exception e) { System.err.println("Error: " + e); }
        }

        /**
         * Function which is used to parse the messages sent by a Client or the Controller.
         * @param message The message which is being sent by the Client or Controller.
         * @param port The port that the Client or Controller is connected on.
         */
        private void messageParser(String message, String port) {
            // Splits the inputted message into an array.
            String messageArgs[] = message.split(" ");

            // Uses switch to check which message the port sent and run the required function.
            switch(messageArgs[0]) {
                case Protocol.STORE_TOKEN -> clientStore(messageArgs[1], messageArgs[2]);                    // When the client wants to store a file at the particular Dstore.
                case Protocol.LOAD_DATA_TOKEN -> clientLoadData(messageArgs[1]);                             // When the client wants particular data from the Dstore.
                case Protocol.REMOVE_TOKEN -> clientRemove(messageArgs[1]);                                  // When the controller wants the Dstore to remove a particular file.
                case Protocol.REBALANCE_TOKEN -> controllerRebalance(message);                               // When the Dstore is to be changed by sending file to other Dstores and removing its own files.
                case Protocol.REBALANCE_STORE_TOKEN -> dstoreRebalanceStore(messageArgs[1], messageArgs[2]); // When another Dstore is sending a file to the current Dstore.
                default -> System.err.println("Malformed message [" + messageArgs + "] recieved from [Port:" + port + "]."); // Malformed message is recieved.
            }
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
                byte[] buf = new byte[Integer.getInteger(filesize)]; // Does this need to be file size??
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
            File newFile = new File(fileFolder + File.separator + filename);

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
         * Function which is used when the controller calls for a rebalance of the files stored in the distributed system.
         * @param message The unaltered orginal message so it can be read properly for future function.
         */
        private void controllerRebalance(String message){}

        /**
         * Function which is used when another Dstore want to send this specific Dstore a file.
         * @param filename The name of the file the Dstore wants to send.
         * @param filesize The size of the file the Dstore wants to send
         */
        private void dstoreRebalanceStore(String filename, String filesize){}
    }
}