import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

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

        // JOIN THE CONTROLLER

        // Trys binding the server socket to the port before starting the dstoress main loop.
        try {
            dstoreSocket = new ServerSocket(dstorePort);
            while(true) { socketLoop(); }
        }

        // Returns an error if a problem happens trying to bind the port before the loop.
        catch (IOException exception){
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
     * Main loop for the dstore, trys to connect new sockets to the system then starts there own thread.
     */
    private static void socketLoop(){

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
        private static void messageParser(String message, String port) {
            // Splits the inputted message into an array.
            String messageArgs[] = message.split(" ");

            // Uses switch to check which message the port sent and run the required function.
            switch(messageArgs[0]) {
                case Protocol.STORE_TOKEN -> clientStore(messageArgs[1], messageArgs[2]);                    // When the client wants to store a file at the particular Dstore.
                case Protocol.LOAD_DATA_TOKEN -> clientLoadData(messageArgs[1]);                             // When the client wants particular data from the Dstore.
                case Protocol.REMOVE_TOKEN -> clientRemove(messageArgs[1]);                                  // When the controller wants the Dstore to remove a particular file.
                case Protocol.LIST_TOKEN -> controllerList();                                                // When the controller is trying to get all the files the Dstore has before a rebalance.
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
        private static void clientStore(String filename, String filesize){}

        /**
         * Function which handles loading of a file from the particular Dstore.
         * @param filename The name of the file the client wants to load.
         */
        private static void clientLoadData(String filename){}

        /**
         * Function which handles removing of a file from the particular Dstore.
         * @param filename The name of the file the client wants to remove.
         */
        private static void clientRemove(String filename){}

        /**
         * Function which handles giving the controller all the files currently stored in are particular Dstore.
         */
        private static void controllerList(){}

        /**
         * Function which is used when the controller calls for a rebalance of the files stored in the distributed system.
         * @param message The unaltered orginal message so it can be read properly for future function.
         */
        private static void controllerRebalance(String message){}

        /**
         * Function which is used when another Dstore want to send this specific Dstore a file.
         * @param filename The name of the file the Dstore wants to send.
         * @param filesize The size of the file the Dstore wants to send
         */
        private static void dstoreRebalanceStore(String filename, String filesize){}
    }
}