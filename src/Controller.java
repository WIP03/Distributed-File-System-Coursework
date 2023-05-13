import java.io.BufferedReader;import java.io.IOException;import java.io.InputStreamReader;import java.io.PrintWriter;import java.net.ServerSocket;import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

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
     * Contains all the current files in the system and the current operations they are going under.
     * HashMap paring goes as follows [FILE, CONTEXT].
     */
    private static HashMap<String,String> indexes;

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
            controllerPort = Integer.getInteger(args[0]);
            replicationFactor = Integer.getInteger(args[1]);
            timeoutMilliseconds = Integer.getInteger(args[2]);
            rebalancePeriod = Integer.getInteger(args[3]);
            indexes = new HashMap<String, String>();
            dstores = new HashMap<Integer, ArrayList<String>>();
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
        catch (IOException exception){
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
                case Protocol.STORE_TOKEN -> clientStore(messageArgs[1], messageArgs[2]);  // When a client wants a files to be store in the system.
                case Protocol.LOAD_TOKEN -> clientLoad(messageArgs[1]);                    // When a client wants to get a file from the system.
                case Protocol.RELOAD_TOKEN -> clientReload(messageArgs[1]);                // Whem a client wants a file from the system but the given Dstore doesn't work.
                case Protocol.REMOVE_TOKEN -> clientRemove(messageArgs[1]);                // When a client wants a file to be removed from the system.
                case Protocol.LIST_TOKEN -> clientList();                                  // When a client wants a list of all files in the system.
                case Protocol.JOIN_TOKEN -> dstoreJoin(messageArgs[1]);                                  // When a Dstore joins the controller.
                //ADD ACK HERE
                default -> System.err.println("Error: malformed message [" + messageArgs + "] recieved from [Port:" + connectedSocket.getPort() + "]."); // Malformed message is recieved.
            }
        }

        /**
         * Function which handles storage of new files into the distributed system.
         * @param filename The name of the file the client wants to store.
         * @param filesize The size of the file the client wants to store.
         */
        private void clientStore(String filename, String filesize){}

        /**
         * Function which handles the loading of files from the distributed system.
         * @param filename The name of the file the client wants to load.
         */
        private void clientLoad(String filename){}

        /**
         * Function which handles loading the same file from the distributed system but with a different Dstore (as last failed).
         * @param filename The name of the file the client wants to load from a new Dstore.
         */
        private void clientReload(String filename){}

        /**
         * Function which handles the removal of a file from the distributed system.
         * @param filename The name of the file the client wants to remove.
         */
        private void clientRemove(String filename){}

        /**
         * Function which handles the listing of files in the distributed system.
         */
        private void clientList(){}

        /**
         * Function which handles the joining of a new Dstore to the distributed system.
         */
        private void dstoreJoin(String port) {
            // Lets the Thread know that it is a Dstore for later use.
            isDstore = true;

            // Adds it to the HashMap of Dstores ready to be updated when files are added.
            dstores.put(Integer.getInteger(port), new ArrayList<String>());

            // Rebalances the storage system as a new Dstore has joined.
            storageRebalanceOperation();
        }
    }
}