public class Dstore {

    /**
     * Port for the Dstore to listen to.
     */
    private static String dstorePort;

    /**
     * The current port the controller is on to talk to it.
     */
    private static String controllerPort;

    /**
     * The time between process and the response its waiting for.
     */
    private static String timeoutMilliseconds;

    /**
     * Where data should be stored locally.
     */
    private static String fileFolder;

    /**
     * Main setup of the Dstore, setups up its main values then stats the programs main loop.
     * @param args Values which are used in setting up the Dstore.
     */
    public static void main(String[] args) {

        // Sets up the main values inputted from the command line.
        try {
            dstorePort = args[0];
            controllerPort = args[1];
            timeoutMilliseconds = args[2];
            fileFolder = args[3];
        } catch (Exception exception) {
            System.err.println("Not all arguments inputted: " + exception);
            return;
        }

        // If succesful in generating the values for the Dstore then all its old data is removed.
        clearFileFolder();

        // JOIN THE CONTROLLER
        // LOOP AROUND WAITING FOR MESSAGES ON ITS PORT, IF ONE IS RECIEVED AND NOT FROM A PORT BEING USED (APART FROM CONTROLLER) THEN PARSE ITS MESSAGE
    }

    /**
     * Clears the file folder removing all of its current contents.
     */
    private static void clearFileFolder(){} //FINNISH ME

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
            case Protocol.STORE_TOKEN -> {clientStore(messageArgs[1], messageArgs[2], port); break;}                        // When the client wants to store a file at the particular Dstore.
            case Protocol.LOAD_DATA_TOKEN -> {clientLoadData(messageArgs[1], port); break;}                                 // When the client wants particular data from the Dstore.
            case Protocol.REMOVE_TOKEN -> {clientRemove(messageArgs[1], port); break;}                                      // When the controller wants the Dstore to remove a particular file.
            case Protocol.LIST_TOKEN -> {controllerList(port); break;}                                                      // When the controller is trying to get all the files the Dstore has before a rebalance.
            case Protocol.REBALANCE_TOKEN -> {controllerRebalance(message, port); break;}                                   // When the Dstore is to be changed by sending file to other Dstores and removing its own files.
            case Protocol.REBALANCE_STORE_TOKEN -> {dstoreRebalanceStore(messageArgs[1], messageArgs[2], port); break;}     // When another Dstore is sending a file to the current Dstore.
            default -> {System.err.println("Malformed message [" + messageArgs + "] recieved from [Port:" + port + "]."); break;} // Malformed message is recieved.
        }
    }

    /**
     * Function which handles storage of new files into the particular Dstore.
     * @param filename The name of the file the client wants to store.
     * @param filesize The size of the file the client wants to store.
     * @param port The port that the client is on.
     */
    private static void clientStore(String filename, String filesize, String port){}

    /**
     * Function which handles loading of a file from the particular Dstore.
     * @param filename The name of the file the client wants to load.
     * @param port The port that the client is on.
     */
    private static void clientLoadData(String filename, String port){}

    /**
     * Function which handles removing of a file from the particular Dstore.
     * @param filename The name of the file the client wants to remove.
     * @param port The port that the client is on.
     */
    private static void clientRemove(String filename, String port){}

    /**
     * Function which handles giving the controller all the files currently stored in are particular Dstore.
     * @param port The port that the controller is connected on.
     */
    private static void controllerList(String port){}

    /**
     * Function which is used when the controller calls for a rebalance of the files stored in the distributed system.
     * @param message The unaltered orginal message so it can be read properly for future function.
     * @param port The port that the controller is connected on.
     */
    private static void controllerRebalance(String message, String port){}

    /**
     * Function which is used when another Dstore want to send this specific Dstore a file.
     * @param filename The name of the file the Dstore wants to send.
     * @param filesize The size of the file the Dstore wants to send
     * @param port The port that the other Dstore is connected on.
     */
    private static void dstoreRebalanceStore(String filename, String filesize, String port){}
}