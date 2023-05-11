import java.util.ArrayList;
import java.util.HashMap;

public class Controller {

    /**
     * Port for the controller to listen to.
     */
    private static String controllerPort;

    /**
     * The number of Dstores which should be used to store a file.
     */
    private static String replicationFactor;

    /**
     * The time between process and the response its waiting for.
     */
    private static String timeoutMilliseconds;

    /**
     * The amount of seconds between rebalance periods.
     */
    private static String rebalancePeriod;

    /**
     * Contains all the current files in the system and the current operations they are going under.
     * HashMap paring goes as follows [FILE, CONTEXT].
     */
    private static HashMap<String,String> indexes;

    /**
     * Contains all the ports for the connected Dstore's and the files each Dstore has.
     * HashMap paring goes as follows [DSTORE_PORT, FILES].
     */
    private static HashMap<String,ArrayList<String>> dstores;

    /**
     * Main setup of the controller, setups up its main values then stats the programs main loop.
     * @param args Values which are used in setting up the controller.
     */
    public static void main(String[] args) {

        // Sets up the main values inputted from the command line.
        try {
            controllerPort = args[0];
            replicationFactor = args[1];
            timeoutMilliseconds = args[2];
            rebalancePeriod = args[3];
            indexes = new HashMap<String, String>();
            dstores = new HashMap<String, ArrayList<String>>();
        } catch (Exception exception) {
            System.err.println("Not all arguments inputted: " + exception);
            return;
        }

        // Loop waiting for a client to send a command or Dstore to join.
    }

    /**
     * Function which is used to parse the messages sent by a Client or Dstore.
     * @param message The message which is being sent by the Client or Dstore.
     * @param port The port that the Client or Dstore is connected on.
     */
    private static void messageParser(String message, String port) {
        // Splits the inputted message into an array.
        String messageArgs[] = message.split(" ");

        // Uses switch to check which message the port sent and run the required function.
        switch(messageArgs[0]) {
            case Protocol.STORE_TOKEN -> {clientStore(messageArgs[1], messageArgs[2], port); break;}  // When a client wants a files to be store in the system.
            case Protocol.LOAD_TOKEN -> {clientLoad(messageArgs[1], port); break;}                    // When a client wants to get a file from the system.
            case Protocol.RELOAD_TOKEN -> {clientReload(messageArgs[1], port); break;}                // Whem a client wants a file from the system but the given Dstore doesn't work.
            case Protocol.REMOVE_TOKEN -> {clientRemove(messageArgs[1], port); break;}                // When a client wants a file to be removed from the system.
            case Protocol.LIST_TOKEN -> {clientList(port); break;}                                    // When a client wants a list of all files in the system.
            case Protocol.JOIN_TOKEN -> {dstoreJoin(port); break;}                                    // When a Dstore joins the controller.
            default -> {System.err.println("Malformed message [" + messageArgs + "] recieved from [Port:" + port + "]."); break;} // Malformed message is recieved.
        }
    }

    /**
     * Function which handles storage of new files into the distributed system.
     * @param filename The name of the file the client wants to store.
     * @param filesize The size of the file the client wants to store.
     * @param port The port that the client is on.
     */
    private static void clientStore(String filename, String filesize, String port){}

    /**
     * Function which handles the loading of files from the distributed system.
     * @param filename The name of the file the client wants to load.
     * @param port The port that the client is on.
     */
    private static void clientLoad(String filename, String port){}

    /**
     * Function which handles loading the same file from the distributed system but with a different Dstore (as last failed).
     * @param filename The name of the file the client wants to load from a new Dstore.
     * @param port The port that the client is on.
     */
    private static void clientReload(String filename, String port){}

    /**
     * Function which handles the removal of a file from the distributed system.
     * @param filename The name of the file the client wants to remove.
     * @param port The port that the client is on.
     */
    private static void clientRemove(String filename, String port){}

    /**
     * Function which handles the listing of files in the distributed system.
     * @param port The port that the client is on.
     */
    private static void clientList(String port){}

    /**
     * Function which handles the joining of a new Dstore to the distributed system.
     * @param port The port that the Dstore is on.
     */
    private static void dstoreJoin(String port){}
}