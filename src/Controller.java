import java.util.ArrayList;import java.util.HashMap;public class Controller {

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
    }
}