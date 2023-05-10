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

        ////CLEAR THE FILE FOLDER HERE
    }
}