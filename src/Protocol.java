/**
 * Contains all protocols for the system.
 */
public abstract class Protocol {

    //CLient protocol messages
    public final static String LIST_TOKEN = "LIST"; // Also for Controller and DStore.
    public final static String STORE_TOKEN = "STORE"; // Also for DStore.
    public final static String LOAD_TOKEN = "LOAD";
    public final static String LOAD_DATA_TOKEN = "LOAD_DATA";
    public final static String RELOAD_TOKEN = "RELOAD";
    public final static String REMOVE_TOKEN = "REMOVE"; // Also for Controller.

    //Controller protocol messages
    public final static String STORE_TO_TOKEN = "STORE_TO";
    public final static String STORE_COMPLETE_TOKEN = "STORE_COMPLETE";
    public final static String LOAD_FROM_TOKEN = "LOAD_FROM";
    public final static String REMOVE_COMPLETE_TOKEN = "REMOVE_COMPLETE";
    public final static String REBALANCE_TOKEN = "REBALANCE";
    public final static String ERROR_FILE_DOES_NOT_EXISTS_TOKEN = "ERROR_FILE_DOES_NOT_EXIST"; // Also for DStore.
    public final static String ERROR_FILE_ALREADY_EXISTS_TOKEN = "ERROR_FILE_ALREADY_EXISTS";
    public final static String ERROR_NOT_ENOUGH_DSTORES_TOKEN = "ERROR_NOT_ENOUGH_DSTORES";
    public final static String ERROR_LOAD_TOKEN = "ERROR_LOAD";

    //DStore protocol messages
    public final static String ACK_TOKEN = "ACK";
    public final static String STORE_ACK_TOKEN = "STORE_ACK";
    public final static String REMOVE_ACK_TOKEN = "REMOVE_ACK";
    public final static String JOIN_TOKEN = "JOIN";
    public final static String REBALANCE_STORE_TOKEN = "REBALANCE_STORE";
    public final static String REBALANCE_COMPLETE_TOKEN = "REBALANCE_COMPLETE";
}