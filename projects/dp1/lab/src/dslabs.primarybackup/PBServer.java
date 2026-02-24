package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.java.Log;

import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    public interface PBResult extends Result {
    }

    @Data
    public static final class InvalidView implements PBResult {
        @NonNull private final View view;
    }

    @Data
    public static final class BackupSuccess implements PBResult {
    }

    @Data
    public static final class SyncSuccess implements PBResult {
    }

    // TODO: declare fields for your implementation ...

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;

        // TODO: wrap app inside AMOApplication ...
    }

    @Override
    public void init() {
        // TODO: initialize fields ...
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        // TODO: handle client request ...
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // TODO: handle view reply from view server ...
    }

    // TODO: your message handlers ...


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingTimer(PingTimer t) {
        // TODO: on ping timeout ...
    }

    // TODO: your message handlers ...


    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // TODO: add utils here ...
}
