package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;
import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    //TODO: declare fields for your implementation ...

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PBClient(Address address, Address viewServer) {
        super(address);
        this.viewServer = viewServer;
    }

    @Override
    public synchronized void init() {
        // TODO: initialize fields ...
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        //TODO: send command to server ...
    }

    @Override
    public synchronized boolean hasResult() {
        //TODO: check whether there is result ...
        return false;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        //TODO: wait to get result ...
        return null;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleReply(Reply m, Address sender) {
        //TODO: check desired reply arrive ...
    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        //TODO: perform action when timer reach timeout ...
    }

    // TODO: add utils here ...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
    }
}
