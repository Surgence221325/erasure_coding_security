package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    private View currentView;
    private int nextSeq;
    private AMOCommand pendingCommand;
    private Result pendingResult;

    public PBClient(Address address, Address viewServer) {
        super(address);
        this.viewServer = viewServer;
    }

    @Override
    public synchronized void init() {
        currentView = new View(STARTUP_VIEWNUM, null, null);
        nextSeq = 1;
        pendingCommand = null;
        pendingResult = null;

        send(new GetView(), viewServer);
    }

    @Override
    public synchronized void sendCommand(Command command) {
        if (pendingCommand != null) {
            throw new IllegalStateException("Client already has a pending command");
        }

        pendingResult = null;
        pendingCommand = new AMOCommand(command, address(), nextSeq++);
        sendToPrimaryOrAskView();
        set(new ClientTimer(pendingCommand.sequenceNum()), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        return pendingResult != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        while (pendingResult == null) {
            wait();
        }
        return pendingResult;
    }

    private synchronized void handleReply(Reply m, Address sender) {
        if (pendingCommand == null) {
            return;
        }

        Result r = m.result();

        // If we already know the primary, ignore replies from anyone else.
        // (This also avoids accepting InvalidView from random/stale servers.)
        if (currentView.primary() != null && !sender.equals(currentView.primary())) {
            return;
        }

        // InvalidView is not a final result; update view and retry.
        if (r instanceof PBServer.InvalidView) {
            PBServer.InvalidView iv = (PBServer.InvalidView) r;
            if (iv.view() != null && iv.view().viewNum() >= currentView.viewNum()) {
                currentView = iv.view();
            }
            sendToPrimaryOrAskView();
            return;
        }

        if (r instanceof AMOResult) {
            AMOResult ar = (AMOResult) r;
            if (ar.sequenceNum() != pendingCommand.sequenceNum()) {
                return;
            }
            pendingResult = ar.result();
        } else {
            pendingResult = r;
        }

        pendingCommand = null;
        notifyAll();
    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        if (m.view() == null) {
            return;
        }
        if (m.view().viewNum() < currentView.viewNum()) {
            return; // ignore stale view replies
        }

        currentView = m.view();

        if (pendingCommand != null) {
            sendToPrimaryOrAskView();
        }
    }

    private synchronized void sendToPrimaryOrAskView() {
        if (pendingCommand == null) {
            return;
        }

        if (currentView.primary() != null) {
            send(new Request(pendingCommand), currentView.primary());
        } else {
            send(new GetView(), viewServer);
        }
    }

    private synchronized void onClientTimer(ClientTimer t) {
        if (pendingCommand == null) {
            return;
        }
        if (pendingCommand.sequenceNum() != t.sequenceNum()) {
            return;
        }

        // Refresh view and retry current pending command
        send(new GetView(), viewServer);
        sendToPrimaryOrAskView();
        set(t, CLIENT_RETRY_MILLIS);
    }
}