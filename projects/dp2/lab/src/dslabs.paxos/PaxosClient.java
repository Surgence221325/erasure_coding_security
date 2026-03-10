package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    private int nextSeq;
    private AMOCommand pendingCommand;
    private Result pendingResult;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {
        nextSeq = 1;
        pendingCommand = null;
        pendingResult = null;
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        if (pendingCommand != null) {
            throw new IllegalStateException("Client already has a pending command");
        }

        pendingResult = null;
        pendingCommand = new AMOCommand(operation, address(), nextSeq++);

        broadcastRequest();
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

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        if (pendingCommand == null) {
            return;
        }

        Result r = m.result();

        if (r instanceof AMOResult) {
            AMOResult amoResult = (AMOResult) r;

            if (amoResult.sequenceNum() != pendingCommand.sequenceNum()) {
                return;
            }

            pendingResult = amoResult.result();
        } else {
            // Defensive fallback in case PaxosReply ever carries a raw result
            pendingResult = r;
        }

        pendingCommand = null;
        notifyAll();
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        if (pendingCommand == null) {
            return;
        }

        if (t.sequenceNum() != pendingCommand.sequenceNum()) {
            return;
        }

        broadcastRequest();
        set(t, CLIENT_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    private void broadcastRequest() {
        if (pendingCommand == null) {
            return;
        }

        PaxosRequest request = new PaxosRequest(pendingCommand);
        for (Address server : servers) {
            send(request, server);
        }
    }
}