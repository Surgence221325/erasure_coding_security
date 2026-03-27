package dslabs.capstone;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * The client node in the capstone distributed KV store.
 *
 * Extends dslabs.framework.Node and implements dslabs.framework.Client —
 * the same pattern as PaxosClient in the Paxos lab.
 *
 * The Client interface is synchronization-aware: sendCommand / hasResult /
 * getResult are called from an external thread (the test harness or demo),
 * while handleFoo handlers are called by the framework.  All public methods
 * are synchronized.  getResult blocks using wait/notifyAll — exactly as
 * PaxosClient does.
 *
 * Command types accepted:
 *   CapstoneWrite (key, value)  → sends WriteRequest to coordinator
 *   CapstoneRead  (key)         → sends ReadRequest to coordinator
 *
 * Result types:
 *   CapstoneWriteResult (success, error)
 *   CapstoneReadResult  (value, error)
 *
 * A ClientRetryTimer re-broadcasts the pending request if no response arrives
 * within CLIENT_RETRY_MILLIS.  This handles message loss in a real network.
 * In the in-memory demo the timer fires only if we explicitly trigger it.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class CapstoneClient extends Node implements Client {

    private final Address coordinator;

    // Sequence number for the next request
    private int nextSeq;

    // The currently pending request (null if none)
    private int     pendingSeq;
    private Command pendingCommand;

    // Set when the response arrives; cleared when sendCommand is called
    private Result pendingResult;

    public CapstoneClient(Address address, Address coordinator) {
        super(address);
        this.coordinator = coordinator;
    }

    @Override
    public synchronized void init() {
        nextSeq        = 1;
        pendingSeq     = -1;
        pendingCommand = null;
        pendingResult  = null;
    }

    // =========================================================================
    //  Client interface — called by external thread
    // =========================================================================

    /**
     * Send a command to the coordinator.
     * Wraps the command in a WriteRequest or ReadRequest with the current
     * sequence number, then sets a retry timer.
     */
    @Override
    public synchronized void sendCommand(Command command) {
        if (pendingCommand != null)
            throw new IllegalStateException("Client already has a pending command");

        pendingResult  = null;
        pendingCommand = command;
        pendingSeq     = nextSeq++;

        sendPending();
        set(new ClientRetryTimer(pendingSeq), ClientRetryTimer.CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        return pendingResult != null;
    }

    /**
     * Block until the response arrives.  Releases the monitor while waiting
     * so that message handlers can run (same pattern as PaxosClient).
     */
    @Override
    public synchronized Result getResult() throws InterruptedException {
        while (pendingResult == null) wait();
        return pendingResult;
    }

    // =========================================================================
    //  Message handlers — called by framework (reflection dispatch)
    // =========================================================================

    private synchronized void handleWriteResponse(WriteResponse resp, Address sender) {
        if (pendingCommand == null || pendingSeq != resp.sequenceNum()) return;

        pendingResult  = new CapstoneWriteResult(resp.success(), resp.error());
        pendingCommand = null;
        notifyAll();
    }

    private synchronized void handleReadResponse(ReadResponse resp, Address sender) {
        if (pendingCommand == null || pendingSeq != resp.sequenceNum()) return;

        pendingResult  = new CapstoneReadResult(resp.value(), resp.error());
        pendingCommand = null;
        notifyAll();
    }

    // =========================================================================
    //  Timer handlers
    // =========================================================================

    private synchronized void onClientRetryTimer(ClientRetryTimer t) {
        // Ignore stale timer fires from previous requests
        if (pendingCommand == null || t.sequenceNum() != pendingSeq) return;

        log("Retrying seq=" + pendingSeq);
        sendPending();
        set(t, ClientRetryTimer.CLIENT_RETRY_MILLIS);
    }

    // =========================================================================
    //  Helpers
    // =========================================================================

    /** Translate the pending Command into the appropriate protocol message. */
    private void sendPending() {
        if (pendingCommand instanceof CapstoneWrite) {
            CapstoneWrite w = (CapstoneWrite) pendingCommand;
            send(new WriteRequest(address().toString(), pendingSeq, w.key(), w.value()), coordinator);
        } else if (pendingCommand instanceof CapstoneRead) {
            CapstoneRead r = (CapstoneRead) pendingCommand;
            send(new ReadRequest(address().toString(), pendingSeq, r.key()), coordinator);
        } else {
            throw new IllegalArgumentException("Unknown command type: " + pendingCommand);
        }
    }

    /** Returns the sequence number used by the most recently sent command. */
    public synchronized int getLastUsedSeq() {
        return nextSeq - 1;
    }

    private void log(String msg) {
        System.out.println("[" + address() + "] " + msg);
    }
}
