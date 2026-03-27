package dslabs.capstone;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * The client node in the capstone distributed KV store.
 *
 * Extends dslabs.framework.Node and implements dslabs.framework.Client —
 * the same pattern as PaxosClient in the Paxos lab.
 *
 * AUTHENTICATION
 * --------------
 * Before issuing any commands, the client performs a challenge-response
 * handshake with the coordinator:
 *   1. Client sends AuthRequest(clientId) in init().
 *   2. Coordinator replies with AuthChallenge(nonce).
 *   3. Client computes HMAC-SHA256(sharedSecret, nonce), sends AuthResponse.
 *   4. Coordinator verifies, returns AuthResultMsg with a session token.
 * sendCommand() blocks until the handshake completes — exactly like
 * getResult() blocks until the response arrives.
 *
 * All subsequent WriteRequest/ReadRequest include the session token.
 * The shared secret never travels over the wire.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class CapstoneClient extends Node implements Client {

    private final Address coordinator;
    private final byte[]  sharedSecret;

    // Sequence number for the next request
    private int nextSeq;

    // The currently pending request (null if none)
    private int     pendingSeq;
    private Command pendingCommand;

    // Set when the response arrives; cleared when sendCommand is called
    private Result pendingResult;

    // --- Authentication state ---
    private boolean authenticated;
    private String  sessionToken;

    public CapstoneClient(Address address, Address coordinator, byte[] sharedSecret) {
        super(address);
        this.coordinator  = coordinator;
        this.sharedSecret = sharedSecret;
    }

    @Override
    public synchronized void init() {
        nextSeq        = 1;
        pendingSeq     = -1;
        pendingCommand = null;
        pendingResult  = null;
        authenticated  = false;
        sessionToken   = null;

        // Initiate authentication handshake
        send(new AuthRequest(address().toString()), coordinator);
        set(new AuthRetryTimer(), AuthRetryTimer.AUTH_RETRY_MILLIS);
    }

    // =========================================================================
    //  Client interface — called by external thread
    // =========================================================================

    /**
     * Send a command to the coordinator.
     * If authentication hasn't completed yet, the request is sent with a null
     * session token.  The coordinator will reject it with AUTH_REQUIRED, which
     * the client ignores.  The ClientRetryTimer re-sends after auth completes.
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
    //  Authentication handlers
    // =========================================================================

    private synchronized void handleAuthChallenge(AuthChallenge challenge, Address sender) {
        if (authenticated) return;
        log("Received auth challenge, computing HMAC");
        byte[] hmac = hmacSha256(sharedSecret, challenge.nonce());
        send(new AuthResponse(address().toString(), hmac), coordinator);
    }

    private synchronized void handleAuthResultMsg(AuthResultMsg result, Address sender) {
        if (authenticated) return;
        if (result.success()) {
            sessionToken  = result.sessionToken();
            authenticated = true;
            log("Authenticated, token=" + sessionToken);
            notifyAll();  // wake up any blocked sendCommand
        } else {
            log("Auth failed: " + result.error());
        }
    }

    private synchronized void onAuthRetryTimer(AuthRetryTimer t) {
        if (authenticated) return;
        log("Retrying auth");
        send(new AuthRequest(address().toString()), coordinator);
        set(t, AuthRetryTimer.AUTH_RETRY_MILLIS);
    }

    // =========================================================================
    //  Command response handlers — called by framework (reflection dispatch)
    // =========================================================================

    private synchronized void handleWriteResponse(WriteResponse resp, Address sender) {
        if (pendingCommand == null || pendingSeq != resp.sequenceNum()) return;

        // Ignore AUTH_REQUIRED — retry timer will re-send after auth completes
        if ("AUTH_REQUIRED".equals(resp.error())) return;

        pendingResult  = new CapstoneWriteResult(resp.success(), resp.error());
        pendingCommand = null;
        notifyAll();
    }

    private synchronized void handleReadResponse(ReadResponse resp, Address sender) {
        if (pendingCommand == null || pendingSeq != resp.sequenceNum()) return;

        // Ignore AUTH_REQUIRED — retry timer will re-send after auth completes
        if ("AUTH_REQUIRED".equals(resp.error())) return;

        pendingResult  = new CapstoneReadResult(resp.value(), resp.error());
        pendingCommand = null;
        notifyAll();
    }

    // =========================================================================
    //  Timer handlers
    // =========================================================================

    private synchronized void onClientRetryTimer(ClientRetryTimer t) {
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
            send(new WriteRequest(address().toString(), pendingSeq,
                    w.key(), w.value(), sessionToken), coordinator);
        } else if (pendingCommand instanceof CapstoneRead) {
            CapstoneRead r = (CapstoneRead) pendingCommand;
            send(new ReadRequest(address().toString(), pendingSeq,
                    r.key(), sessionToken), coordinator);
        } else {
            throw new IllegalArgumentException("Unknown command type: " + pendingCommand);
        }
    }

    private static byte[] hmacSha256(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) { throw new RuntimeException("HMAC failed", e); }
    }

    private void log(String msg) {
        System.out.println("[" + address() + "] " + msg);
    }
}
