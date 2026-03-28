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

    private final Address[] allCoordinators;  // all known coordinator addresses
    private Address coordinator;               // current coordinator (may change on redirect)
    private int     coordinatorIndex;          // index into allCoordinators for round-robin
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

    // Track consecutive auth failures to trigger coordinator rotation
    private int authFailureCount;
    private static final int AUTH_FAILURES_BEFORE_ROTATE = 3;

    /** Single-coordinator constructor (backward compatible). */
    public CapstoneClient(Address address, Address coordinator, byte[] sharedSecret) {
        this(address, new Address[]{coordinator}, sharedSecret);
    }

    /** Multi-coordinator constructor (for Raft). */
    public CapstoneClient(Address address, Address[] coordinators, byte[] sharedSecret) {
        super(address);
        this.allCoordinators = coordinators;
        this.coordinator     = coordinators[0];
        this.coordinatorIndex = 0;
        this.sharedSecret    = sharedSecret;
    }

    @Override
    public synchronized void init() {
        nextSeq        = 1;
        pendingSeq     = -1;
        pendingCommand = null;
        pendingResult  = null;
        authenticated    = false;
        sessionToken     = null;
        authFailureCount = 0;

        // Initiate authentication handshake
        AuthRetryTimer authTimer = new AuthRetryTimer();
        send(new AuthRequest(address().toString()), coordinator);
        set(authTimer, authTimer.backoffMillis());
    }

    /** Try the next coordinator in the list (round-robin). */
    private void rotateCoordinator() {
        coordinatorIndex = (coordinatorIndex + 1) % allCoordinators.length;
        coordinator = allCoordinators[coordinatorIndex];
        authenticated = false;
        sessionToken  = null;
        log("Rotating to coordinator " + coordinator);
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
        ClientRetryTimer retryTimer = new ClientRetryTimer(pendingSeq);
        set(retryTimer, retryTimer.backoffMillis());
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
        byte[] hmac = CryptoUtil.hmacSha256(sharedSecret, challenge.nonce());
        send(new AuthResponse(address().toString(), hmac), coordinator);
    }

    private synchronized void handleAuthResultMsg(AuthResultMsg result, Address sender) {
        if (authenticated) return;
        if (result.success()) {
            sessionToken  = result.sessionToken();
            authenticated = true;
            log("Authenticated, token=" + sessionToken);
            // Immediately re-send pending command with valid token (if any).
            // This ensures progress even without retry timers (e.g., in search tests).
            if (pendingCommand != null) {
                sendPending();
            }
            notifyAll();
        } else {
            log("Auth failed: " + result.error());
        }
    }

    private synchronized void onAuthRetryTimer(AuthRetryTimer t) {
        if (authenticated) return;

        // If auth keeps failing with current coordinator, try the next one
        authFailureCount++;
        if (authFailureCount >= AUTH_FAILURES_BEFORE_ROTATE && allCoordinators.length > 1) {
            rotateCoordinator();
            authFailureCount = 0;
        }

        log("Retrying auth (backoff=" + t.backoffMillis() + "ms) with " + coordinator);
        send(new AuthRequest(address().toString()), coordinator);
        AuthRetryTimer next = t.nextBackoff();
        set(next, next.backoffMillis());
    }

    // =========================================================================
    //  Raft redirect handler
    // =========================================================================

    private synchronized void handleNotLeaderResponse(NotLeaderResponse resp, Address sender) {
        if (resp.leaderAddress() != null && !resp.leaderAddress().equals(coordinator)) {
            log("Redirected to leader " + resp.leaderAddress());
            coordinator = resp.leaderAddress();
            // Need to re-authenticate with the new leader
            authenticated = false;
            sessionToken  = null;
            send(new AuthRequest(address().toString()), coordinator);
            AuthRetryTimer authTimer = new AuthRetryTimer();
            set(authTimer, authTimer.backoffMillis());
        }
        // Re-send pending command will happen via retry timer or auth completion
        if (pendingCommand != null && authenticated) {
            sendPending();
        }
    }

    // =========================================================================
    //  Command response handlers — called by framework (reflection dispatch)
    // =========================================================================

    private synchronized void handleWriteResponse(WriteResponse resp, Address sender) {
        if (pendingCommand == null || pendingSeq != resp.sequenceNum()) return;

        // Ignore transient errors — retry timer will re-send
        if ("AUTH_REQUIRED".equals(resp.error())) return;
        if ("INSUFFICIENT_REGIONS".equals(resp.error())) return;
        if ("RECONFIGURING".equals(resp.error())) return;
        if ("RAFT_COMMIT_TIMEOUT".equals(resp.error())) return;

        pendingResult  = new CapstoneWriteResult(resp.success(), resp.error());
        pendingCommand = null;
        notifyAll();
    }

    private synchronized void handleReadResponse(ReadResponse resp, Address sender) {
        if (pendingCommand == null || pendingSeq != resp.sequenceNum()) return;

        // Ignore transient errors — retry timer will re-send
        if ("AUTH_REQUIRED".equals(resp.error())) return;
        if ("INSUFFICIENT_REGIONS".equals(resp.error())) return;
        if ("RECONFIGURING".equals(resp.error())) return;

        pendingResult  = new CapstoneReadResult(resp.value(), resp.error());
        pendingCommand = null;
        notifyAll();
    }

    // =========================================================================
    //  Timer handlers
    // =========================================================================

    private synchronized void onClientRetryTimer(ClientRetryTimer t) {
        if (pendingCommand == null || t.sequenceNum() != pendingSeq) return;

        log("Retrying seq=" + pendingSeq + " (backoff=" + t.backoffMillis() + "ms)");
        sendPending();
        ClientRetryTimer next = t.nextBackoff();
        set(next, next.backoffMillis());
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

    private void log(String msg) {
        System.out.println("[" + address() + "] " + msg);
    }
}
