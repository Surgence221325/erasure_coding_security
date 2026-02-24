package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.java.Log;

import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    public interface PBResult extends Result {}

    @Data
    public static final class InvalidView implements PBResult {
        @NonNull private final View view;
    }

    @Data
    public static final class BackupSuccess implements PBResult {}

    @Data
    public static final class SyncSuccess implements PBResult {}

    private AMOApplication<Application> app;
    private View currentView = new View(STARTUP_VIEWNUM, null, null);

    // Primary-side pending op while waiting for backup ack
    private AMOCommand pendingForwardCmd;
    private Address pendingClient;
    private AMOResult pendingPrimaryResult;

    // Block primary serving requests until backup catches up after view change
    private boolean syncingBackup = false;

    @SuppressWarnings("unchecked")
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;
        this.app = new AMOApplication<>(app);
    }

    @Override
    public void init() {
        send(new Ping(STARTUP_VIEWNUM), viewServer);
        set(new PingTimer(), PING_MILLIS);
    }

    private boolean amPrimary() {
        return currentView.primary() != null && currentView.primary().equals(address());
    }

    private boolean amBackup() {
        return currentView.backup() != null && currentView.backup().equals(address());
    }

    private void clearPending() {
        pendingForwardCmd = null;
        pendingClient = null;
        pendingPrimaryResult = null;
    }

    private void handleRequest(Request m, Address sender) {
        // Only current primary serves clients
        if (!amPrimary()) {
            send(new Reply(new InvalidView(currentView)), sender);
            return;
        }

        AMOCommand c = m.command();

        // If backup exists but is still syncing, don't serve yet.
        // Let client retry (do NOT send InvalidView since this server is still primary).
        if (currentView.backup() != null && syncingBackup) {
            return;
        }

        // No backup: execute and reply directly
        if (currentView.backup() == null) {
            AMOResult r = app.execute(c);
            send(new Reply(r), sender);
            return;
        }

        // One in-flight request at a time
        if (pendingForwardCmd != null) {
            return;
        }

        // Forward all ops (including reads) when backup exists, to avoid stale-primary reads
        pendingForwardCmd = c;
        pendingClient = sender;
        pendingPrimaryResult = app.execute(c); // AMO handles duplicates
        send(new ForwardRequest(c, currentView), currentView.backup());
    }

    private void handleViewReply(ViewReply m, Address sender) {
        View newView = m.view();
        if (newView == null) {
            return;
        }

        // Ignore stale/out-of-order replies
        if (newView.viewNum() < currentView.viewNum()) {
            return;
        }

        View oldView = currentView;
        boolean changed = !newView.equals(oldView);
        currentView = newView;

        if (!changed) {
            return;
        }

        if (amPrimary()) {
            Address oldBackup = oldView.backup();
            Address newBackup = newView.backup();

            boolean backupChanged =
                (oldBackup == null && newBackup != null) ||
                (oldBackup != null && !oldBackup.equals(newBackup));

            // Any in-flight request tied to previous view/backup is unsafe now
            if (backupChanged) {
                clearPending();
            }

            // If a backup was added/replaced, transfer full state and block serving until ack
            if (newBackup != null && (oldBackup == null || !newBackup.equals(oldBackup))) {
                syncingBackup = true;
                send(new StateTransfer(app, currentView), newBackup);
            } else if (newBackup == null) {
                // No backup -> no syncing needed
                syncingBackup = false;
            }
        } else {
            // If not primary anymore, clear primary-only state
            syncingBackup = false;
            clearPending();
        }
    }

    private void handleForwardRequest(ForwardRequest m, Address sender) {
        // Only the current backup should handle forwarded requests
        if (!amBackup()) {
            // If sender is an old primary, tell it its view is stale
            send(new ForwardReply(new InvalidView(currentView), null, currentView), sender);
            return;
        }

        // Only accept from current primary
        if (currentView.primary() == null || !sender.equals(currentView.primary())) {
            return;
        }

        // Must match current view exactly
        if (m.view() == null || !m.view().equals(currentView)) {
            send(new ForwardReply(new InvalidView(currentView), null, currentView), sender);
            return;
        }

        // If desired, you can block during backup sync; usually unnecessary because state transfer
        // completes before primary starts forwarding again. Leaving enabled is fine:
        // if (syncingBackup) { return; }

        AMOResult r = app.execute(m.command());
        send(new ForwardReply(new BackupSuccess(), r, currentView), sender);
    }

    private void handleForwardReply(ForwardReply m, Address sender) {
        // Only current primary processes backup replies
        if (!amPrimary()) {
            return;
        }

        // Must be from current backup
        if (currentView.backup() == null || !sender.equals(currentView.backup())) {
            return;
        }

        // Must have a pending forwarded op
        if (pendingForwardCmd == null || pendingClient == null || pendingPrimaryResult == null) {
            return;
        }

        // Ignore stale/mismatched view replies
        if (m.view() == null || !m.view().equals(currentView)) {
            return;
        }

        if (!(m.result() instanceof BackupSuccess)) {
            // Backup says sender/view is stale (or otherwise rejected)
            clearPending();
            if (currentView.backup() != null) {
                syncingBackup = true;
            }
            return;
        }

        // Success: reply to client and clear pending
        send(new Reply(pendingPrimaryResult), pendingClient);
        clearPending();
    }

    private void handleStateTransfer(StateTransfer m, Address sender) {
        // Only current backup accepts state transfer
        if (!amBackup()) {
            return;
        }

        // Must come from current primary
        if (currentView.primary() == null || !sender.equals(currentView.primary())) {
            return;
        }

        // Must match current view exactly
        if (m.view() == null || !m.view().equals(currentView)) {
            return;
        }

        @SuppressWarnings("unchecked")
        AMOApplication<Application> incoming = (AMOApplication<Application>) m.app();
        this.app = incoming;

        send(new StateTransferAck(currentView), sender);
    }

    private void handleStateTransferAck(StateTransferAck m, Address sender) {
        if (!amPrimary()) {
            return;
        }

        if (!syncingBackup) {
            return; // stale/superseded ack
        }

        if (currentView.backup() == null || !sender.equals(currentView.backup())) {
            return;
        }

        if (m.view() == null || !m.view().equals(currentView)) {
            return;
        }

        syncingBackup = false;
    }

    private void onPingTimer(PingTimer t) {
        send(new Ping(currentView.viewNum()), viewServer);
        send(new GetView(), viewServer);

        // Retry state transfer until ack arrives
        if (amPrimary() && syncingBackup && currentView.backup() != null) {
            send(new StateTransfer(app, currentView), currentView.backup());
        }

        // Retry in-flight forwarded request (important in unreliable runs)
        if (amPrimary()
                && !syncingBackup
                && currentView.backup() != null
                && pendingForwardCmd != null) {
            send(new ForwardRequest(pendingForwardCmd, currentView), currentView.backup());
        }

        set(t, PING_MILLIS);
    }
}