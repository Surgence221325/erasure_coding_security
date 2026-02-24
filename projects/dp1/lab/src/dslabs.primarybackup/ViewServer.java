package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
    static final int STARTUP_VIEWNUM = 0;
    private static final int INITIAL_VIEWNUM = 1;

    // Your code here...
    private View currentView = new View(STARTUP_VIEWNUM, null, null);
    private boolean currentViewAcked = false;
    private final Set<Address> pingedThisInterval = new HashSet<>();
    private final Set<Address> knownServers = new LinkedHashSet<>();
    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ViewServer(Address address) {
        super(address);
    }

    @Override
    public void init() {
        set(new PingCheckTimer(), PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePing(Ping m, Address sender) {
        pingedThisInterval.add(sender);
        knownServers.add(sender);

        // Start the very first view
        if (currentView.viewNum() == STARTUP_VIEWNUM && currentView.primary() == null) {
            currentView = new View(INITIAL_VIEWNUM, sender, null);
            currentViewAcked = false; // wait for primary to ping(INITIAL_VIEWNUM)
            send(new ViewReply(currentView), sender);
            return;
        }

        // Primary ACKs current view by pinging with current view number
        if (sender.equals(currentView.primary()) && m.viewNum() == currentView.viewNum()) {
            currentViewAcked = true;
            if (currentView.backup() == null) {
        Address candidate = pickIdleServer(currentView.primary());
        if (candidate != null) {
            installNewView(currentView.primary(), candidate);
        }
    }
        }
        if (currentViewAcked
        && currentView.primary() != null
        && currentView.backup() == null
        && !sender.equals(currentView.primary())) {
        installNewView(currentView.primary(), sender);
        }

        send(new ViewReply(currentView), sender);
    }

    private void handleGetView(GetView m, Address sender) {
        send(new ViewReply(currentView), sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t) {
        Address primary = currentView.primary();
        Address backup = currentView.backup();

        boolean primaryAlive = primary != null && pingedThisInterval.contains(primary);
        boolean backupAlive = backup != null && pingedThisInterval.contains(backup);

        // Can only change views after current primary has ACKed the current view
        if (currentView.viewNum() != STARTUP_VIEWNUM && !currentViewAcked) {
            pingedThisInterval.clear();
            set(t, PING_CHECK_MILLIS);
            return;
        }

        // Case 1: primary dead -> promote backup (if any)
        if (primary != null && !primaryAlive) {
            if (backup != null) {
                Address newPrimary = backup;
                Address newBackup = pickIdleServer(newPrimary);
                installNewView(newPrimary, newBackup);
            }
            // else stuck by design (no backup to promote)
        }
        // Case 2: backup dead -> remove/replace backup
        else if (backup != null && !backupAlive) {
            Address newBackup = pickIdleServer(primary);
            installNewView(primary, newBackup); // may become null if none available
        }
        // Case 3: no backup and an idle server exists -> add backup
        else if (primary != null && backup == null) {
            Address newBackup = pickIdleServer(primary);
            if (newBackup != null) {
                installNewView(primary, newBackup);
            }
        }

        pingedThisInterval.clear();
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    private Address pickIdleServer(Address excludePrimary) {
        for (Address s : knownServers) {
            if (s.equals(excludePrimary)) {
                continue;
            }
            if (currentView.backup() != null && s.equals(currentView.backup())) {
                continue;
            }
            // only pick servers that are alive this interval
            if (!pingedThisInterval.contains(s)) {
                continue;
            }
            return s;
        }
        return null;
    }

    private void installNewView(Address primary, Address backup) {
        // Don't create an identical view
        if (Objects.equals(currentView.primary(), primary) &&
    Objects.equals(currentView.backup(), backup)) {
    return;
}

        int nextViewNum =
                (currentView.viewNum() == STARTUP_VIEWNUM) ? INITIAL_VIEWNUM
                        : currentView.viewNum() + 1;

        currentView = new View(nextViewNum, primary, backup);
        currentViewAcked = false;
    }
}
