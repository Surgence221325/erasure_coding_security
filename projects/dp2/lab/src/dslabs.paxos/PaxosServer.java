package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Message;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import java.io.Serializable;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;

    // app
    private final AMOApplication<Application> app;

    // Strictly for convenience, technically not needed as we could proceed just off ballot numbers.
    private Address knownLeader;
    // rest of information about leader/Ballot State
    private Ballot myBallot;
    private boolean isActive;

    //Log information
    private final Map<Integer, LogEntry> log = new HashMap<>();
    private int slotIn;
    private int slotOut;

    // leader Proposal Bookkeeping, ie. clientRequests for a particular slot
    private final Map<Integer, PaxosRequest> proposals = new HashMap<>();

    // phase 1 ie. We first attempt to get majority responses (p1bResponds), then we maintain the highest non-gc
    // collected proposals for each slot
    private final Set<Address> p1bResponders = new HashSet<>();
    private final Map<Integer, Pvalue> adopted = new HashMap<>();

    // phase 2 ie. central work, leader proposes for a particular slot and when it gets a majority it
    // marks that slot as decided
    private final Map<Integer, Phase2State> p2bResponders = new HashMap<>();

    //catchup/gc related fields
    // when we reach two, this server should propose
    private int missedHeartbeats;
    //for leader, on downtime it will periodically send out the chosen slot each follower needs to catch up
    private final Map<Address, Integer> followerSlotOut = new HashMap<>();
    // last executed slot for this server
    private int firstNonCleared;
    // last non-empty slot for this server
    private int lastNonEmpty;


    private static final class LogEntry implements Serializable {
        private static final long serialVersionUID = 1L;

        Ballot acceptedBallot;
        PaxosRequest acceptedValue;
        boolean chosen;
        PaxosRequest chosenValue;
    }

    public static final class NoOp implements Command, Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static final class Phase2State implements Serializable {
        int slot;
        Ballot ballot;
        PaxosRequest value;
        Set<Address> responders = new HashSet<>();
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;
        this.app = new AMOApplication<>(app);
    }


    @Override
    public void init() {
        this.knownLeader = null;
        this.myBallot = new Ballot(0, address());
        this.isActive = false;

        this.slotIn = 1;
        this.slotOut = 1;
        this.firstNonCleared = 1;
        this.lastNonEmpty = 0;

        this.missedHeartbeats = 0;

        this.p1bResponders.clear();
        this.adopted.clear();
        this.proposals.clear();
        this.p2bResponders.clear();
        this.followerSlotOut.clear();
        this.log.clear();

        for (Address s : servers) {
            followerSlotOut.put(s, 1);
        }
        set(new HeartbeatCheckTimer(), HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the server's local log.
     *
     * If this server has garbage-collected this slot, it should return {@link
     * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen
     * command for this slot. If this server has both accepted and chosen a
     * command for this slot, it should return {@link PaxosLogSlotStatus#CHOSEN}.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     *
     * @see PaxosLogSlotStatus
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        if (logSlotNum < firstNonCleared) {
            return PaxosLogSlotStatus.CLEARED;
        }

        LogEntry entry = log.get(logSlotNum);
        if (entry == null) {
            return PaxosLogSlotStatus.EMPTY;
        }
        if (entry.chosen) {
            return PaxosLogSlotStatus.CHOSEN;
        }
        if (entry.acceptedValue != null) {
            return PaxosLogSlotStatus.ACCEPTED;
        }
        return PaxosLogSlotStatus.EMPTY;
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log.
     *
     * If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}.
     * Otherwise, return the command this server has chosen or accepted,
     * according to {@link PaxosServer#status}.
     *
     * If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     *
     * @see PaxosLogSlotStatus
     */
    public Command command(int logSlotNum) {
        PaxosLogSlotStatus s = status(logSlotNum);
        if (s == PaxosLogSlotStatus.CLEARED || s == PaxosLogSlotStatus.EMPTY) {
            return null;
        }

        LogEntry entry = log.get(logSlotNum);
        PaxosRequest req = entry.chosen ? entry.chosenValue : entry.acceptedValue;

        if (req == null || isNoOp(req)) {
            return null;
        }

        AMOCommand amo = req.command();
        return amo.command();
    }

    /**
     * Return the index of the first non-cleared slot in the server's local log.
     * The first non-cleared slot is the first slot which has not yet been
     * garbage-collected. By default, the first non-cleared slot is 1.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int firstNonCleared() {
        return this.firstNonCleared;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log,
     * according to the defined states in {@link PaxosLogSlotStatus}. If there
     * are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int lastNonEmpty() {
        return lastNonEmpty;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/

    // Phase 1/Election:
    private void handleP1A(P1A m, Address sender) {
        // we receive the bid from the sender
        Ballot incoming = m.ballot();

        // compare to ours
        if (incoming.compareTo(myBallot) > 0) {
            myBallot = incoming;
            isActive = false;
            knownLeader = sender;
        }

        // send our response
            // either positive to add a vote
            // negative to update the ballot of the server rejected
        send(new P1B(myBallot, collectAcceptedPvalues()), sender);
    }

    private void handleP1B(P1B m, Address sender) {
        Ballot incoming = m.ballot();

        // stale reply for an old phase 1 we no longer care about
        if (incoming.compareTo(myBallot) < 0) {
            return;
        }

        // we have been preempted by a higher ballot
        if (incoming.compareTo(myBallot) > 0) {
            myBallot = incoming;
            isActive = false;
            knownLeader = sender;
            p1bResponders.clear();
            adopted.clear();
            return;
        }

        // only meaningful while trying to become leader
        if (isActive) {
            return;
        }

        p1bResponders.add(sender);

        // we are trying to create a new log containing all non-gc proposed entries from different logs
        // essentially we maintain the highest proposed ballot for a particular slot
        for (Map.Entry<Integer, Pvalue> e : m.accepted().entrySet()) {
            int slot = e.getKey();
            Pvalue newPvalue = e.getValue();
            Pvalue current = adopted.get(slot);

            if (current == null
                || newPvalue.ballot().compareTo(current.ballot()) > 0) {
                adopted.put(slot, newPvalue);
            }
        }

        if (p1bResponders.size() < majority()) {
            return;
        }

        becomeLeaderAfterPhase1();
    }

    private void startPhase1() {
        isActive = false;
        // knownLeader = null;

        myBallot = new Ballot(myBallot.sequenceNum() + 1, address());

        p1bResponders.clear();
        p2bResponders.clear();
        adopted.clear();
        proposals.clear();

        // count self immediately
        p1bResponders.add(address());

        Map<Integer, Pvalue> selfAccepted = collectAcceptedPvalues();
        for (Map.Entry<Integer, Pvalue> e : selfAccepted.entrySet()) {
            adopted.put(e.getKey(), e.getValue());
        }

        // single-node quorum (or self already enough)
        if (p1bResponders.size() >= majority()) {
            becomeLeaderAfterPhase1();
            return;
        }

        broadcast(new P1A(myBallot));
    }

    // Phase 2/Proposal

    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        if (isActive) {
            Integer existingSlot = existingProposalSlot(m);
            if (existingSlot != null) {
                LogEntry entry = log.get(existingSlot);

                if (entry != null && entry.chosen) {
                    return;
                }

                Phase2State st = p2bResponders.get(existingSlot);
                if (st != null && st.ballot.equals(myBallot)) {
                    resendP2A(existingSlot);
                    return;
                }

                proposals.remove(existingSlot);
            }

            int slot = nextProposalSlot();
            proposals.put(slot, m);
            slotIn = Math.max(slotIn, slot + 1);
            sendP2A(slot, m);
            return;
        }

        if (knownLeader != null && !knownLeader.equals(address())) {
            send(m, knownLeader);
        }
    }

    private void handleP2A(P2A m, Address sender) {
        Ballot incoming = m.ballot();
        int slot = m.slotNum();

        if (slot < firstNonCleared) {
            send(new P2B(myBallot, slot, false), sender);
            return;
        }

        if (incoming.compareTo(myBallot) < 0) {
            send(new P2B(myBallot, slot, false), sender);
            return;
        }

        if (incoming.compareTo(myBallot) > 0) {
            myBallot = incoming;
            isActive = false;
            knownLeader = sender;
            p1bResponders.clear();
            adopted.clear();
            p2bResponders.clear();
            proposals.clear();
        }

        LogEntry entry = ensureLogEntry(slot);

        // already chosen: only ack if same chosen value
        if (entry.chosen) {
            boolean sameChosen = sameRequest(entry.chosenValue, m.value());
            send(new P2B(myBallot, slot, sameChosen), sender);
            return;
        }

        // do not allow same ballot + same slot to switch to a different value
        if (entry.acceptedBallot != null
            && entry.acceptedBallot.equals(incoming)
            && entry.acceptedValue != null
            && !sameRequest(entry.acceptedValue, m.value())) {
            send(new P2B(myBallot, slot, false), sender);
            return;
        }

        entry.acceptedBallot = incoming;
        entry.acceptedValue = m.value();
        lastNonEmpty = Math.max(lastNonEmpty, slot);

        send(new P2B(myBallot, slot, true), sender);
    }

    private void handleP2B(P2B m, Address sender) {
        Ballot incoming = m.ballot();
        int slot = m.slotNum();

        if (incoming.compareTo(myBallot) < 0) {
            return;
        }

        if (incoming.compareTo(myBallot) > 0) {
            myBallot = incoming;
            isActive = false;
            knownLeader = sender;
            p1bResponders.clear();
            adopted.clear();
            p2bResponders.clear();
            return;
        }

        if (!isActive || !m.accepted()) {
            return;
        }

        Phase2State st = p2bResponders.get(slot);
        if (st == null) {
            return;
        }

        // only count replies for the exact current in-flight proposal
        if (!st.ballot.equals(myBallot)) {
            return;
        }

        LogEntry entry = log.get(slot);
        if (entry == null || entry.chosen) {
            return;
        }

        // make sure the slot still corresponds to the same value
        if (!sameRequest(st.value, entry.acceptedValue)) {
            return;
        }

        st.responders.add(sender);

        if (st.responders.size() < majority()) {
            return;
        }

        chooseSlotLocally(slot, st.value);
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        if (!isActive) {
            missedHeartbeats++;
            if (missedHeartbeats >= 2) {
                startPhase1();
                missedHeartbeats = 0;
            }
        }

        set(t, HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS);
    }

    private void onHeartbeatTimer(HeartbeatTimer t) {
        if (isActive) {
            followerSlotOut.put(address(), slotOut);

            for (Map.Entry<Integer, PaxosRequest> e : proposals.entrySet()) {
                int slot = e.getKey();
                PaxosRequest value = e.getValue();

                if (slot < firstNonCleared) {
                    continue;
                }

                LogEntry entry = log.get(slot);
                if (entry != null && entry.chosen) {
                    continue;
                }

                Phase2State st = p2bResponders.get(slot);
                if (st == null) {
                    continue;
                }

                // Only retry if this exact proposal is still the active one.
                if (!st.ballot.equals(myBallot) || !sameRequest(st.value, value)) {
                    continue;
                }

                broadcast(new P2A(myBallot, slot, value));
            }

            broadcast(new Heartbeat(myBallot, firstNonCleared, slotOut));
            set(t, HeartbeatTimer.HEARTBEAT_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private int majority() {
        return (servers.length / 2) + 1;
    }

    private void broadcast(Message m) {
        for (Address server : servers) {
            if (!server.equals(address())) {
                send(m, server);
            }
        }
    }

    private Map<Integer, Pvalue> collectAcceptedPvalues() {
        Map<Integer, Pvalue> accepted = new HashMap<>();

        for (Map.Entry<Integer, LogEntry> e : log.entrySet()) {
            int slot = e.getKey();
            LogEntry entry = e.getValue();

            if (slot < firstNonCleared) {
                continue;
            }

            if (entry.acceptedBallot != null && entry.acceptedValue != null) {
                accepted.put(slot, new Pvalue(slot, entry.acceptedBallot, entry.acceptedValue));
            }
        }

        return accepted;
    }

    private LogEntry ensureLogEntry(int slot) {
        log.putIfAbsent(slot, new LogEntry());
        return log.get(slot);
    }

    private void becomeLeaderAfterPhase1() {
        isActive = true;
        knownLeader = address();
        missedHeartbeats = 0;
        followerSlotOut.put(address(), slotOut);
        p2bResponders.clear();

        int maxSeen = slotOut - 1;
        for (int slot : adopted.keySet()) {
            if (slot >= firstNonCleared) {
                maxSeen = Math.max(maxSeen, slot);
            }
        }

        for (int slot = slotOut; slot <= maxSeen; slot++) {
            PaxosRequest value;
            Pvalue pv = adopted.get(slot);

            if (pv != null) {
                value = pv.paxosRequest();
            } else {
                value = makeNoOpRequest();
            }

            proposals.put(slot, value);
            sendP2A(slot, value);
        }

        slotIn = Math.max(slotIn, maxSeen + 1);
        set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_MILLIS);
    }

    private void chooseSlotLocally(int slot, PaxosRequest value) {
        LogEntry entry = ensureLogEntry(slot);

        if (entry.chosen) {
            return;
        }

        entry.chosen = true;
        entry.chosenValue = value;
        entry.acceptedBallot = myBallot;
        entry.acceptedValue = value;
        lastNonEmpty = Math.max(lastNonEmpty, slot);

        Decision d = new Decision(slot, value);
        broadcast(d);

        // clear proposal bookkeeping now that it's decided
        proposals.remove(slot);
        p2bResponders.remove(slot);

        handleDecision(d, address());
    }

    private void sendP2A(int slot, PaxosRequest value) {
        LogEntry entry = ensureLogEntry(slot);

        if (entry.chosen) {
            return;
        }

        proposals.put(slot, value);

        entry.acceptedBallot = myBallot;
        entry.acceptedValue = value;
        lastNonEmpty = Math.max(lastNonEmpty, slot);

        // fresh phase-2 state for this exact proposal
        Phase2State st = new Phase2State();
        st.slot = slot;
        st.ballot = myBallot;
        st.value = value;
        st.responders.add(address()); // self vote
        p2bResponders.put(slot, st);

        if (st.responders.size() >= majority()) {
            chooseSlotLocally(slot, value);
            return;
        }

        broadcast(new P2A(myBallot, slot, value));
    }

    private void handleDecision(Decision m, Address sender) {
        int slot = m.slotNum();

        if (slot < firstNonCleared) {
            return;
        }

        LogEntry entry = ensureLogEntry(slot);

        // Never allow a chosen slot to be overwritten with a different value.
        if (entry.chosen) {
            if (!sameRequest(entry.chosenValue, m.value())) {
                return;
            }
            executeChosen();
            return;
        }

        // If we had previously accepted something different locally, do not
        // overwrite accepted state blindly; chosen value is now authoritative.
        entry.chosen = true;
        entry.chosenValue = m.value();
        entry.acceptedValue = m.value();
        lastNonEmpty = Math.max(lastNonEmpty, slot);

        // This slot is no longer "in flight" from the leader's perspective.
        proposals.remove(slot);
        p2bResponders.remove(slot);

        executeChosen();
    }

    private void handleHeartbeat(Heartbeat m, Address sender) {
        Ballot incoming = m.ballot();

        if (incoming.compareTo(myBallot) < 0) {
            return;
        }

        if (incoming.compareTo(myBallot) > 0) {
            p1bResponders.clear();
            adopted.clear();
            p2bResponders.clear();
            myBallot = incoming;
        }

        isActive = false;
        knownLeader = sender;
        missedHeartbeats = 0;

        if (m.firstNonCleared() > firstNonCleared) {
            applyGc(m.firstNonCleared());
        }

        send(new HeartbeatReply(myBallot, slotOut), sender);
    }

    private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
        if (!isActive) {
            return;
        }

        if (m.ballot().compareTo(myBallot) > 0) {
            myBallot = m.ballot();
            isActive = false;
            knownLeader = sender;
            p1bResponders.clear();
            adopted.clear();
            p2bResponders.clear();
            return;
        }

        if (!m.ballot().equals(myBallot)) {
            return;
        }

        followerSlotOut.put(sender, m.slotOut());

        // Keep leader's own executed frontier up to date too.
        followerSlotOut.put(address(), slotOut);

        sendMissingChosenSlots(sender, m.slotOut());

        // Since slotOut is the next slot to execute, the new firstNonCleared
        // should be min(all slotOuts).
        int newFirstNonCleared = slotOut;
        for (Address s : servers) {
            newFirstNonCleared = Math.min(
                newFirstNonCleared,
                followerSlotOut.getOrDefault(s, 1)
            );
        }

        applyGc(newFirstNonCleared);
    }

    private void sendMissingChosenSlots(Address follower, int followerNextSlot) {
        for (int slot = followerNextSlot; slot < slotOut; slot++) {
            LogEntry entry = log.get(slot);
            if (entry != null && entry.chosen && entry.chosenValue != null) {
                send(new Decision(slot, entry.chosenValue), follower);
            }
        }
    }

    private void executeChosen() {
        while (true) {
            LogEntry entry = log.get(slotOut);
            if (entry == null || !entry.chosen || entry.chosenValue == null) {
                return;
            }

            PaxosRequest req = entry.chosenValue;

            // No-op fills holes in the log but must not be executed on KVStore
            // or replied to as a client command.
            if (!isNoOp(req)) {
                AMOCommand amoCommand = req.command();
                AMOResult result = app.execute(amoCommand);
                send(new PaxosReply(result), amoCommand.clientAddress());
            }

            slotOut++;
        }
    }

    private int nextProposalSlot() {
        int s = firstNonCleared;

        while (true) {
            LogEntry entry = log.get(s);

            boolean hasAccepted = entry != null && entry.acceptedValue != null;
            boolean isChosen = entry != null && entry.chosen;
            boolean reserved = proposals.containsKey(s);

            if (!hasAccepted && !isChosen && !reserved) {
                return s;
            }

            s++;
        }
    }

    private void applyGc(int newFirstNonCleared) {
        if (newFirstNonCleared <= firstNonCleared) {
            return;
        }

        for (int slot = firstNonCleared; slot < newFirstNonCleared; slot++) {
            log.remove(slot);
            proposals.remove(slot);
            p2bResponders.remove(slot);
            adopted.remove(slot);
        }

        firstNonCleared = newFirstNonCleared;
        recomputeLastNonEmpty();
    }

    private void recomputeLastNonEmpty() {
        int max = firstNonCleared - 1;

        for (int slot : log.keySet()) {
            if (slot >= firstNonCleared) {
                max = Math.max(max, slot);
            }
        }

        lastNonEmpty = max;
    }

    private PaxosRequest makeNoOpRequest() {
        AMOCommand amo = new AMOCommand(new NoOp(), address(), -1);
        return new PaxosRequest(amo);
    }

    private boolean isNoOp(PaxosRequest req) {
        if (req == null || req.command() == null) {
            return false;
        }
        return req.command().command() instanceof NoOp;
    }

    private boolean sameRequest(PaxosRequest a, PaxosRequest b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        return a.equals(b);
    }

    private Integer existingProposalSlot(PaxosRequest req) {
        for (Map.Entry<Integer, PaxosRequest> e : proposals.entrySet()) {
            if (sameRequest(e.getValue(), req)) {
                return e.getKey();
            }
        }

        for (Map.Entry<Integer, LogEntry> e : log.entrySet()) {
            int slot = e.getKey();
            LogEntry entry = e.getValue();

            if (slot < firstNonCleared) {
                continue;
            }

            if (entry == null) {
                continue;
            }

            if (sameRequest(entry.chosenValue, req) || sameRequest(entry.acceptedValue, req)) {
                return slot;
            }
        }

        return null;
    }

    private void resendP2A(int slot) {
        Phase2State st = p2bResponders.get(slot);
        LogEntry entry = log.get(slot);

        if (!isActive || st == null || entry == null || entry.chosen) {
            return;
        }
        if (!st.ballot.equals(myBallot)) {
            return;
        }

        broadcast(new P2A(myBallot, slot, st.value));
    }
}
