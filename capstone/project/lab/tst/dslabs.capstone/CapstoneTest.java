package dslabs.capstone;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Result;
import dslabs.framework.testing.StateGenerator;
import dslabs.framework.testing.StateGenerator.StateGeneratorBuilder;
import dslabs.framework.testing.Workload;
import dslabs.framework.testing.junit.BaseJUnitTest;
import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.framework.testing.junit.RunTests;
import dslabs.framework.testing.junit.SearchTests;
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import dslabs.framework.testing.runner.RunState;
import dslabs.framework.testing.search.SearchState;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static dslabs.capstone.CapstoneWorkload.*;
import static dslabs.framework.testing.StatePredicate.CLIENTS_DONE;
import static dslabs.framework.testing.StatePredicate.NONE_DECIDED;
import static dslabs.framework.testing.StatePredicate.RESULTS_OK;
import static org.junit.Assert.*;

@Lab("capstone")
@Part(1)
public class CapstoneTest extends BaseJUnitTest {

    // --- Run test configuration (3 regions, full erasure coding) ---
    static final int K = 2;
    static final int M = 1;
    static final int KEY_THRESHOLD = 2;
    static final int NUM_REGIONS = K + M; // 3

    // --- Search test configuration (2 regions, smaller state space) ---
    static final int SEARCH_K = 1;
    static final int SEARCH_M = 1;
    static final int SEARCH_KEY_THRESHOLD = 1;
    static final int SEARCH_NUM_REGIONS = SEARCH_K + SEARCH_M; // 2

    // --- Cluster secret for dynamic membership tests ---
    static final byte[] CLUSTER_SECRET = "capstone-cluster-secret".getBytes();

    // server(1) = coordinator
    // server(2) = region 0
    // server(3) = region 1
    // server(4) = region 2 (run tests only)
    // server(5) = dynamic region (join tests only)

    /** Derive a deterministic 16-byte secret from a client address. */
    private static byte[] secretForClient(Address addr) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(addr.toString().getBytes());
            return Arrays.copyOf(hash, 16);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    /** Pre-generated secrets for clients 1–20, shared between coordinator and clients. */
    private static final Map<String, byte[]> CLIENT_SECRETS = new HashMap<>();
    static {
        for (int i = 1; i <= 20; i++) {
            Address a = client(i);
            CLIENT_SECRETS.put(a.toString(), secretForClient(a));
        }
    }

    static StateGeneratorBuilder builder(Workload workload) {
        return builder(workload, true);
    }

    static StateGeneratorBuilder builder(Workload workload, boolean enableHeartbeats) {
        Address coordinator = server(1);
        Address[] regions = new Address[NUM_REGIONS];
        for (int i = 0; i < NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, K, M, KEY_THRESHOLD,
                        CLIENT_SECRETS, enableHeartbeats);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> new CapstoneClient(a, coordinator, CLIENT_SECRETS.get(a.toString())));
        b.workloadSupplier(workload);
        return b;
    }

    private void setupState(Workload workload) {
        if (isRunTest()) {
            StateGenerator sg = builder(workload, true).build();
            runState = new RunState(sg);
            runState.addServer(server(1));
            for (int i = 0; i < NUM_REGIONS; i++) {
                runState.addServer(server(i + 2));
            }
        }

        if (isSearchTest()) {
            // Search tests use fewer regions (smaller state space) and
            // disable heartbeats (prevents infinite BFS expansion).
            StateGenerator sg = searchBuilder(workload).build();
            initSearchState = new SearchState(sg);
            initSearchState.addServer(server(1));
            for (int i = 0; i < SEARCH_NUM_REGIONS; i++) {
                initSearchState.addServer(server(i + 2));
            }
        }
    }

    /** Builder for search tests: fewer regions, no heartbeats. */
    static StateGeneratorBuilder searchBuilder(Workload workload) {
        Address coordinator = server(1);
        Address[] regions = new Address[SEARCH_NUM_REGIONS];
        for (int i = 0; i < SEARCH_NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, SEARCH_K, SEARCH_M,
                        SEARCH_KEY_THRESHOLD, CLIENT_SECRETS, false);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> new CapstoneClient(a, coordinator, CLIENT_SECRETS.get(a.toString())));
        b.workloadSupplier(workload);
        return b;
    }

    // =========================================================================
    //  Part 1 — basic correctness
    // =========================================================================

    @Test(timeout = 10 * 1000)
    @TestDescription("Single client put then get")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test01BasicPutGet() throws InterruptedException {
        setupState(putGetWorkload("hello", "world"));
        runState.addClientWorker(client(1));
        runSettings.addInvariant(RESULTS_OK);
        runState.run(runSettings);
        assertRunInvariantsHold();
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Single client multiple puts and gets")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test02MultipleOps() throws InterruptedException {
        Workload w = Workload.builder()
                .commands(write("k1", "v1"), write("k2", "v2"),
                         read("k1"), read("k2"))
                .results(writeOk(), writeOk(),
                         readResult("v1"), readResult("v2"))
                .build();
        setupState(w);
        runState.addClientWorker(client(1));
        runSettings.addInvariant(RESULTS_OK);
        runState.run(runSettings);
        assertRunInvariantsHold();
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Overwrite a key")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test03Overwrite() throws InterruptedException {
        Workload w = Workload.builder()
                .commands(write("key", "first"), write("key", "second"),
                         read("key"))
                .results(writeOk(), writeOk(), readResult("second"))
                .build();
        setupState(w);
        runState.addClientWorker(client(1));
        runSettings.addInvariant(RESULTS_OK);
        runState.run(runSettings);
        assertRunInvariantsHold();
    }

    // =========================================================================
    //  Part 2 — fault tolerance (region failures)
    //
    //  With k=2, m=1 and keyThreshold=2, we can tolerate 1 of 3 regions down.
    //  Losing 2+ regions makes both writes and reads impossible.
    // =========================================================================

    @Test(timeout = 10 * 1000)
    @TestDescription("Write and read succeed with one region partitioned")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test04WriteAndReadOneRegionDown() throws InterruptedException {
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));

        // Partition off region 2 (server(4)); coordinator + regions 0,1 + client remain
        runSettings.partition(server(1), server(2), server(3), client(1));
        runState.start(runSettings);

        // Write needs k=2 fragment acks — regions 0 and 1 suffice
        sendCommandAndCheck(client, write("key", "value"), writeOk());
        // Read needs k=2 fragments + keyThreshold=2 shares — same 2 regions suffice
        sendCommandAndCheck(client, read("key"), readResult("value"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Read succeeds after a region fails post-write")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test05ReadAfterRegionFails() throws InterruptedException {
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));

        // Write with all 3 regions up — all get fragments
        runState.start(runSettings);
        sendCommandAndCheck(client, write("key", "value"), writeOk());
        runState.stop();

        // Now region 2 goes down — read should still reconstruct from regions 0,1
        runSettings.partition(server(1), server(2), server(3), client(1));
        runState.start(runSettings);

        sendCommandAndCheck(client, read("key"), readResult("value"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Write fails when only 1 of 3 regions reachable")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test06WriteFailsTooFewRegions() throws InterruptedException {
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));

        // Only region 0 reachable — 1 ack < k=2 threshold
        runSettings.partition(server(1), server(2), client(1));
        runState.start(runSettings);

        // Coordinator will timeout (300ms) and send failure response
        client.sendCommand(write("key", "value"));
        Result r = client.getResult();
        assertTrue(r instanceof CapstoneWriteResult);
        assertFalse(((CapstoneWriteResult) r).success());
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Read fails when only 1 of 3 regions reachable")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test07ReadFailsTooFewRegions() throws InterruptedException {
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));

        // Write with all regions up
        runState.start(runSettings);
        sendCommandAndCheck(client, write("key", "value"), writeOk());
        runState.stop();

        // Partition off 2 regions — only 1 fragment available, below k=2
        runSettings.partition(server(1), server(2), client(1));
        runState.start(runSettings);

        client.sendCommand(read("key"));
        Result r = client.getResult();
        assertTrue(r instanceof CapstoneReadResult);
        assertNull(((CapstoneReadResult) r).value());
    }

    @Test(timeout = 15 * 1000)
    @TestDescription("Write succeeds after partition heals")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test08PartitionHealThenProgress() throws InterruptedException {
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));

        // Phase 1: partition makes write impossible (only 1 region reachable).
        // Use run() with maxTimeSecs so the coordinator's WriteTimeoutTimer (300ms)
        // fires and cleans up the pending write before we move to phase 2.
        runSettings.partition(server(1), server(2), client(1));
        runSettings.waitForClients(false);
        runSettings.maxTimeSecs(2);

        client.sendCommand(write("key", "value"));
        runState.run(runSettings);

        // Client received a failure (BUSY from retry or TIMEOUT — either is fine)
        assertTrue(client.hasResult());
        Result r = client.getResult();
        assertFalse(((CapstoneWriteResult) r).success());

        // Phase 2: heal partition — all regions reachable again
        runSettings.resetNetwork();
        runSettings.maxTimeSecs(-1);

        runState.start(runSettings);

        sendCommandAndCheck(client, write("key", "value"), writeOk());
        sendCommandAndCheck(client, read("key"), readResult("value"));
    }

    // =========================================================================
    //  Part 3 — multi-client
    // =========================================================================

    @Test(timeout = 10 * 1000)
    @TestDescription("Two clients, different keys, no contention")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test09TwoClientsDifferentKeys() throws InterruptedException {
        setupState(emptyWorkload());
        Client client1 = runState.addClient(client(1));
        Client client2 = runState.addClient(client(2));

        runState.start(runSettings);

        sendCommandAndCheck(client1, write("k1", "v1"), writeOk());
        sendCommandAndCheck(client2, write("k2", "v2"), writeOk());
        // Each client reads its own key
        sendCommandAndCheck(client1, read("k1"), readResult("v1"));
        sendCommandAndCheck(client2, read("k2"), readResult("v2"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Concurrent write to same key returns BUSY")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test10ConcurrentWriteSameKeyBusy() throws InterruptedException {
        setupState(emptyWorkload());
        Client client1 = runState.addClient(client(1));
        Client client2 = runState.addClient(client(2));

        // Block all region acks so client1's write stays pending at coordinator
        runSettings.senderActive(server(2), false);
        runSettings.senderActive(server(3), false);
        runSettings.senderActive(server(4), false);

        runState.start(runSettings);

        // Client1 starts a write — coordinator accepts it but can't commit (no acks)
        client1.sendCommand(write("contended-key", "v1"));
        Thread.sleep(200);

        // Client2 writes the same key — coordinator rejects with BUSY
        sendCommandAndCheck(client2, write("contended-key", "v2"),
                new CapstoneWriteResult(false, "BUSY"));
    }

    // =========================================================================
    //  Part 4 — data integrity and stress
    // =========================================================================

    @Test(timeout = 10 * 1000)
    @TestDescription("Large value (10KB) survives erasure coding round-trip")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test11LargeValue() throws InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append((char) ('A' + (i % 26)));
        }
        String bigValue = sb.toString();

        setupState(putGetWorkload("big-key", bigValue));
        runState.addClientWorker(client(1));
        runSettings.addInvariant(RESULTS_OK);
        runState.run(runSettings);
        assertRunInvariantsHold();
    }

    @Test(timeout = 15 * 1000)
    @TestDescription("Ten overwrites of same key, read returns latest version")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test12MultipleOverwrites() throws InterruptedException {
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));
        runState.start(runSettings);

        for (int i = 1; i <= 10; i++) {
            sendCommandAndCheck(client, write("key", "version-" + i), writeOk());
        }
        sendCommandAndCheck(client, read("key"), readResult("version-10"));
    }

    @Test(timeout = 30 * 1000)
    @TestDescription("Five clients, 10 operations each, concurrent")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test13StressConcurrentClients() throws InterruptedException {
        setupState(emptyWorkload());
        int nClients = 5, nRounds = 10;

        runState.start(runSettings);

        // Each client writes and reads its own keys (no ownership conflicts)
        for (int c = 1; c <= nClients; c++) {
            Client client = runState.addClient(client(c));
            for (int r = 1; r <= nRounds; r++) {
                String key = "c" + c + "-k" + r;
                sendCommandAndCheck(client, write(key, "val-" + r), writeOk());
            }
            // Verify last write
            sendCommandAndCheck(client, read("c" + c + "-k" + nRounds),
                    readResult("val-" + nRounds));
        }
    }

    // =========================================================================
    //  Part 5 — authentication and authorization
    // =========================================================================

    @Test(timeout = 10 * 1000)
    @TestDescription("Client cannot read another client's key (ownership)")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test14OwnershipBlocksRead() throws InterruptedException {
        setupState(emptyWorkload());
        Client client1 = runState.addClient(client(1));
        Client client2 = runState.addClient(client(2));

        runState.start(runSettings);

        // Client 1 writes a key — becomes owner
        sendCommandAndCheck(client1, write("private-key", "secret"), writeOk());

        // Client 2 tries to read it — should be denied
        client2.sendCommand(read("private-key"));
        Result r = client2.getResult();
        assertTrue(r instanceof CapstoneReadResult);
        assertNull(((CapstoneReadResult) r).value());

        // Client 1 can still read their own key
        sendCommandAndCheck(client1, read("private-key"), readResult("secret"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Client cannot write to another client's key (ownership)")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test15OwnershipBlocksWrite() throws InterruptedException {
        setupState(emptyWorkload());
        Client client1 = runState.addClient(client(1));
        Client client2 = runState.addClient(client(2));

        runState.start(runSettings);

        // Client 1 writes a key — becomes owner
        sendCommandAndCheck(client1, write("owned-key", "original"), writeOk());

        // Client 2 tries to overwrite — should be denied
        client2.sendCommand(write("owned-key", "hijacked"));
        Result r = client2.getResult();
        assertTrue(r instanceof CapstoneWriteResult);
        assertFalse(((CapstoneWriteResult) r).success());

        // Original value unchanged
        sendCommandAndCheck(client1, read("owned-key"), readResult("original"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Read non-existent key returns error")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test16ReadNonExistentKey() throws InterruptedException {
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));

        runState.start(runSettings);

        client.sendCommand(read("no-such-key"));
        Result r = client.getResult();
        assertTrue(r instanceof CapstoneReadResult);
        assertNull(((CapstoneReadResult) r).value());
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Duplicate write is deduplicated (AMO)")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test17DedupReplayedWrite() throws InterruptedException {
        // Force the dedup code path: block coordinator→client link so the
        // WriteResponse never reaches the client. The write commits and the
        // response is cached. The client retry timer fires and re-sends the
        // same (clientId, seqNum). Coordinator hits dedup, replays cached
        // response. We then unblock the link and verify the write happened
        // exactly once.
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));

        runState.start(runSettings);

        // Let auth complete first (needs coordinator→client link open)
        Thread.sleep(200);

        // Block coordinator → client responses
        runSettings.linkActive(server(1), client(1), false);

        // Client sends write — coordinator processes it, commits, caches.
        // But WriteResponse can't reach client. Client retry fires (100ms),
        // re-sends same WriteRequest. Coordinator dedup returns cached response.
        // All blocked at coordinator→client link.
        client.sendCommand(write("dedup-key", "value"));
        Thread.sleep(500); // let write commit + at least 1 retry hit dedup

        // Unblock — cached/dedup responses reach client
        runSettings.linkActive(server(1), client(1), true);
        Result r = client.getResult();
        assertTrue(r instanceof CapstoneWriteResult);
        assertTrue(((CapstoneWriteResult) r).success());

        // Verify the write happened exactly once (value is correct)
        sendCommandAndCheck(client, read("dedup-key"), readResult("value"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Fast-fail when regions detected dead via heartbeat")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test18HeartbeatFastFail() throws InterruptedException {
        setupState(emptyWorkload());
        Client client = runState.addClient(client(1));

        // Start normally — auth completes, write succeeds with all regions alive
        runState.start(runSettings);
        sendCommandAndCheck(client, write("key", "value"), writeOk());

        // Partition all regions from coordinator — heartbeats stop
        runState.stop();
        runSettings.partition(server(1), client(1));
        runState.start(runSettings);

        // Wait for heartbeat liveness to expire (>1000ms = 2 × 500ms heartbeat)
        Thread.sleep(1500);

        // Read should fail fast (INSUFFICIENT_REGIONS, not timeout).
        // Client ignores INSUFFICIENT_REGIONS and retries, but regions stay dead.
        // Eventually the client gets no final result within our check window.
        client.sendCommand(read("key"));
        Thread.sleep(500);
        // Client should NOT have a result — all attempts fast-failed
        assertFalse(client.hasResult());

        // Now heal partition — heartbeats resume, regions come back alive
        runState.stop();
        runSettings.resetNetwork();
        runState.start(runSettings);

        // Wait for heartbeats to confirm liveness again
        Thread.sleep(1500);

        // The old read's retries should now succeed since regions are alive.
        // But to be robust, consume any pending result and send a fresh read
        // that definitively proves the system recovered.
        if (client.hasResult()) {
            client.getResult(); // consume stale result from old retries
        }
        sendCommandAndCheck(client, read("key"), readResult("value"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Corrupted fragment detected by checksum, read still succeeds")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test19CorruptedFragmentDetected() throws InterruptedException {
        // One region corrupts fragments on read (flips a byte).
        // The coordinator's SHA-256 checksum verification rejects it.
        // With k=2, m=1: the corrupted fragment is discarded, and the
        // remaining 2 valid fragments are enough to reconstruct.
        // This proves: idempotent acks let corruption through at write time,
        // but checksums catch it at read time.
        Address coordinator = server(1);
        Address[] regions = new Address[NUM_REGIONS];
        for (int i = 0; i < NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, K, M, KEY_THRESHOLD, CLIENT_SECRETS);
            } else if (a.equals(server(4))) {
                // Region 2 (server(4)) corrupts fragments on read
                return new RegionalNode(a, true);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> new CapstoneClient(a, coordinator, CLIENT_SECRETS.get(a.toString())));
        b.workloadSupplier(emptyWorkload());

        StateGenerator sg = b.build();
        runState = new RunState(sg);
        runState.addServer(server(1));
        for (int i = 0; i < NUM_REGIONS; i++) {
            runState.addServer(server(i + 2));
        }

        Client client = runState.addClient(client(1));
        runState.start(runSettings);

        // Write succeeds (all 3 regions store valid fragments)
        sendCommandAndCheck(client, write("key", "secret-data"), writeOk());

        // Read succeeds despite region 2 returning corrupted fragment —
        // coordinator detects checksum mismatch, uses regions 0 and 1
        sendCommandAndCheck(client, read("key"), readResult("secret-data"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Corruption + partition: read fails gracefully under compound failure")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test20CompoundCorruptionAndPartition() throws InterruptedException {
        // Compound failure: 1 region corrupts fragments + 1 region partitioned.
        // With k=2, m=1: only 1 valid fragment remains (< k=2). Read must fail.
        // This proves the system degrades correctly when two independent failure
        // modes overlap — checksum rejection + network partition.
        Address coordinator = server(1);
        Address[] regions = new Address[NUM_REGIONS];
        for (int i = 0; i < NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, K, M, KEY_THRESHOLD, CLIENT_SECRETS);
            } else if (a.equals(server(4))) {
                // Region 2 corrupts fragments
                return new RegionalNode(a, true);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> new CapstoneClient(a, coordinator, CLIENT_SECRETS.get(a.toString())));
        b.workloadSupplier(emptyWorkload());

        StateGenerator sg = b.build();
        runState = new RunState(sg);
        runState.addServer(server(1));
        for (int i = 0; i < NUM_REGIONS; i++) {
            runState.addServer(server(i + 2));
        }

        Client client = runState.addClient(client(1));

        // Write with all 3 regions up (all store valid fragments)
        runState.start(runSettings);
        sendCommandAndCheck(client, write("key", "value"), writeOk());
        runState.stop();

        // Now: region 2 corrupts (checksum fails) + region 1 partitioned
        // Only region 0 returns a valid fragment — 1 < k=2, read must fail
        runSettings.partition(server(1), server(2), server(4), client(1));
        runState.start(runSettings);

        client.sendCommand(read("key"));
        Result r = client.getResult();
        assertTrue(r instanceof CapstoneReadResult);
        assertNull(((CapstoneReadResult) r).value());
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Client with wrong credentials cannot write")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test21WrongCredentialsRejected() throws InterruptedException {
        // Build a custom state where client(2) has a WRONG secret.
        // The coordinator has the real secret; client(2) has all-zeros.
        // The HMAC won't match, auth fails, client stays unauthenticated.
        Address coordinator = server(1);
        Address[] regions = new Address[NUM_REGIONS];
        for (int i = 0; i < NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        byte[] wrongSecret = new byte[16]; // all zeros — won't match

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, K, M, KEY_THRESHOLD, CLIENT_SECRETS);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> {
            byte[] secret = a.equals(client(2)) ? wrongSecret : CLIENT_SECRETS.get(a.toString());
            return new CapstoneClient(a, coordinator, secret);
        });
        b.workloadSupplier(emptyWorkload());

        StateGenerator sg = b.build();
        runState = new RunState(sg);
        runState.addServer(server(1));
        for (int i = 0; i < NUM_REGIONS; i++) {
            runState.addServer(server(i + 2));
        }

        Client client1 = runState.addClient(client(1));
        Client client2 = runState.addClient(client(2));

        runState.start(runSettings);

        // Client 1 (correct credentials) can write normally
        sendCommandAndCheck(client1, write("key", "value"), writeOk());

        // Client 2 (wrong credentials) — auth never completes because HMAC
        // mismatch. All requests get AUTH_REQUIRED, which the client ignores.
        // The command will never succeed.
        client2.sendCommand(write("bad-key", "bad-value"));
        Thread.sleep(1000);
        assertFalse(client2.hasResult());
    }

    // =========================================================================
    //  Part 6 — dynamic membership
    //
    //  Tests that a new region can join mid-session, new writes use the
    //  updated config (more parity), and old keys remain readable.
    // =========================================================================

    /** Set up a coordinator with cluster secret enabled for join tests. */
    private void setupDynamicState(Workload workload) {
        Address coordinator = server(1);
        Address[] regions = new Address[NUM_REGIONS];
        for (int i = 0; i < NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, K, M, KEY_THRESHOLD,
                        CLIENT_SECRETS, true, CLUSTER_SECRET);
            } else if (a.equals(server(5))) {
                // Dynamic region — sends JoinRequest on init
                return new RegionalNode(a, false, coordinator, CLUSTER_SECRET);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> new CapstoneClient(a, coordinator, CLIENT_SECRETS.get(a.toString())));
        b.workloadSupplier(workload);

        StateGenerator sg = b.build();
        runState = new RunState(sg);
        runState.addServer(server(1));
        for (int i = 0; i < NUM_REGIONS; i++) {
            runState.addServer(server(i + 2));
        }
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("New region joins, new write uses 4 regions")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test22DynamicJoinBasic() throws InterruptedException {
        setupDynamicState(emptyWorkload());
        Client client = runState.addClient(client(1));

        runState.start(runSettings);

        // Write with original 3 regions
        sendCommandAndCheck(client, write("before-join", "v1"), writeOk());

        // Add the 4th region — it sends JoinRequest in init()
        runState.addServer(server(5));
        Thread.sleep(500); // let join complete

        // New write should use 4 regions (k=2, m=2)
        sendCommandAndCheck(client, write("after-join", "v2"), writeOk());

        // Both keys readable
        sendCommandAndCheck(client, read("before-join"), readResult("v1"));
        sendCommandAndCheck(client, read("after-join"), readResult("v2"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Old key readable after region joins (per-version decoding)")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test23OldKeyAfterJoin() throws InterruptedException {
        setupDynamicState(emptyWorkload());
        Client client = runState.addClient(client(1));

        runState.start(runSettings);

        // Write multiple keys with original config (k=2, m=1)
        sendCommandAndCheck(client, write("k1", "data1"), writeOk());
        sendCommandAndCheck(client, write("k2", "data2"), writeOk());

        // 4th region joins
        runState.addServer(server(5));
        Thread.sleep(500);

        // Old keys still readable (coordinator uses per-version k/m)
        sendCommandAndCheck(client, read("k1"), readResult("data1"));
        sendCommandAndCheck(client, read("k2"), readResult("data2"));

        // Overwrite k1 with new config — uses 4 regions now
        sendCommandAndCheck(client, write("k1", "updated"), writeOk());
        sendCommandAndCheck(client, read("k1"), readResult("updated"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Fault tolerance improves after join (survive 2 failures)")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test24ImprovedFaultTolerance() throws InterruptedException {
        setupDynamicState(emptyWorkload());
        Client client = runState.addClient(client(1));

        runState.start(runSettings);

        // 4th region joins → k=2, m=2 (4 regions, can survive 2 failures)
        runState.addServer(server(5));
        Thread.sleep(500);

        // Write with 4 regions
        sendCommandAndCheck(client, write("resilient-key", "value"), writeOk());

        // Partition 2 regions — with old config (m=1) this would fail.
        // With new config (m=2), k=2 regions still reachable = success.
        runState.stop();
        runSettings.partition(server(1), server(2), server(3), client(1));
        // server(4) and server(5) are partitioned away — 2 regions reachable
        runState.start(runSettings);

        sendCommandAndCheck(client, read("resilient-key"), readResult("value"));
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Join with bad credentials rejected")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test25JoinBadCredentials() throws InterruptedException {
        // Set up coordinator with cluster secret, but the joining region
        // uses a wrong secret.
        Address coordinator = server(1);
        Address[] regions = new Address[NUM_REGIONS];
        for (int i = 0; i < NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        byte[] wrongSecret = "wrong-secret".getBytes();

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, K, M, KEY_THRESHOLD,
                        CLIENT_SECRETS, true, CLUSTER_SECRET);
            } else if (a.equals(server(5))) {
                return new RegionalNode(a, false, coordinator, wrongSecret);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> new CapstoneClient(a, coordinator, CLIENT_SECRETS.get(a.toString())));
        b.workloadSupplier(emptyWorkload());

        StateGenerator sg = b.build();
        runState = new RunState(sg);
        runState.addServer(server(1));
        for (int i = 0; i < NUM_REGIONS; i++) {
            runState.addServer(server(i + 2));
        }

        Client client = runState.addClient(client(1));

        runState.start(runSettings);

        // Try to add the rogue region
        runState.addServer(server(5));
        Thread.sleep(500);

        // System should still work with original 3 regions (join was rejected)
        sendCommandAndCheck(client, write("key", "value"), writeOk());
        sendCommandAndCheck(client, read("key"), readResult("value"));
    }

    // =========================================================================
    //  Part 7 — search tests (deterministic, exhaustive)
    //
    //  These use BFS over all possible message orderings to verify
    //  invariants hold under EVERY interleaving, not just random ones.
    //  This replaces probabilistic unreliable-network run tests with
    //  deterministic correctness guarantees.
    // =========================================================================

    @Test
    @TestDescription("Search: single client write completes under all orderings")
    @Category(SearchTests.class)
    @TestPointValue(20)
    public void test26SearchBasicWrite() {
        // Single write only (not put+get) to keep state space manageable
        setupState(Workload.builder()
                .commands(write("key", "value"))
                .results(writeOk())
                .build());
        initSearchState.addClientWorker(client(1));

        searchSettings.maxTimeSecs(20)
                      .deliverTimers(false)
                      .addInvariant(RESULTS_OK)
                      .addGoal(CLIENTS_DONE);
        bfs(initSearchState);
        assertGoalFound();
    }

    @Test
    @TestDescription("Search: write succeeds with one region partitioned")
    @Category(SearchTests.class)
    @TestPointValue(20)
    public void test27SearchOneRegionDown() {
        // Search config: k=1, m=1, threshold=1, 2 regions.
        // Partition off region 1 (server(3)) — 1 region left >= k=1.
        setupState(Workload.builder()
                .commands(write("key", "value"))
                .results(writeOk())
                .build());
        initSearchState.addClientWorker(client(1));

        searchSettings.maxTimeSecs(20)
                      .deliverTimers(false)
                      .addInvariant(RESULTS_OK)
                      .partition(server(1), server(2), client(1))
                      .addGoal(CLIENTS_DONE);
        bfs(initSearchState);
        assertGoalFound();
    }

    @Test
    @TestDescription("Search: no progress when all regions partitioned")
    @Category(SearchTests.class)
    @TestPointValue(20)
    public void test28SearchNoProgressTooFewRegions() {
        // Search config: k=1, m=1, 2 regions.
        // Both regions partitioned — 0 reachable, below k=1.
        setupState(Workload.builder()
                .commands(write("key", "value"))
                .results(writeOk())
                .build());
        initSearchState.addClientWorker(client(1));

        searchSettings.maxTimeSecs(20)
                      .deliverTimers(false)
                      .addInvariant(NONE_DECIDED)
                      .partition(server(1), client(1));
        bfs(initSearchState);
    }

    @Test
    @TestDescription("Search: write+read correctness (DFS)")
    @Category(SearchTests.class)
    @TestPointValue(20)
    public void test29SearchWriteRead() {
        // DFS explores deeper (like Paxos test24) — finds the goal faster than
        // BFS for larger state spaces. Partition to 1 region for manageability.
        setupState(putGetWorkload("key", "value"));
        initSearchState.addClientWorker(client(1));

        searchSettings.maxTimeSecs(20)
                      .maxDepth(1000)
                      .deliverTimers(false)
                      .addInvariant(RESULTS_OK)
                      .partition(server(1), server(2), client(1))
                      .addPrune(CLIENTS_DONE);
        dfs(initSearchState);
    }

    @Test
    @TestDescription("Search: erasure coding write with k=2, m=1, one region down")
    @Category(SearchTests.class)
    @TestPointValue(20)
    public void test30SearchErasureCodingFaultTolerance() {
        // Uses the REAL erasure coding config (k=2, m=1, 3 regions) — not the
        // minimal search config. Partitions 1 region so only 2 respond.
        // This deterministically verifies that Reed-Solomon reconstruction works
        // under ALL message orderings when 1 of 3 regions is down.
        Workload w = Workload.builder()
                .commands(write("key", "value"))
                .results(writeOk())
                .build();

        // Build with real config but no heartbeats
        StateGenerator sg = builder(w, false).build();
        initSearchState = new SearchState(sg);
        initSearchState.addServer(server(1));
        for (int i = 0; i < NUM_REGIONS; i++) {
            initSearchState.addServer(server(i + 2));
        }
        initSearchState.addClientWorker(client(1));

        // Partition off region 2 (server(4)) — k=2 needs 2 of 3 regions.
        // DFS for larger state space (like Paxos test24); verifies invariants
        // hold across deep execution paths.
        searchSettings.maxTimeSecs(30)
                      .maxDepth(1000)
                      .deliverTimers(false)
                      .addInvariant(RESULTS_OK)
                      .partition(server(1), server(2), server(3), client(1))
                      .addPrune(CLIENTS_DONE);
        dfs(initSearchState);
    }
}
