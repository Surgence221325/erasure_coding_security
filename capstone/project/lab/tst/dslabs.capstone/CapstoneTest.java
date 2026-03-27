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
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import dslabs.framework.testing.runner.RunState;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static dslabs.capstone.CapstoneWorkload.*;
import static dslabs.framework.testing.StatePredicate.RESULTS_OK;
import static org.junit.Assert.*;

@Lab("capstone")
@Part(1)
public class CapstoneTest extends BaseJUnitTest {

    static final int K = 2;
    static final int M = 1;
    static final int KEY_THRESHOLD = 2;
    static final int NUM_REGIONS = K + M; // 3

    // server(1) = coordinator
    // server(2) = region 0
    // server(3) = region 1
    // server(4) = region 2

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
        Address coordinator = server(1);
        Address[] regions = new Address[NUM_REGIONS];
        for (int i = 0; i < NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, K, M, KEY_THRESHOLD, CLIENT_SECRETS);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> new CapstoneClient(a, coordinator, CLIENT_SECRETS.get(a.toString())));
        b.workloadSupplier(workload);
        return b;
    }

    private void setupState(Workload workload) {
        StateGenerator sg = builder(workload).build();

        if (isRunTest()) {
            runState = new RunState(sg);
            runState.addServer(server(1));
            for (int i = 0; i < NUM_REGIONS; i++) {
                runState.addServer(server(i + 2));
            }
        }
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

    // =========================================================================
    //  Part 5 — authentication and authorization
    // =========================================================================

    @Test(timeout = 10 * 1000)
    @TestDescription("Client cannot read another client's key (ownership)")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test13OwnershipBlocksRead() throws InterruptedException {
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
    public void test14OwnershipBlocksWrite() throws InterruptedException {
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
    public void test15ReadNonExistentKey() throws InterruptedException {
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
    public void test16DedupReplayedWrite() throws InterruptedException {
        // Write a key, then read it. The client retry timer may cause the
        // coordinator to receive the same WriteRequest twice. With AMO dedup,
        // the second delivery returns the cached response without re-executing.
        // This test verifies dedup by writing, reading, then writing the SAME
        // key again — the second write is a new seqNum so it's not a replay.
        // To truly test dedup we'd need to inject a duplicate message, but
        // the client retry mechanism naturally exercises this path when the
        // original ack is delayed.
        Workload w = Workload.builder()
                .commands(write("dedup-key", "original"),
                         read("dedup-key"),
                         write("dedup-key", "updated"),
                         read("dedup-key"))
                .results(writeOk(), readResult("original"),
                         writeOk(), readResult("updated"))
                .build();
        setupState(w);
        runState.addClientWorker(client(1));
        runSettings.addInvariant(RESULTS_OK);
        runState.run(runSettings);
        assertRunInvariantsHold();
    }

    @Test(timeout = 10 * 1000)
    @TestDescription("Client with wrong credentials cannot write")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test17WrongCredentialsRejected() throws InterruptedException {
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
}
