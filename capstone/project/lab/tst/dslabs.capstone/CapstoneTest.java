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

    static StateGeneratorBuilder builder(Workload workload) {
        Address coordinator = server(1);
        Address[] regions = new Address[NUM_REGIONS];
        for (int i = 0; i < NUM_REGIONS; i++) {
            regions[i] = server(i + 2);
        }

        StateGeneratorBuilder b = StateGenerator.builder();
        b.serverSupplier(a -> {
            if (a.equals(coordinator)) {
                return new CoordinatorNode(a, regions, K, M, KEY_THRESHOLD);
            } else {
                return new RegionalNode(a);
            }
        });
        b.clientSupplier(a -> new CapstoneClient(a, coordinator));
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
        // Cross-read: each client reads the other's key
        sendCommandAndCheck(client1, read("k2"), readResult("v2"));
        sendCommandAndCheck(client2, read("k1"), readResult("v1"));
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
}
