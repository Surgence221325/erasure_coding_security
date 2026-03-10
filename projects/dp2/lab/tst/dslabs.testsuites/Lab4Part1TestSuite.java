package dslabs.testsuites;

import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.paxos.PaxosTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses(PaxosTest.class)
public interface Lab4Part1TestSuite {
}