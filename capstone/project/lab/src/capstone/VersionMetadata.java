package dslabs.capstone;

import java.io.Serializable;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;

/**
 * Coordinator-side metadata for one version of one object.
 *
 * The coordinator stores NO plaintext, NO ciphertext, NO AES key.
 * Those are distributed across regions.  What it stores:
 *
 *   - Fragment placement/checksum info (to verify integrity on read)
 *   - The AES IV (not secret — CBC IV is public)
 *   - Original ciphertext length (to strip erasure-coding padding on decode)
 *   - Erasure coding parameters (k, m) and key-share threshold
 *   - Commit status — uncommitted versions are invisible to readers
 */
public class VersionMetadata implements Serializable {

    public final String key;
    public final int    version;
    public final int    k;            // data fragments
    public final int    m;            // parity fragments
    public final int    keyThreshold; // Shamir shares needed to decrypt

    /** AES-CBC IV: public, stored at coordinator. */
    public final byte[] iv;

    /** Length of ciphertext before erasure-coding padding was added. */
    public final int originalCiphertextLength;

    /** SHA-256 checksum per fragment index — verifies integrity on read. */
    public final Map<Integer, byte[]> fragmentChecksums;

    /**
     * Becomes true once the coordinator has received >= k fragment acks
     * AND >= keyThreshold key-share acks.  Only committed versions are
     * returned to clients.
     */
    public boolean committed = false;

    public VersionMetadata(String key, int version, int k, int m, int keyThreshold,
                           byte[] iv, int originalCiphertextLength,
                           Map<Integer, byte[]> fragmentChecksums) {
        this.key                      = key;
        this.version                  = version;
        this.k                        = k;
        this.m                        = m;
        this.keyThreshold             = keyThreshold;
        this.iv                       = iv;
        this.originalCiphertextLength = originalCiphertextLength;
        this.fragmentChecksums        = fragmentChecksums;
    }

    public boolean verifyFragment(int regionIndex, byte[] fragment) {
        byte[] expected = fragmentChecksums.get(regionIndex);
        if (expected == null || fragment == null) return false;
        return Arrays.equals(expected, sha256(fragment));
    }

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Override
    public String toString() {
        return "VersionMetadata(key=" + key + ", v=" + version
             + ", k=" + k + ", m=" + m + ", threshold=" + keyThreshold
             + ", committed=" + committed + ")";
    }
}
