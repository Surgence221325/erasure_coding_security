package dslabs.capstone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Shared cryptographic utility methods.
 * Avoids duplicating HMAC-SHA256 across CoordinatorNode, CapstoneClient, and RegionalNode.
 */
final class CryptoUtil {
    private CryptoUtil() {}

    static byte[] hmacSha256(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) { throw new RuntimeException("HMAC failed", e); }
    }
}
