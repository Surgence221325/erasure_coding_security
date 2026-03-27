package dslabs.capstone;

import java.security.SecureRandom;

/**
 * Shamir's threshold secret sharing over GF(256).
 *
 * Splits a secret (our AES key) into n shares such that any `threshold` shares
 * reconstruct it, but fewer reveal nothing (information-theoretic security).
 *
 * For a multi-byte secret (e.g. 16-byte AES key) we apply the single-byte
 * scheme independently to each byte position — shares are 16-byte arrays.
 *
 * Split:    build a random degree-(t-1) polynomial with f(0)=secret; share_i = f(i+1)
 * Recover:  Lagrange interpolation at x=0 from any t (x_i, y_i) pairs
 *
 * Reuses GF256 arithmetic — both erasure coding and secret sharing use the same field.
 */
public class ShamirSecretSharing {

    private static final SecureRandom RNG = new SecureRandom();

    /**
     * Split secret into n shares, requiring `threshold` to reconstruct.
     * @return shares[i] is the share for region index i (evaluated at x=i+1).
     */
    public static byte[][] split(byte[] secret, int n, int threshold) {
        byte[][] shares = new byte[n][secret.length];
        for (int b = 0; b < secret.length; b++) {
            int[] poly = new int[threshold];
            poly[0] = secret[b] & 0xFF;
            for (int d = 1; d < threshold; d++) poly[d] = RNG.nextInt(256);

            for (int i = 0; i < n; i++)
                shares[i][b] = (byte) evalPoly(poly, i + 1);
        }
        return shares;
    }

    /**
     * Reconstruct secret from a subset of shares.
     * @param shares   The collected share byte arrays.
     * @param indices  0-based region indices (x = index+1).
     */
    public static byte[] recover(byte[][] shares, int[] indices) {
        int secretLen = shares[0].length;
        byte[] secret = new byte[secretLen];
        for (int b = 0; b < secretLen; b++) {
            int[] xs = new int[shares.length];
            int[] ys = new int[shares.length];
            for (int i = 0; i < shares.length; i++) {
                xs[i] = indices[i] + 1;
                ys[i] = shares[i][b] & 0xFF;
            }
            secret[b] = (byte) lagrangeAt0(xs, ys);
        }
        return secret;
    }

    // Evaluate polynomial via Horner's method over GF(256)
    private static int evalPoly(int[] poly, int x) {
        int result = 0, xpow = 1;
        for (int coeff : poly) {
            result = GF256.add(result, GF256.mul(coeff, xpow));
            xpow = GF256.mul(xpow, x);
        }
        return result;
    }

    // Lagrange interpolation at x=0: f(0) = sum_i y_i * product_{j!=i} x_j/(x_i XOR x_j)
    private static int lagrangeAt0(int[] xs, int[] ys) {
        int result = 0;
        for (int i = 0; i < xs.length; i++) {
            int num = 1, den = 1;
            for (int j = 0; j < xs.length; j++) {
                if (j == i) continue;
                num = GF256.mul(num, xs[j]);
                den = GF256.mul(den, GF256.sub(xs[i], xs[j]));
            }
            result = GF256.add(result, GF256.mul(ys[i], GF256.mul(num, GF256.inv(den))));
        }
        return result;
    }
}
