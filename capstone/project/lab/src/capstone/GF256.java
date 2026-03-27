package dslabs.capstone;

/**
 * Arithmetic over GF(2^8) — the finite field with 256 elements.
 *
 * We use the irreducible polynomial x^8 + x^4 + x^3 + x^2 + 1 (0x11d),
 * which is standard for Reed-Solomon implementations.
 *
 * Key facts:
 *   - Addition (and subtraction) in GF(2^8) is just XOR. There's no carry.
 *   - Multiplication is polynomial multiplication modulo the irreducible poly.
 *   - We precompute log/antilog tables so multiplication becomes O(1) table lookups.
 *
 * This class is the shared foundation for ErasureCoder and ShamirSecretSharing.
 * Both need finite field arithmetic over the same field.
 */
public class GF256 {

    private static final int POLY = 0x11d;

    // exp[i] = generator^i mod POLY.  Generator g=2 works for 0x11d.
    // Doubled to 512 entries so LOG[a]+LOG[b] can index directly without mod.
    static final int[] EXP = new int[512];
    static final int[] LOG = new int[256];

    static {
        int x = 1;
        for (int i = 0; i < 255; i++) {
            EXP[i] = x;
            LOG[x] = i;
            x <<= 1;
            if ((x & 0x100) != 0) x ^= POLY;
        }
        for (int i = 255; i < 512; i++) EXP[i] = EXP[i - 255];
    }

    /** Addition in GF(2^8) is XOR. */
    public static int add(int a, int b) { return a ^ b; }

    /** Subtraction in GF(2^8) is identical to addition. */
    public static int sub(int a, int b) { return a ^ b; }

    /** Multiplication via discrete log: mul(a,b) = g^(log(a)+log(b)). */
    public static int mul(int a, int b) {
        if (a == 0 || b == 0) return 0;
        return EXP[LOG[a] + LOG[b]];
    }

    /** Division: a/b = g^(log(a)-log(b)). */
    public static int div(int a, int b) {
        if (a == 0) return 0;
        if (b == 0) throw new ArithmeticException("GF256: division by zero");
        return EXP[(LOG[a] - LOG[b] + 255) % 255];
    }

    /** Exponentiation: base^exp in GF(256). */
    public static int pow(int base, int exp) {
        if (exp == 0) return 1;
        if (base == 0) return 0;
        return EXP[(LOG[base] * exp) % 255];
    }

    /** Multiplicative inverse: the unique b such that a*b = 1. */
    public static int inv(int a) {
        if (a == 0) throw new ArithmeticException("GF256: no inverse of 0");
        return EXP[255 - LOG[a]];
    }
}
