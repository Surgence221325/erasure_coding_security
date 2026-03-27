package dslabs.capstone;

import java.util.Arrays;

/**
 * Reed-Solomon erasure coding over GF(256) using a Vandermonde generator matrix.
 *
 * Produces k+m fragments from k data chunks such that any k of the k+m fragments
 * suffice to reconstruct the original data.  Losing up to m fragments is tolerated.
 *
 * Encoding:  output[i][p] = sum_j  V[i][j] * chunk[j][p]   (arithmetic in GF(256))
 * Decoding:  given any k fragment indices, invert the k×k submatrix of V via
 *            Gauss-Jordan over GF(256) to recover the original data chunks.
 *
 * The Vandermonde matrix V[i][j] = (i+1)^j guarantees that any k rows form an
 * invertible submatrix — the classical property that enables erasure tolerance.
 */
public class ErasureCoder {

    private final int k;
    private final int m;
    private final int[][] V;  // (k+m) × k Vandermonde generator matrix

    public ErasureCoder(int k, int m) {
        this.k = k;
        this.m = m;
        this.V = buildVandermonde(k + m, k);
    }

    public int getK() { return k; }
    public int getM() { return m; }

    // -------------------------------------------------------------------------
    //  Encoding
    // -------------------------------------------------------------------------

    /**
     * Encode data into k+m fragments.
     * Pads to a multiple of k bytes; caller must remember original length for decode.
     */
    public byte[][] encode(byte[] data) {
        int paddedLen = ((data.length + k - 1) / k) * k;
        byte[] padded = Arrays.copyOf(data, paddedLen);
        int chunkSize = paddedLen / k;

        byte[][] chunks = new byte[k][chunkSize];
        for (int i = 0; i < k; i++)
            System.arraycopy(padded, i * chunkSize, chunks[i], 0, chunkSize);

        byte[][] fragments = new byte[k + m][chunkSize];
        for (int i = 0; i < k + m; i++) {
            for (int p = 0; p < chunkSize; p++) {
                int val = 0;
                for (int j = 0; j < k; j++)
                    val = GF256.add(val, GF256.mul(V[i][j], chunks[j][p] & 0xFF));
                fragments[i][p] = (byte) val;
            }
        }
        return fragments;
    }

    // -------------------------------------------------------------------------
    //  Decoding
    // -------------------------------------------------------------------------

    /**
     * Decode fragments back to original data.
     * @param fragments  Length-(k+m) array; null entries are erased/missing.
     * @param originalLength  Original data length before padding.
     */
    public byte[] decode(byte[][] fragments, int originalLength) {
        int[] available = new int[k];
        int count = 0;
        for (int i = 0; i < k + m && count < k; i++)
            if (fragments[i] != null) available[count++] = i;

        if (count < k)
            throw new IllegalStateException("Need " + k + " fragments, only have " + count);

        int chunkSize = fragments[available[0]].length;

        int[][] Vsub = new int[k][k];
        for (int r = 0; r < k; r++)
            Vsub[r] = Arrays.copyOf(V[available[r]], k);

        int[][] Vinv = gaussianInvert(Vsub);

        byte[][] recovered = new byte[k][chunkSize];
        for (int i = 0; i < k; i++) {
            for (int p = 0; p < chunkSize; p++) {
                int val = 0;
                for (int r = 0; r < k; r++)
                    val = GF256.add(val, GF256.mul(Vinv[i][r], fragments[available[r]][p] & 0xFF));
                recovered[i][p] = (byte) val;
            }
        }

        byte[] result = new byte[k * chunkSize];
        for (int i = 0; i < k; i++)
            System.arraycopy(recovered[i], 0, result, i * chunkSize, chunkSize);
        return Arrays.copyOf(result, originalLength);
    }

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    private int[][] buildVandermonde(int rows, int cols) {
        int[][] v = new int[rows][cols];
        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                v[i][j] = GF256.pow(i + 1, j);
        return v;
    }

    /** Gauss-Jordan inversion over GF(256). */
    private int[][] gaussianInvert(int[][] mat) {
        int n = mat.length;
        int[][] aug = new int[n][2 * n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(mat[i], 0, aug[i], 0, n);
            aug[i][n + i] = 1;
        }

        for (int col = 0; col < n; col++) {
            int pivot = -1;
            for (int row = col; row < n; row++)
                if (aug[row][col] != 0) { pivot = row; break; }
            if (pivot < 0) throw new ArithmeticException("Singular matrix");

            int[] tmp = aug[col]; aug[col] = aug[pivot]; aug[pivot] = tmp;

            int scale = GF256.inv(aug[col][col]);
            for (int j = col; j < 2 * n; j++)
                aug[col][j] = GF256.mul(aug[col][j], scale);

            for (int row = 0; row < n; row++) {
                if (row == col || aug[row][col] == 0) continue;
                int factor = aug[row][col];
                for (int j = col; j < 2 * n; j++)
                    aug[row][j] = GF256.sub(aug[row][j], GF256.mul(factor, aug[col][j]));
            }
        }

        int[][] inv = new int[n][n];
        for (int i = 0; i < n; i++)
            System.arraycopy(aug[i], n, inv[i], 0, n);
        return inv;
    }
}
