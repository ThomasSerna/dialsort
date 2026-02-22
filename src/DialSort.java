import java.util.Arrays;
import java.util.Random;

/**
 * DialSort (Ordenamiento por Mapeo Espacial No-Comparativo) + Benchmark decente
 * - DialSort adaptativo: COUNTING si U es viable, si no RADIX (int32, 4 pasadas base 256)
 * - Benchmark: warmup + best-of + alterna orden (reduce sesgo)
 */
public class DialSort {

    // Política adaptativa (ajustable)
    private static final int MAX_U_FOR_COUNTING = 5_000_000; // bins max para counts (payload ~20MB)
    private static final int U_TO_N_RATIO = 8;               // counting si U <= 8*n

    public enum Mode { COUNTING, RADIX }

    // =========================================================
    // API principal
    // =========================================================
    public static Mode sort(int[] a) {
        if (a == null) throw new IllegalArgumentException("array null");
        if (a.length <= 1) return Mode.COUNTING;

        int min = a[0], max = a[0];
        for (int v : a) {
            if (v < min) min = v;
            if (v > max) max = v;
        }

        long Ulong = (long) max - (long) min + 1L;

        boolean countingOk =
                Ulong > 0 &&
                        Ulong <= Integer.MAX_VALUE &&
                        Ulong <= MAX_U_FOR_COUNTING &&
                        Ulong <= (long) U_TO_N_RATIO * (long) a.length;

        if (countingOk) {
            countingInPlace(a, min, (int) Ulong);
            return Mode.COUNTING;
        } else {
            radixInPlace(a);
            return Mode.RADIX;
        }
    }

    public static boolean isSorted(int[] a) {
        for (int i = 1; i < a.length; i++) if (a[i - 1] > a[i]) return false;
        return true;
    }

    // =========================================================
    // Benchmark "decente": warmup + best-of + alterna orden
    // =========================================================
    public static void benchProper(String title, int[] base) {
        final int WARM = 3;
        final int RUNS = 10;

        // Warmup DialSort
        for (int i = 0; i < WARM; i++) {
            int[] x = base.clone();
            sort(x);
            if (!isSorted(x)) throw new IllegalStateException("DialSort fallo en warmup");
        }

        // Warmup Arrays.sort
        for (int i = 0; i < WARM; i++) {
            int[] x = base.clone();
            Arrays.sort(x);
            if (!isSorted(x)) throw new IllegalStateException("Arrays.sort fallo en warmup");
        }

        long bestDial = Long.MAX_VALUE;
        long bestArr  = Long.MAX_VALUE;

        for (int i = 0; i < RUNS; i++) {
            // Alterna el orden para reducir sesgo de cache/JIT
            if ((i & 1) == 0) {
                bestDial = Math.min(bestDial, timeDial(base));
                bestArr  = Math.min(bestArr,  timeArr(base));
            } else {
                bestArr  = Math.min(bestArr,  timeArr(base));
                bestDial = Math.min(bestDial, timeDial(base));
            }
        }

        double msDial = bestDial / 1_000_000.0;
        double msArr  = bestArr  / 1_000_000.0;
        double speed  = (double) bestArr / (double) bestDial;

        System.out.println("=== " + title + " ===");
        System.out.printf("DialSort best : %.3f ms%n", msDial);
        System.out.printf("Arrays best   : %.3f ms%n", msArr);
        System.out.printf("Speedup       : %.3fx%n", speed);
        System.out.println();
    }

    private static long timeDial(int[] base) {
        int[] a = base.clone();
        long t0 = System.nanoTime();
        sort(a);
        long t1 = System.nanoTime();
        if (!isSorted(a)) throw new IllegalStateException("DialSort no ordeno!");
        return t1 - t0;
    }

    private static long timeArr(int[] base) {
        int[] b = base.clone();
        long t0 = System.nanoTime();
        Arrays.sort(b);
        long t1 = System.nanoTime();
        if (!isSorted(b)) throw new IllegalStateException("Arrays.sort no ordeno!");
        return t1 - t0;
    }

    // =========================================================
    // Implementación DialSort: COUNTING (mapeo+scan)
    // =========================================================
    private static void countingInPlace(int[] a, int min, int U) {
        int[] counts = new int[U];
        for (int v : a) counts[v - min]++;

        int k = 0;
        for (int y = 0; y < U; y++) {
            int c = counts[y];
            int val = y + min;
            while (c-- > 0) a[k++] = val;
        }

        if (k != a.length) {
            throw new IllegalStateException("Counting no lleno el arreglo: k=" + k + " n=" + a.length);
        }
    }

    // =========================================================
    // Implementación DialSort: RADIX (int32 signed, 4 pasadas base 256)
    // =========================================================
    private static void radixInPlace(int[] a) {
        int n = a.length;
        if (n <= 1) return;

        int[] out = new int[n];
        int[] count = new int[256];

        int[] src = a;
        int[] dst = out;

        for (int pass = 0; pass < 4; pass++) {
            Arrays.fill(count, 0);
            int shift = pass * 8;

            for (int i = 0; i < n; i++) {
                int key = src[i] ^ 0x80000000; // signed -> unsigned order
                int b = (key >>> shift) & 0xFF;
                count[b]++;
            }

            int sum = 0;
            for (int i = 0; i < 256; i++) {
                int c = count[i];
                count[i] = sum;
                sum += c;
            }

            for (int i = 0; i < n; i++) {
                int v = src[i];
                int key = v ^ 0x80000000;
                int b = (key >>> shift) & 0xFF;
                dst[count[b]++] = v;
            }

            int[] tmp = src;
            src = dst;
            dst = tmp;
        }

        if (src != a) System.arraycopy(src, 0, a, 0, n);
    }

    // =========================================================
    // Demo main
    // =========================================================
    public static void main(String[] args) {
        int n = 100_000;

        // Caso 1: rango pequeño (Counting)
        int[] small = randomBounded(n, 1000, 123);
        benchProper("SmallRange [0..999]", small);

        // Caso 2: rango completo int (Radix)
        int[] full = randomFullRange(n, 456);
        benchProper("FullRange int", full);
    }

    private static int[] randomBounded(int n, int bound, long seed) {
        Random r = new Random(seed);
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = r.nextInt(bound);
        return a;
    }

    private static int[] randomFullRange(int n, long seed) {
        Random r = new Random(seed);
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = r.nextInt();
        return a;
    }
}