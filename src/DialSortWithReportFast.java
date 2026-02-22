import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

/**
 * DialSort + Report (paper-ready) - versión FAST:
 * - Reutiliza buffers (sin GC pesado)
 * - No usa clone() dentro del timing: usa System.arraycopy a un buffer de trabajo
 * - Warmup + best-of + alterna orden
 */
public class DialSortWithReportFast {

    // Política adaptativa
    private static final int MAX_U_FOR_COUNTING = 5_000_000; // counts[U] payload ~20MB
    private static final int U_TO_N_RATIO = 8;               // counting si U <= 8*n

    // Benchmark config
    private static final int WARM = 3;
    private static final int RUNS = 10;

    public enum Mode { COUNTING, RADIX }

    public static class DialSortMeta {
        public final Mode mode;
        public final int min;
        public final int max;
        public final long U;
        public final long extraBytes;
        DialSortMeta(Mode mode, int min, int max, long U, long extraBytes) {
            this.mode = mode; this.min = min; this.max = max; this.U = U; this.extraBytes = extraBytes;
        }
    }

    public static class DialSortReport {
        public final String title;
        public final int N;
        public final DialSortMeta meta;
        public final long dialNsBest;
        public final long arrNsBest;

        DialSortReport(String title, int n, DialSortMeta meta, long dialNsBest, long arrNsBest) {
            this.title = title;
            this.N = n;
            this.meta = meta;
            this.dialNsBest = dialNsBest;
            this.arrNsBest = arrNsBest;
        }

        public void print() {
            double dialMs = dialNsBest / 1_000_000.0;
            double arrMs  = arrNsBest  / 1_000_000.0;
            double dialMeps = (N / (dialNsBest / 1_000_000_000.0)) / 1_000_000.0;
            double arrMeps  = (N / (arrNsBest  / 1_000_000_000.0)) / 1_000_000.0;
            double speedup = (double) arrNsBest / (double) dialNsBest;

            System.out.println("=== " + title + " ===");
            System.out.println("Data: N=" + N + " | min=" + meta.min + " max=" + meta.max + " U=" + meta.U);
            System.out.println();
            System.out.printf(Locale.US,
                    "%-12s %-9s %10s %12s %16s %14s %12s %12s%n",
                    "Algorithm", "Mode", "ms(best)", "M elems/s", "speedup(vs Arr)", "mem extra", "Arr ms", "Arr M/s");
            System.out.println("----------------------------------------------------------------------------------------------------------");
            System.out.printf(Locale.US,
                    "%-12s %-9s %10.3f %12.3f %16.3f %14s %12.3f %12.3f%n",
                    "DialSort", meta.mode, dialMs, dialMeps, speedup, formatBytes(meta.extraBytes), arrMs, arrMeps);
            System.out.println();
        }
    }

    /**
     * Objeto DialSorter reutilizable (buffers persistentes).
     */
    public static class DialSorter {
        private int[] counts = new int[0];     // para counting
        private int[] radixOut = new int[0];   // para radix
        private final int[] radixCount = new int[256];

        public DialSortMeta sortInPlace(int[] a) {
            if (a.length <= 1) return new DialSortMeta(Mode.COUNTING, 0, 0, 1, 0);

            int min = a[0], max = a[0];
            for (int v : a) { if (v < min) min = v; if (v > max) max = v; }
            long U = (long) max - (long) min + 1L;

            boolean countingOk =
                    U > 0 &&
                            U <= Integer.MAX_VALUE &&
                            U <= MAX_U_FOR_COUNTING &&
                            U <= (long) U_TO_N_RATIO * (long) a.length;

            if (countingOk) {
                int u = (int) U;
                ensureCounts(u);
                Arrays.fill(counts, 0, u, 0);
                for (int v : a) counts[v - min]++;

                int k = 0;
                for (int y = 0; y < u; y++) {
                    int c = counts[y];
                    int val = y + min;
                    while (c-- > 0) a[k++] = val;
                }
                if (k != a.length) throw new IllegalStateException("Counting no lleno: k=" + k + " n=" + a.length);

                long mem = bytesIntArray(u); // counts[u]
                return new DialSortMeta(Mode.COUNTING, min, max, U, mem);
            } else {
                ensureRadixOut(a.length);
                radixInPlace(a);
                long mem = bytesIntArray(a.length) + bytesIntArray(256); // out[n] + count[256]
                return new DialSortMeta(Mode.RADIX, min, max, U, mem);
            }
        }

        private void ensureCounts(int u) {
            if (counts.length < u) counts = new int[u];
        }

        private void ensureRadixOut(int n) {
            if (radixOut.length < n) radixOut = new int[n];
        }

        // Radix LSD base 256, 4 pasadas, signed-safe
        private void radixInPlace(int[] a) {
            int n = a.length;
            if (n <= 1) return;

            int[] src = a;
            int[] dst = radixOut;

            for (int pass = 0; pass < 4; pass++) {
                Arrays.fill(radixCount, 0);
                int shift = pass * 8;

                for (int i = 0; i < n; i++) {
                    int key = src[i] ^ 0x80000000;
                    radixCount[(key >>> shift) & 0xFF]++;
                }

                int sum = 0;
                for (int i = 0; i < 256; i++) {
                    int c = radixCount[i];
                    radixCount[i] = sum;
                    sum += c;
                }

                for (int i = 0; i < n; i++) {
                    int v = src[i];
                    int key = v ^ 0x80000000;
                    int b = (key >>> shift) & 0xFF;
                    dst[radixCount[b]++] = v;
                }

                int[] tmp = src; src = dst; dst = tmp;
            }

            if (src != a) System.arraycopy(src, 0, a, 0, n);
        }
    }

    // -------------------------
    // Benchmark (fast, sin GC)
    // -------------------------
    public static DialSortReport benchWithReport(String title, int[] base) {
        DialSorter sorter = new DialSorter();
        int n = base.length;

        int[] workDial = new int[n];
        int[] workArr  = new int[n];

        System.out.println("Warming... (" + title + ")");

        // Warmup DialSort
        for (int i = 0; i < WARM; i++) {
            System.arraycopy(base, 0, workDial, 0, n);
            sorter.sortInPlace(workDial);
            if (!isSorted(workDial)) throw new IllegalStateException("DialSort fallo warmup");
        }

        // Warmup Arrays.sort
        for (int i = 0; i < WARM; i++) {
            System.arraycopy(base, 0, workArr, 0, n);
            Arrays.sort(workArr);
            if (!isSorted(workArr)) throw new IllegalStateException("Arrays.sort fallo warmup");
        }

        // Meta snapshot (solo stats + politica, sin ordenar base)
        DialSortMeta meta = metaOnly(base);

        long bestDial = Long.MAX_VALUE;
        long bestArr  = Long.MAX_VALUE;

        for (int i = 0; i < RUNS; i++) {
            if ((i & 1) == 0) {
                bestDial = Math.min(bestDial, timeDial(sorter, base, workDial));
                bestArr  = Math.min(bestArr,  timeArr(base, workArr));
            } else {
                bestArr  = Math.min(bestArr,  timeArr(base, workArr));
                bestDial = Math.min(bestDial, timeDial(sorter, base, workDial));
            }
        }

        return new DialSortReport(title, n, meta, bestDial, bestArr);
    }

    private static long timeDial(DialSorter sorter, int[] base, int[] work) {
        System.arraycopy(base, 0, work, 0, base.length);
        long t0 = System.nanoTime();
        sorter.sortInPlace(work);
        long t1 = System.nanoTime();
        if (!isSorted(work)) throw new IllegalStateException("DialSort no ordeno!");
        return t1 - t0;
    }

    private static long timeArr(int[] base, int[] work) {
        System.arraycopy(base, 0, work, 0, base.length);
        long t0 = System.nanoTime();
        Arrays.sort(work);
        long t1 = System.nanoTime();
        if (!isSorted(work)) throw new IllegalStateException("Arrays.sort no ordeno!");
        return t1 - t0;
    }

    // -------------------------
    // Meta sin ordenar
    // -------------------------
    private static DialSortMeta metaOnly(int[] base) {
        int min = base[0], max = base[0];
        for (int v : base) { if (v < min) min = v; if (v > max) max = v; }
        long U = (long) max - (long) min + 1L;

        boolean countingOk =
                U > 0 &&
                        U <= Integer.MAX_VALUE &&
                        U <= MAX_U_FOR_COUNTING &&
                        U <= (long) U_TO_N_RATIO * (long) base.length;

        if (countingOk) {
            long mem = bytesIntArray(U);
            return new DialSortMeta(Mode.COUNTING, min, max, U, mem);
        } else {
            long mem = bytesIntArray(base.length) + bytesIntArray(256);
            return new DialSortMeta(Mode.RADIX, min, max, U, mem);
        }
    }

    // -------------------------
    // Utils
    // -------------------------
    private static boolean isSorted(int[] a) {
        for (int i = 1; i < a.length; i++) if (a[i - 1] > a[i]) return false;
        return true;
    }

    private static long bytesIntArray(long len) { return Math.max(0, len) * 4L; }

    private static String formatBytes(long b) {
        if (b < 1024) return b + " B";
        double kb = b / 1024.0;
        if (kb < 1024) return String.format(Locale.US, "%.1f KB", kb);
        double mb = kb / 1024.0;
        if (mb < 1024) return String.format(Locale.US, "%.2f MB", mb);
        double gb = mb / 1024.0;
        return String.format(Locale.US, "%.2f GB", gb);
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

    // -------------------------
    // Demo main
    // -------------------------
    public static void main(String[] args) {
        Locale.setDefault(Locale.US);

        int n = 100_000;

        int[] small = randomBounded(n, 1000, 123);
        benchWithReport("SmallRange [0..999]", small).print();

        int[] full = randomFullRange(n, 456);
        benchWithReport("FullRange int", full).print();
    }
}