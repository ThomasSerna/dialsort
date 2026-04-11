import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

/**
 * DialSort (Ordenamiento por Mapeo Espacial No-Comparativo) + Report paper-ready
 * - Adaptativo: COUNTING si U viable, si no RADIX (int32, 4 pasadas base 256)
 * - Benchmark: warmup + best-of + alterna orden
 * - Report: mode, min/max/U, ms, M elems/s, speedup vs Arrays, memoria extra estimada
 */
public class DialSortWithReport {

    // Política adaptativa (ajustable)
    private static final int MAX_U_FOR_COUNTING = 5_000_000; // counts[U] payload ~20MB
    private static final int U_TO_N_RATIO = 8;               // counting si U <= 8*n

    public enum Mode { COUNTING, RADIX }

    public static class DialSortMeta {
        public final Mode mode;
        public final int min;
        public final int max;
        public final long U;            // range = max-min+1
        public final long extraBytes;   // memoria extra estimada (payload int=4 bytes)
        DialSortMeta(Mode mode, int min, int max, long U, long extraBytes) {
            this.mode = mode; this.min = min; this.max = max; this.U = U; this.extraBytes = extraBytes;
        }
    }

    public static class DialSortReport {
        public final String title;
        public final int N;
        public final DialSortMeta dialMeta;
        public final double dialMsBest;
        public final double arraysMsBest;
        public final double dialMeps;       // M elems/s
        public final double arraysMeps;
        public final double speedupVsArrays;

        DialSortReport(String title, int n, DialSortMeta meta,
                       long dialNsBest, long arrNsBest) {
            this.title = title;
            this.N = n;
            this.dialMeta = meta;
            this.dialMsBest = dialNsBest / 1_000_000.0;
            this.arraysMsBest = arrNsBest / 1_000_000.0;
            this.dialMeps = (n / (dialNsBest / 1_000_000_000.0)) / 1_000_000.0;
            this.arraysMeps = (n / (arrNsBest / 1_000_000_000.0)) / 1_000_000.0;
            this.speedupVsArrays = (double) arrNsBest / (double) dialNsBest;
        }

        public void print() {
            System.out.println("=== " + title + " ===");
            System.out.println("Data: N=" + N + " | min=" + dialMeta.min + " max=" + dialMeta.max + " U=" + dialMeta.U);
            System.out.println();
            System.out.printf(Locale.US, "%-18s %-10s %-10s %-10s %-14s %-14s %-14s %-14s%n",
                    "Algorithm", "Mode", "ms(best)", "M elems/s", "speedup(vs Arr)", "mem extra", "Arr ms(best)", "Arr M/s");
            System.out.println("---------------------------------------------------------------------------------------------------------------");
            System.out.printf(Locale.US, "%-18s %-10s %10.3f %10.3f %14.3f %14s %14.3f %14.3f%n",
                    "DialSort", dialMeta.mode, dialMsBest, dialMeps, speedupVsArrays, formatBytes(dialMeta.extraBytes),
                    arraysMsBest, arraysMeps);
            System.out.println();
        }
    }

    // =========================================================
    // API principal
    // =========================================================
    public static DialSortMeta sortWithMeta(int[] a) {
        if (a == null) throw new IllegalArgumentException("array null");
        if (a.length <= 1) return new DialSortMeta(Mode.COUNTING, 0, 0, 1, 0);

        int min = a[0], max = a[0];
        for (int v : a) { if (v < min) min = v; if (v > max) max = v; }

        long Ulong = (long) max - (long) min + 1L;

        boolean countingOk =
                Ulong > 0 &&
                        Ulong <= Integer.MAX_VALUE &&
                        Ulong <= MAX_U_FOR_COUNTING &&
                        Ulong <= (long) U_TO_N_RATIO * (long) a.length;

        if (countingOk) {
            countingInPlace(a, min, (int) Ulong);
            long mem = bytesIntArray(Ulong); // counts[U]
            return new DialSortMeta(Mode.COUNTING, min, max, Ulong, mem);
        } else {
            radixInPlace(a);
            long mem = bytesIntArray(a.length) + bytesIntArray(256); // out[n] + count[256]
            return new DialSortMeta(Mode.RADIX, min, max, Ulong, mem);
        }
    }

    public static boolean isSorted(int[] a) {
        for (int i = 1; i < a.length; i++) if (a[i - 1] > a[i]) return false;
        return true;
    }

    // =========================================================
    // Benchmark: warmup + best-of + alterna orden
    // =========================================================
    public static DialSortReport benchWithReport(String title, int[] base) {
        final int WARM = 3;
        final int RUNS = 10;

        // Warmup DialSort
        for (int i = 0; i < WARM; i++) {
            int[] x = base.clone();
            sortWithMeta(x);
            if (!isSorted(x)) throw new IllegalStateException("DialSort fallo en warmup");
        }

        // Warmup Arrays.sort
        for (int i = 0; i < WARM; i++) {
            int[] x = base.clone();
            Arrays.sort(x);
            if (!isSorted(x)) throw new IllegalStateException("Arrays.sort fallo en warmup");
        }

        // Capturamos meta (mode/min/max/U) una vez (no afecta al ordenamiento, pero sí al reporte)
        DialSortMeta metaSnapshot = sortMetaOnly(base);

        long bestDial = Long.MAX_VALUE;
        long bestArr  = Long.MAX_VALUE;

        for (int i = 0; i < RUNS; i++) {
            if ((i & 1) == 0) {
                bestDial = Math.min(bestDial, timeDial(base));
                bestArr  = Math.min(bestArr,  timeArr(base));
            } else {
                bestArr  = Math.min(bestArr,  timeArr(base));
                bestDial = Math.min(bestDial, timeDial(base));
            }
        }

        return new DialSortReport(title, base.length, metaSnapshot, bestDial, bestArr);
    }

    private static long timeDial(int[] base) {
        int[] a = base.clone();
        long t0 = System.nanoTime();
        sortWithMeta(a);
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

    // Meta sin ordenar (para no tocar el array base)
    private static DialSortMeta sortMetaOnly(int[] base) {
        int min = base[0], max = base[0];
        for (int v : base) { if (v < min) min = v; if (v > max) max = v; }
        long Ulong = (long) max - (long) min + 1L;

        boolean countingOk =
                Ulong > 0 &&
                        Ulong <= Integer.MAX_VALUE &&
                        Ulong <= MAX_U_FOR_COUNTING &&
                        Ulong <= (long) U_TO_N_RATIO * (long) base.length;

        if (countingOk) {
            long mem = bytesIntArray(Ulong);
            return new DialSortMeta(Mode.COUNTING, min, max, Ulong, mem);
        } else {
            long mem = bytesIntArray(base.length) + bytesIntArray(256);
            return new DialSortMeta(Mode.RADIX, min, max, Ulong, mem);
        }
    }

    // =========================================================
    // COUNTING: mapeo+scan
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
        if (k != a.length) throw new IllegalStateException("Counting no lleno el arreglo: k=" + k + " n=" + a.length);
    }

    // =========================================================
    // RADIX: int32 signed, 4 pasadas base 256
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
                int key = src[i] ^ 0x80000000;
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

            int[] tmp = src; src = dst; dst = tmp;
        }

        if (src != a) System.arraycopy(src, 0, a, 0, n);
    }

    // =========================================================
    // Utils
    // =========================================================
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

    // =========================================================
    // Demo main
    // =========================================================
    public static void main(String[] args) {
        Locale.setDefault(Locale.US);

        int n = 100_000;

        int[] small = randomBounded(n, 1000, 123);
        DialSortReport r1 = benchWithReport("SmallRange [0..999]", small);
        r1.print();

        int[] full = randomFullRange(n, 456);
        DialSortReport r2 = benchWithReport("FullRange int", full);
        r2.print();
    }
}