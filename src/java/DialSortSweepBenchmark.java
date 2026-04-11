import java.util.*;
import java.util.concurrent.*;

/**
 * DialSortSweepBenchmark
 * - Sweep sobre N y U (rango acotado) para comparar:
 *   DialSort-Counting, DialSort-CountingParallel, QuickSort, Arrays.sort
 * - Sweep sobre N (rango int completo) para comparar:
 *   DialSort-Radix, QuickSort, Arrays.sort
 *
 * Output: CSV (ideal para Excel / paper).
 */
public class DialSortSweepBenchmark {

    // =======================
    // Ajustes del experimento
    // =======================
    static final int[] Ns = {10_000, 100_000, 1_000_000};
    static final int[] Us = {256, 1024, 10_000, 1_000_000};

    static final int WARMUP = 2;   // repeticiones de calentamiento
    static final int RUNS = 5;     // repeticiones medidas (se reporta "best")

    static final long SEED_BASE = 12345L;

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.US);

        int p = Math.max(1, Runtime.getRuntime().availableProcessors());
        ExecutorService pool = Executors.newFixedThreadPool(p);

        System.out.println("# cores(p)=" + p + " | Warmup=" + WARMUP + " Runs=" + RUNS);
        System.out.println("# CSV columns:");
        System.out.println("scenario,N,U,algorithm,ms,Melements_per_s,speedup_vs_Arrays,mem_extra_bytes");

        // =========================
        // 1) Sweep rango acotado U
        // =========================
        for (int n : Ns) {
            for (int u : Us) {
                long seed = SEED_BASE + 31L * n + u;

                int[] base = randomBounded(n, u, seed);
                Stats st = stats(base);
                long Ureal = st.range; // ~u

                // Warmup
                warmupBounded(base, pool);

                // Baseline Arrays.sort
                long tArrays = measureSortBest(base, Sorter.ARRAYS_SORT, pool);

                // DialSort Counting (secuencial)
                long tCount = measureSortBest(base, Sorter.DIAL_COUNTING, pool);

                // DialSort Counting paralelo (seguro)
                long tCountPar = measureSortBest(base, Sorter.DIAL_COUNTING_PAR, pool);

                // QuickSort
                long tQuick = measureSortBest(base, Sorter.QUICK_SORT, pool);

                // Memoria extra (payload int=4 bytes, sin overhead)
                long memCount = bytesIntArray(Ureal);
                long memCountPar = bytesIntArray(Ureal) * p + bytesIntArray(Ureal); // locals + merge counts
                long memQuick = 0;
                long memArrays = 0;

                // Report CSV
                printCsv("bounded", n, u, "DialSort-Counting", tCount, tArrays, memCount);
                printCsv("bounded", n, u, "DialSort-CountingParallel", tCountPar, tArrays, memCountPar);
                printCsv("bounded", n, u, "QuickSort", tQuick, tArrays, memQuick);
                printCsv("bounded", n, u, "Arrays.sort", tArrays, tArrays, memArrays);

                // Separador “humano”
                System.out.println("# ----");
            }
        }

        // =========================
        // 2) Sweep rango int completo
        // =========================
        for (int n : Ns) {
            long seed = SEED_BASE + 999_999L + n;

            int[] base = randomFullRange(n, seed);
            Stats st = stats(base);

            warmupFullRange(base);

            long tArrays = measureSortBest(base, Sorter.ARRAYS_SORT, pool);
            long tRadix = measureSortBest(base, Sorter.DIAL_RADIX, pool);
            long tQuick = measureSortBest(base, Sorter.QUICK_SORT, pool);

            long memRadix = bytesIntArray(n) + bytesIntArray(256); // out[n] + count[256]
            long memQuick = 0;
            long memArrays = 0;

            // aquí U “no aplica” (vacío) => ponemos -1
            printCsv("full_range", n, -1, "DialSort-Radix", tRadix, tArrays, memRadix);
            printCsv("full_range", n, -1, "QuickSort", tQuick, tArrays, memQuick);
            printCsv("full_range", n, -1, "Arrays.sort", tArrays, tArrays, memArrays);

            System.out.println("# ----");
            System.out.println("# full_range stats: N=" + n + " min=" + st.min + " max=" + st.max + " U=" + st.range);
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
    }

    // =======================
    // CSV printing helpers
    // =======================
    static void printCsv(String scenario, int n, int u, String algo, long timeNs, long baselineArraysNs, long memBytes) {
        double ms = timeNs / 1_000_000.0;
        double melems = (n / (timeNs / 1_000_000_000.0)) / 1_000_000.0;
        double speedup = (double) baselineArraysNs / (double) timeNs;

        System.out.printf(Locale.US,
                "%s,%d,%d,%s,%.6f,%.6f,%.6f,%d%n",
                scenario, n, u, algo, ms, melems, speedup, memBytes
        );
    }

    static long bytesIntArray(long len) { return Math.max(0, len) * 4L; }

    // =======================
    // Measure (best-of RUNS)
    // =======================
    static long measureSortBest(int[] base, Sorter sorter, ExecutorService pool) throws Exception {
        long best = Long.MAX_VALUE;

        for (int i = 0; i < RUNS; i++) {
            int[] x = base.clone();

            long t0 = System.nanoTime();
            sorter.sort(x, pool);
            long t1 = System.nanoTime();

            // Validación fuera del tiempo medido
            checkSorted(x);

            best = Math.min(best, (t1 - t0));
        }
        return best;
    }

    // =======================
    // Warmups
    // =======================
    static void warmupBounded(int[] base, ExecutorService pool) throws Exception {
        for (int i = 0; i < WARMUP; i++) {
            int[] a1 = base.clone(); dialSortCounting(a1);
            int[] a2 = base.clone(); dialSortCountingParallelSafe(a2, pool);
            int[] a3 = base.clone(); quickSort(a3);
            int[] a4 = base.clone(); Arrays.sort(a4);
        }
    }

    static void warmupFullRange(int[] base) {
        for (int i = 0; i < WARMUP; i++) {
            int[] a1 = base.clone(); dialSortRadixInPlace(a1);
            int[] a2 = base.clone(); quickSort(a2);
            int[] a3 = base.clone(); Arrays.sort(a3);
        }
    }

    // =======================
    // Sorter enum
    // =======================
    enum Sorter {
        DIAL_COUNTING {
            @Override void sort(int[] a, ExecutorService pool) { dialSortCounting(a); }
        },
        DIAL_COUNTING_PAR {
            @Override void sort(int[] a, ExecutorService pool) { dialSortCountingParallelSafe(a, pool); }
        },
        DIAL_RADIX {
            @Override void sort(int[] a, ExecutorService pool) { dialSortRadixInPlace(a); }
        },
        QUICK_SORT {
            @Override void sort(int[] a, ExecutorService pool) { quickSort(a); }
        },
        ARRAYS_SORT {
            @Override void sort(int[] a, ExecutorService pool) { Arrays.sort(a); }
        };

        abstract void sort(int[] a, ExecutorService pool) throws Exception;
    }

    // =======================
    // DialSort Counting
    // =======================
    static void dialSortCounting(int[] a) {
        if (a.length == 0) return;

        int min = a[0], max = a[0];
        for (int v : a) { if (v < min) min = v; if (v > max) max = v; }

        long rangeLong = (long) max - (long) min + 1L;
        if (rangeLong > Integer.MAX_VALUE) {
            dialSortRadixInPlace(a);
            return;
        }
        int range = (int) rangeLong;

        int[] counts = new int[range];
        for (int v : a) counts[v - min]++;

        int k = 0;
        for (int y = 0; y < range; y++) {
            int c = counts[y];
            int val = y + min;
            while (c-- > 0) a[k++] = val;
        }
        if (k != a.length) throw new IllegalStateException("Counting no lleno el arreglo: k=" + k + " n=" + a.length);
    }

    // =======================
    // DialSort Counting Parallel Safe (rieles)
    // =======================
    static void dialSortCountingParallelSafe(int[] a, ExecutorService pool) {
        if (a.length == 0) return;

        int min = a[0], max = a[0];
        for (int v : a) { if (v < min) min = v; if (v > max) max = v; }

        long rangeLong = (long) max - (long) min + 1L;

        // Si U gigantesco, counting no conviene
        if (rangeLong > 5_000_000L) {
            dialSortRadixInPlace(a);
            return;
        }
        int range = (int) rangeLong;

        int n = a.length;
        int p = Math.max(1, Runtime.getRuntime().availableProcessors());
        int chunk = (n + p - 1) / p;

        int[][] locals = new int[p][range];
        CountDownLatch latch = new CountDownLatch(p);

        for (int t = 0; t < p; t++) {
            final int tid = t;
            final int start = t * chunk;
            final int end = Math.min(n, start + chunk);

            int finalMin = min;
            pool.execute(() -> {
                int[] local = locals[tid];
                for (int i = start; i < end; i++) local[a[i] - finalMin]++;
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        int[] counts = new int[range];
        for (int t = 0; t < p; t++) {
            int[] local = locals[t];
            for (int y = 0; y < range; y++) counts[y] += local[y];
        }

        int k = 0;
        for (int y = 0; y < range; y++) {
            int c = counts[y];
            int val = y + min;
            while (c-- > 0) a[k++] = val;
        }
        if (k != a.length) throw new IllegalStateException("CountingParallel no lleno el arreglo: k=" + k + " n=" + a.length);
    }

    // =======================
    // DialSort Radix (int32, signed)
    // =======================
    static void dialSortRadixInPlace(int[] a) {
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

    // =======================
    // QuickSort robusto
    // =======================
    static void quickSort(int[] a) {
        if (a.length <= 1) return;
        quickSort(a, 0, a.length - 1);
    }

    static void quickSort(int[] a, int lo, int hi) {
        while (lo < hi) {
            if (hi - lo < 24) {
                insertionSort(a, lo, hi);
                return;
            }
            int p = partitionHoareMedian3(a, lo, hi);

            if (p - lo < hi - p) {
                quickSort(a, lo, p);
                lo = p + 1;
            } else {
                quickSort(a, p + 1, hi);
                hi = p;
            }
        }
    }

    static int partitionHoareMedian3(int[] a, int lo, int hi) {
        int mid = lo + ((hi - lo) >>> 1);
        int pivot = median(a[lo], a[mid], a[hi]);

        int i = lo - 1;
        int j = hi + 1;

        while (true) {
            do { i++; } while (a[i] < pivot);
            do { j--; } while (a[j] > pivot);
            if (i >= j) return j;
            swap(a, i, j);
        }
    }

    static int median(int a, int b, int c) {
        if (a < b) {
            if (b < c) return b;
            return (a < c) ? c : a;
        } else {
            if (a < c) return a;
            return (b < c) ? c : b;
        }
    }

    static void insertionSort(int[] a, int lo, int hi) {
        for (int i = lo + 1; i <= hi; i++) {
            int key = a[i];
            int j = i - 1;
            while (j >= lo && a[j] > key) {
                a[j + 1] = a[j];
                j--;
            }
            a[j + 1] = key;
        }
    }

    static void swap(int[] a, int i, int j) {
        int t = a[i]; a[i] = a[j]; a[j] = t;
    }

    // =======================
    // Data generation
    // =======================
    static int[] randomBounded(int n, int bound, long seed) {
        Random r = new Random(seed);
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = r.nextInt(bound);
        return a;
    }

    static int[] randomFullRange(int n, long seed) {
        Random r = new Random(seed);
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = r.nextInt();
        return a;
    }

    // =======================
    // Stats + validation
    // =======================
    static class Stats {
        int min, max;
        long range;
        Stats(int min, int max, long range) { this.min = min; this.max = max; this.range = range; }
    }

    static Stats stats(int[] a) {
        int min = a[0], max = a[0];
        for (int v : a) { if (v < min) min = v; if (v > max) max = v; }
        long range = (long) max - (long) min + 1L;
        return new Stats(min, max, range);
    }

    static void checkSorted(int[] a) {
        for (int i = 1; i < a.length; i++) {
            if (a[i - 1] > a[i]) {
                throw new IllegalStateException("Array NO ordenado en i=" + i + " (" + a[i - 1] + ">" + a[i] + ")");
            }
        }
    }
}