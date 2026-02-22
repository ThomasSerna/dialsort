import java.util.*;
import java.util.concurrent.*;

public class BenchmarkDialSortsReport {

    static final int N = 100_000;
    static final int WARMUP = 2;
    static final int RUNS = 5;

    public static void main(String[] args) throws Exception {
        System.out.println("=== Benchmark: N = " + N + " ===");
        System.out.println("Warmup=" + WARMUP + ", Runs=" + RUNS);
        System.out.println("Notas:");
        System.out.println(" - Throughput = N / tiempo (M elems/s).");
        System.out.println(" - Speedup vs Arrays.sort: >1 significa que es mas rapido que Arrays.sort.");
        System.out.println(" - Memoria extra: estimada solo por arrays de int (4 bytes), sin overhead de objetos.");
        System.out.println();

        // Escenario A: rango pequeño (ideal para Counting)
        int smallRange = 1000; // 0..999
        int[] baseSmall = randomIntArrayBounded(N, smallRange, 123);

        System.out.println("ESCENARIO A) Rango pequeno: valores en [0, " + (smallRange - 1) + "]");
        runSuite(baseSmall, true);
        System.out.println();

        // Escenario B: rango grande (usar Radix)
        int[] baseLarge = randomIntArrayFullRange(N, 456);

        System.out.println("ESCENARIO B) Rango grande: int completo (incluye negativos)");
        runSuite(baseLarge, false);
        System.out.println();

        System.out.println("=== Analisis Big-O (resumen) ===");
        System.out.println("DialSort-Counting (mapeo+scan): Tiempo O(n + U), Memoria O(U)");
        System.out.println("  - U = (max-min+1). Si U es pequeno/constante => ~O(n)");
        System.out.println("DialSort-CountingParallel: Tiempo ~O(n/p + U + merge), Memoria O(p*U)");
        System.out.println("DialSort-Radix (LSD base 256): Tiempo O(k*n), Memoria O(n + 256)");
        System.out.println("  - int32 con base 256 => k=4 pasadas => ~O(4n) => O(n)");
        System.out.println("QuickSort (comparativo): Promedio O(n log n), Peor O(n^2), Pila ~O(log n)");
    }

    static void runSuite(int[] base, boolean smallRangeCase) throws Exception {
        // Pool reutilizable (importante para que CountingParallel no pierda por crear hilos)
        int p = Math.max(1, Runtime.getRuntime().availableProcessors());
        ExecutorService pool = Executors.newFixedThreadPool(p);

        // Warmup
        for (int i = 0; i < WARMUP; i++) {
            int[] w1 = base.clone();
            if (smallRangeCase) dialSortCounting(w1);
            else dialSortRadixInPlace(w1);

            int[] w2 = base.clone();
            quickSort(w2);

            int[] w3 = base.clone();
            Arrays.sort(w3);

            if (smallRangeCase) {
                int[] w4 = base.clone();
                dialSortCountingParallelSafe(w4, pool);
            }
        }

        // Para estimar memoria en scenario A calculamos U real del dataset
        Stats st = stats(base);
        long U = st.range; // max-min+1 (puede ser <= smallRange en este escenario)
        long memCounting = bytesIntArray(U);                         // counts[U]
        long memCountingPar = bytesIntArray(U) * p + bytesIntArray(U); // locals[p][U] + counts[U]
        long memRadix = bytesIntArray(base.length) + bytesIntArray(256); // out[n] + count[256]
        long memQuick = 0; // in-place; la pila no se estima en bytes exactos aquí
        long memArrays = 0; // in-place para int (Dual-Pivot Quicksort), pila O(log n)

        Result rArrays = measure("Arrays.sort", () -> {
            int[] x = base.clone();
            Arrays.sort(x);
            checkSorted(x);
        }, memArrays);

        Result rQuick = measure("QuickSort", () -> {
            int[] x = base.clone();
            quickSort(x);
            checkSorted(x);
        }, memQuick);

        Result rDial1;
        Result rDial2 = null;

        if (smallRangeCase) {
            rDial1 = measure("DialSort-Counting", () -> {
                int[] x = base.clone();
                dialSortCounting(x);
                checkSorted(x);
            }, memCounting);

            rDial2 = measure("DialSort-CountingParallel", () -> {
                int[] x = base.clone();
                dialSortCountingParallelSafe(x, pool);
                checkSorted(x);
            }, memCountingPar);

            printHeader(st, p);
            printRow(rDial1, rArrays.timeNs);
            printRow(rDial2, rArrays.timeNs);
            printRow(rQuick, rArrays.timeNs);
            printRow(rArrays, rArrays.timeNs);

        } else {
            rDial1 = measure("DialSort-Radix", () -> {
                int[] x = base.clone();
                dialSortRadixInPlace(x);
                checkSorted(x);
            }, memRadix);

            printHeader(st, p);
            printRow(rDial1, rArrays.timeNs);
            printRow(rQuick, rArrays.timeNs);
            printRow(rArrays, rArrays.timeNs);
        }

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }

    // ---------- Report helpers ----------
    static void printHeader(Stats st, int p) {
        System.out.println("Data: min=" + st.min + " max=" + st.max + " U(range)=" + st.range + " | cores(p)=" + p);
        System.out.printf(Locale.US, "%-26s %10s %14s %14s %16s%n",
                "Algoritmo", "ms", "M elems/s", "speedup(vs Arrays)", "mem extra aprox");
        System.out.println("---------------------------------------------------------------------------------------------");
    }

    static void printRow(Result r, long baselineArraysNs) {
        double ms = r.timeNs / 1_000_000.0;
        double melems = (N / (r.timeNs / 1_000_000_000.0)) / 1_000_000.0;
        double speedup = (double) baselineArraysNs / (double) r.timeNs;

        System.out.printf(Locale.US, "%-26s %10.3f %14.3f %14.3f %16s%n",
                r.name, ms, melems, speedup, formatBytes(r.extraBytes));
    }

    static Result measure(String name, Job job, long extraBytes) throws Exception {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < RUNS; i++) {
            long t0 = System.nanoTime();
            job.run();
            long t1 = System.nanoTime();
            best = Math.min(best, (t1 - t0));
        }
        return new Result(name, best, extraBytes);
    }

    interface Job { void run() throws Exception; }

    static class Result {
        String name;
        long timeNs;
        long extraBytes;
        Result(String name, long timeNs, long extraBytes) {
            this.name = name;
            this.timeNs = timeNs;
            this.extraBytes = extraBytes;
        }
    }

    static class Stats {
        int min, max;
        long range;
        Stats(int min, int max, long range) { this.min=min; this.max=max; this.range=range; }
    }

    static Stats stats(int[] a) {
        int min = a[0], max = a[0];
        for (int v : a) { if (v < min) min = v; if (v > max) max = v; }
        long range = (long)max - (long)min + 1L;
        return new Stats(min, max, range);
    }

    static long bytesIntArray(long len) {
        // solo payload: len * 4 bytes (sin overhead de objeto)
        return Math.max(0, len) * 4L;
    }

    static String formatBytes(long b) {
        if (b < 1024) return b + " B";
        double kb = b / 1024.0;
        if (kb < 1024) return String.format(Locale.US, "%.1f KB", kb);
        double mb = kb / 1024.0;
        if (mb < 1024) return String.format(Locale.US, "%.2f MB", mb);
        double gb = mb / 1024.0;
        return String.format(Locale.US, "%.2f GB", gb);
    }

    // =========================================================
    // DialSort A: Counting / Scanline (secuencial)
    // =========================================================
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

    // =========================================================
    // DialSort A (paralelo SEGURO): histograma local por hilo + merge
    // =========================================================
    static void dialSortCountingParallelSafe(int[] a, ExecutorService pool) {
        if (a.length == 0) return;

        int min = a[0], max = a[0];
        for (int v : a) { if (v < min) min = v; if (v > max) max = v; }

        long rangeLong = (long) max - (long) min + 1L;
        if (rangeLong > 5_000_000L) { // guardia: counting con U enorme no conviene
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
                for (int i = start; i < end; i++) {
                    local[a[i] - finalMin]++;
                }
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

    // =========================================================
    // DialSort B: Radix (no comparativo) int32, incluye negativos
    // =========================================================
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

    // =========================================================
    // QuickSort robusto: Hoare + median-of-3 + insertion cutoff
    // =========================================================
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

    // =========================================================
    // Datos
    // =========================================================
    static int[] randomIntArrayBounded(int n, int bound, long seed) {
        Random r = new Random(seed);
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = r.nextInt(bound);
        return a;
    }

    static int[] randomIntArrayFullRange(int n, long seed) {
        Random r = new Random(seed);
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = r.nextInt(); // incluye negativos
        return a;
    }

    // =========================================================
    // Validacion
    // =========================================================
    static void checkSorted(int[] a) {
        for (int i = 1; i < a.length; i++) {
            if (a[i - 1] > a[i]) {
                throw new IllegalStateException("Array NO ordenado en i=" + i + " (" + a[i-1] + ">" + a[i] + ")");
            }
        }
    }
}