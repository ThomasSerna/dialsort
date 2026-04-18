#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using i32 = int32_t;
using u32 = uint32_t;

// -------------------------------
// Política adaptativa (ajustable)
// -------------------------------
static constexpr int   MAX_U_FOR_COUNTING = 5'000'000; // bins max (payload counts ~ 20MB)
static constexpr int   U_TO_N_RATIO       = 8;         // counting si U <= 8*n

// Paralelización counting
static constexpr size_t MIN_ITEMS_FOR_PARALLEL_COUNTING = 100'000;
static constexpr size_t PARALLEL_COUNTING_THREADS       = 8;
static constexpr uint64_t MAX_PARALLEL_COUNTING_BYTES   = 256ULL * 1024ULL * 1024ULL;

// Benchmark config
static constexpr int WARM = 3;
static constexpr int RUNS = 10;

enum class Mode { COUNTING, RADIX };

struct Meta {
    Mode mode;
    i32 minv;
    i32 maxv;
    uint64_t U;          // range = max-min+1
    uint64_t extraBytes; // memoria extra estimada (payload)
};

static inline bool is_sorted_nondec(const std::vector<i32>& a) {
    for (size_t i = 1; i < a.size(); i++) if (a[i - 1] > a[i]) return false;
    return true;
}

static inline uint64_t bytes_int_array(uint64_t len) { return len * 4ULL; }

static std::string fmt_bytes(uint64_t b) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1);
    if (b < 1024) return std::to_string(b) + " B";
    double kb = b / 1024.0;
    if (kb < 1024) {
        oss << kb << " KB";
        return oss.str();
    }
    double mb = kb / 1024.0;
    if (mb < 1024) {
        oss << std::setprecision(2) << mb << " MB";
        return oss.str();
    }
    double gb = mb / 1024.0;
    oss << std::setprecision(2) << gb << " GB";
    return oss.str();
}

struct RangeInfo {
    i32 minv;
    i32 maxv;
    uint64_t U;
};

static RangeInfo range_of(const std::vector<i32>& a) {
    i32 minv = a[0], maxv = a[0];
    for (i32 v : a) {
        if (v < minv) minv = v;
        if (v > maxv) maxv = v;
    }
    uint64_t U = (uint64_t)((int64_t)maxv - (int64_t)minv + 1LL);
    return {minv, maxv, U};
}

static bool counting_ok(uint64_t U, size_t n) {
    return U > 0 &&
           U <= (uint64_t)std::numeric_limits<int>::max() &&
           U <= (uint64_t)MAX_U_FOR_COUNTING &&
           U <= (uint64_t)U_TO_N_RATIO * (uint64_t)n;
}

static size_t choose_parallel_counting_threads(size_t n, size_t u) {
    if (n < MIN_ITEMS_FOR_PARALLEL_COUNTING) return 1;

    const uint64_t localCountsBytes =
        (uint64_t)PARALLEL_COUNTING_THREADS * bytes_int_array((uint64_t)u);
    if (localCountsBytes > MAX_PARALLEL_COUNTING_BYTES) return 1;

    return PARALLEL_COUNTING_THREADS;
}

// ------------------------------------
// DialSorter reutilizable (buffers)
// ------------------------------------
struct DialSorter {
    std::vector<i32> radixOut;
    std::array<uint32_t, 256> radixCount{};
    std::vector<uint32_t> counts;

    std::mutex countingMutex;
    std::condition_variable countingTaskCv;
    std::condition_variable countingDoneCv;
    std::vector<std::thread> countingWorkers;

    const std::vector<i32>* countingInput = nullptr;
    i32 countingMinv = 0;
    size_t countingU = 0;
    size_t countingBase = 0;
    size_t countingExtra = 0;
    uint32_t* countingLocalCounts = nullptr;
    size_t countingActiveWorkers = 0;
    size_t countingCompletedWorkers = 0;
    uint64_t countingGeneration = 0;
    bool stopCountingWorkers = false;

    ~DialSorter() { shutdown_counting_workers(); }

    void ensure_radix_out(size_t n) {
        if (radixOut.size() < n) radixOut.resize(n);
    }
    void ensure_counts(size_t u) {
        if (counts.size() < u) counts.resize(u);
    }

    void ensure_counting_workers(size_t threads) {
        while (countingWorkers.size() < threads) {
            const size_t workerId = countingWorkers.size();
            countingWorkers.emplace_back([this, workerId]() { counting_worker_loop(workerId); });
        }
    }

    void shutdown_counting_workers() {
        {
            std::lock_guard<std::mutex> lock(countingMutex);
            stopCountingWorkers = true;
        }
        countingTaskCv.notify_all();
        for (auto& worker : countingWorkers) {
            if (worker.joinable()) worker.join();
        }
        countingWorkers.clear();
    }

    void counting_worker_loop(size_t workerId) {
        uint64_t seenGeneration = 0;

        while (true) {
            const std::vector<i32>* input = nullptr;
            i32 minv = 0;
            size_t u = 0;
            size_t left = 0;
            size_t right = 0;

            {
                std::unique_lock<std::mutex> lock(countingMutex);
                countingTaskCv.wait(lock, [&]() {
                    return stopCountingWorkers || countingGeneration != seenGeneration;
                });
                if (stopCountingWorkers) return;

                seenGeneration = countingGeneration;
                if (workerId >= countingActiveWorkers) continue;

                input = countingInput;
                minv = countingMinv;
                u = countingU;
                left = workerId * countingBase + std::min(workerId, countingExtra);
                right = left + countingBase + (workerId < countingExtra ? 1 : 0);
            }

            uint32_t* local = countingLocalCounts + workerId * u;
            for (size_t i = left; i < right; i++) {
                local[(size_t)((*input)[i] - minv)]++;
            }

            {
                std::lock_guard<std::mutex> lock(countingMutex);
                ++countingCompletedWorkers;
                if (countingCompletedWorkers == countingActiveWorkers) {
                    countingDoneCv.notify_one();
                }
            }
        }
    }

    std::vector<uint32_t> run_parallel_counting(std::vector<i32>& a, i32 minv, size_t u, size_t threads) {
        ensure_counting_workers(threads);
        std::vector<uint32_t> localCounts(threads * u, 0);

        {
            std::lock_guard<std::mutex> lock(countingMutex);
            countingInput = &a;
            countingMinv = minv;
            countingU = u;
            countingBase = a.size() / threads;
            countingExtra = a.size() % threads;
            countingLocalCounts = localCounts.data();
            countingActiveWorkers = threads;
            countingCompletedWorkers = 0;
            ++countingGeneration;
        }

        countingTaskCv.notify_all();

        std::unique_lock<std::mutex> lock(countingMutex);
        countingDoneCv.wait(lock, [&]() {
            return countingCompletedWorkers == countingActiveWorkers;
        });

        return localCounts;
    }

    uint64_t counting_extra_bytes(size_t u, size_t n) const {
        const size_t threads = choose_parallel_counting_threads(n, u);
        return bytes_int_array((uint64_t)u) * (uint64_t)threads;
    }

    Meta counting_sort_in_place(std::vector<i32>& a, i32 minv, i32 maxv, uint64_t U) {
        const size_t u = (size_t)U;
        const size_t n = a.size();
        const size_t threads = choose_parallel_counting_threads(n, u);

        if (threads <= 1) {
            ensure_counts(u);
            std::fill(counts.begin(), counts.begin() + u, 0);

            for (i32 v : a) counts[(size_t)(v - minv)]++;

            size_t k = 0;
            for (size_t y = 0; y < u; y++) {
                uint32_t c = counts[y];
                i32 val = (i32)y + minv;
                while (c--) a[k++] = val;
            }
            if (k != a.size()) throw std::runtime_error("Counting did not fill output.");

            return {Mode::COUNTING, minv, maxv, U, bytes_int_array(U)};
        }

        std::vector<uint32_t> localCounts = run_parallel_counting(a, minv, u, threads);

        ensure_counts(u);
        for (size_t y = 0; y < u; y++) {
            uint32_t sum = 0;
            for (size_t t = 0; t < threads; t++) {
                sum += localCounts[t * u + y];
            }
            counts[y] = sum;
        }

        size_t k = 0;
        for (size_t y = 0; y < u; y++) {
            uint32_t c = counts[y];
            i32 val = (i32)y + minv;
            while (c--) a[k++] = val;
        }
        if (k != a.size()) throw std::runtime_error("Parallel counting did not fill output.");

        return {Mode::COUNTING, minv, maxv, U, bytes_int_array((uint64_t)u) * (uint64_t)threads};
    }

    Meta sort_in_place(std::vector<i32>& a) {
        if (a.size() <= 1) {
            return {Mode::COUNTING, 0, 0, 1, 0};
        }

        const RangeInfo info = range_of(a);
        if (counting_ok(info.U, a.size())) {
            return counting_sort_in_place(a, info.minv, info.maxv, info.U);
        }

        ensure_radix_out(a.size());
        radix_sort_int32(a);
        // out[n] + count[256]
        uint64_t mem = bytes_int_array(a.size()) + bytes_int_array(256);
        return {Mode::RADIX, info.minv, info.maxv, info.U, mem};
    }

    // Radix LSD base 256, 4 pasadas, signed-safe
    void radix_sort_int32(std::vector<i32>& a) {
        const size_t n = a.size();
        if (n <= 1) return;

        std::vector<i32>* src = &a;
        std::vector<i32>* dst = &radixOut;

        for (int pass = 0; pass < 4; pass++) {
            radixCount.fill(0);
            int shift = pass * 8;

            for (size_t i = 0; i < n; i++) {
                u32 key = (u32)((*src)[i] ^ (i32)0x80000000);
                u32 b = (key >> shift) & 0xFFu;
                radixCount[b]++;
            }

            uint32_t sum = 0;
            for (int i = 0; i < 256; i++) {
                uint32_t c = radixCount[i];
                radixCount[i] = sum;
                sum += c;
            }

            dst->resize(n);
            for (size_t i = 0; i < n; i++) {
                i32 v = (*src)[i];
                u32 key = (u32)(v ^ (i32)0x80000000);
                u32 b = (key >> shift) & 0xFFu;
                (*dst)[radixCount[b]++] = v;
            }

            std::swap(src, dst);
        }

        if (src != &a) {
            a.assign(src->begin(), src->end());
        }
    }
};

// ---------------------------
// Data generation
// ---------------------------
static std::vector<i32> random_bounded(size_t n, int bound, uint64_t seed) {
    std::mt19937 rng((uint32_t)seed);
    std::uniform_int_distribution<int> dist(0, bound - 1);
    std::vector<i32> a(n);
    for (size_t i = 0; i < n; i++) a[i] = (i32)dist(rng);
    return a;
}

static std::vector<i32> random_full_range(size_t n, uint64_t seed) {
    std::mt19937 rng((uint32_t)seed);
    std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int32_t>::min(),
                                                std::numeric_limits<int32_t>::max());
    std::vector<i32> a(n);
    for (size_t i = 0; i < n; i++) a[i] = dist(rng);
    return a;
}

// ---------------------------
// Benchmark helpers
// ---------------------------
static inline uint64_t now_ns() {
    return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct Report {
    std::string title;
    size_t N;
    Meta meta;
    uint64_t dialBestNs;
    uint64_t sortBestNs;

    void print() const {
        double dialMs = dialBestNs / 1'000'000.0;
        double sortMs = sortBestNs / 1'000'000.0;
        double dialMeps = (N / (dialBestNs / 1e9)) / 1e6;
        double sortMeps = (N / (sortBestNs / 1e9)) / 1e6;
        double speedup = (double)sortBestNs / (double)dialBestNs;

        std::cout << "=== " << title << " ===\n";
        std::cout << "Data: N=" << N << " | min=" << meta.minv << " max=" << meta.maxv << " U=" << meta.U
                  << "\n\n";

        std::cout << std::left << std::setw(12) << "Algorithm" << std::setw(9) << "Mode" << std::setw(12)
                  << "ms(best)" << std::setw(12) << "M elems/s" << std::setw(18) << "speedup(vs sort)"
                  << std::setw(14) << "mem extra" << std::setw(12) << "sort ms" << std::setw(12) << "sort M/s"
                  << "\n";
        std::cout << std::string(95, '-') << "\n";

        std::cout << std::left << std::setw(12) << "DialSort" << std::setw(9)
                  << (meta.mode == Mode::COUNTING ? "COUNTING" : "RADIX") << std::setw(12) << std::fixed
                  << std::setprecision(3) << dialMs << std::setw(12) << std::fixed << std::setprecision(3)
                  << dialMeps << std::setw(18) << std::fixed << std::setprecision(3) << speedup << std::setw(14)
                  << fmt_bytes(meta.extraBytes) << std::setw(12) << std::fixed << std::setprecision(3) << sortMs
                  << std::setw(12) << std::fixed << std::setprecision(3) << sortMeps << "\n\n";
    }
};

static Meta meta_only(const std::vector<i32>& base) {
    const RangeInfo info = range_of(base);
    if (counting_ok(info.U, base.size())) {
        const size_t threads = choose_parallel_counting_threads(base.size(), (size_t)info.U);
        return {Mode::COUNTING, info.minv, info.maxv, info.U,
                bytes_int_array(info.U) * (uint64_t)threads};
    }
    uint64_t mem = bytes_int_array(base.size()) + bytes_int_array(256);
    return {Mode::RADIX, info.minv, info.maxv, info.U, mem};
}

static uint64_t time_dial(DialSorter& sorter, const std::vector<i32>& base, std::vector<i32>& work) {
    work = base;
    uint64_t t0 = now_ns();
    sorter.sort_in_place(work);
    uint64_t t1 = now_ns();
    if (!is_sorted_nondec(work)) throw std::runtime_error("DialSort failed.");
    return t1 - t0;
}

static uint64_t time_sort(const std::vector<i32>& base, std::vector<i32>& work) {
    work = base;
    uint64_t t0 = now_ns();
    std::sort(work.begin(), work.end());
    uint64_t t1 = now_ns();
    if (!is_sorted_nondec(work)) throw std::runtime_error("std::sort failed.");
    return t1 - t0;
}

static Report bench_with_report(const std::string& title, const std::vector<i32>& base) {
    DialSorter sorter;
    std::vector<i32> workDial, workSort;
    workDial.reserve(base.size());
    workSort.reserve(base.size());

    // std::cout << "Warming... (" << title << ")\n";

    for (int i = 0; i < WARM; i++) {
        workDial = base;
        sorter.sort_in_place(workDial);
    }
    for (int i = 0; i < WARM; i++) {
        workSort = base;
        std::sort(workSort.begin(), workSort.end());
    }

    Meta meta = meta_only(base);

    uint64_t bestDial = std::numeric_limits<uint64_t>::max();
    uint64_t bestSort = std::numeric_limits<uint64_t>::max();

    for (int i = 0; i < RUNS; i++) {
        if ((i & 1) == 0) {
            bestDial = std::min(bestDial, time_dial(sorter, base, workDial));
            bestSort = std::min(bestSort, time_sort(base, workSort));
        } else {
            bestSort = std::min(bestSort, time_sort(base, workSort));
            bestDial = std::min(bestDial, time_dial(sorter, base, workDial));
        }
    }

    return {title, base.size(), meta, bestDial, bestSort};
}

static void testParallel(size_t n, int q)
{
    for (int i = 0; i < q; ++i)
    {
        auto small = random_bounded(n, 1000, 123);
        std::cout << bench_with_report("SmallRange [0..999]", small).dialBestNs/ 1'000'000.0 << std::endl;
    }
}

// ---------------------------
// Main demo
// ---------------------------
int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    size_t n = 10'000'000;

    auto small = random_bounded(n, 1000, 123);
    bench_with_report("SmallRange [0..999]", small).print();
    auto full = random_full_range(n, 456);
    bench_with_report("FullRange int32", full).print();

    return 0;
}