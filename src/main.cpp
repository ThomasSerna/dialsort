#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <vector>

using i32 = int32_t;
using u32 = uint32_t;

// -------------------------------
// Política adaptativa (ajustable)
// -------------------------------
static constexpr int   MAX_U_FOR_COUNTING = 5'000'000; // bins max (payload counts ~ 20MB)
static constexpr int   U_TO_N_RATIO       = 8;         // counting si U <= 8*n

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
    for (size_t i = 1; i < a.size(); i++) if (a[i-1] > a[i]) return false;
    return true;
}

static inline uint64_t bytes_int_array(uint64_t len) { return len * 4ULL; }

static std::string fmt_bytes(uint64_t b) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1);
    if (b < 1024) return std::to_string(b) + " B";
    double kb = b / 1024.0;
    if (kb < 1024) { oss << kb << " KB"; return oss.str(); }
    double mb = kb / 1024.0;
    if (mb < 1024) { oss << std::setprecision(2) << mb << " MB"; return oss.str(); }
    double gb = mb / 1024.0;
    oss << std::setprecision(2) << gb << " GB";
    return oss.str();
}

// ------------------------------------
// DialSorter reutilizable (buffers)
// ------------------------------------
struct DialSorter {
    std::vector<i32> radixOut;
    std::array<uint32_t, 256> radixCount{};
    std::vector<uint32_t> counts;

    void ensure_radix_out(size_t n) {
        if (radixOut.size() < n) radixOut.resize(n);
    }
    void ensure_counts(size_t u) {
        if (counts.size() < u) counts.resize(u);
    }

    Meta sort_in_place(std::vector<i32>& a) {
        if (a.size() <= 1) {
            return {Mode::COUNTING, 0, 0, 1, 0};
        }

        i32 minv = a[0], maxv = a[0];
        for (i32 v : a) {
            if (v < minv) minv = v;
            if (v > maxv) maxv = v;
        }

        uint64_t U = (uint64_t)((int64_t)maxv - (int64_t)minv + 1LL);

        bool countingOk =
            U > 0 &&
            U <= (uint64_t)std::numeric_limits<int>::max() &&
            U <= (uint64_t)MAX_U_FOR_COUNTING &&
            U <= (uint64_t)U_TO_N_RATIO * (uint64_t)a.size();

        if (countingOk) {
            size_t u = (size_t)U;
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
        } else {
            ensure_radix_out(a.size());
            radix_sort_int32(a);
            // out[n] + count[256]
            uint64_t mem = bytes_int_array(a.size()) + bytes_int_array(256);
            return {Mode::RADIX, minv, maxv, U, mem};
        }
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
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
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
        std::cout << "Data: N=" << N
                  << " | min=" << meta.minv
                  << " max=" << meta.maxv
                  << " U=" << meta.U << "\n\n";

        std::cout << std::left
                  << std::setw(12) << "Algorithm"
                  << std::setw(9)  << "Mode"
                  << std::setw(12) << "ms(best)"
                  << std::setw(12) << "M elems/s"
                  << std::setw(18) << "speedup(vs sort)"
                  << std::setw(14) << "mem extra"
                  << std::setw(12) << "sort ms"
                  << std::setw(12) << "sort M/s"
                  << "\n";
        std::cout << std::string(95, '-') << "\n";

        std::cout << std::left
                  << std::setw(12) << "DialSort"
                  << std::setw(9)  << (meta.mode == Mode::COUNTING ? "COUNTING" : "RADIX")
                  << std::setw(12) << std::fixed << std::setprecision(3) << dialMs
                  << std::setw(12) << std::fixed << std::setprecision(3) << dialMeps
                  << std::setw(18) << std::fixed << std::setprecision(3) << speedup
                  << std::setw(14) << fmt_bytes(meta.extraBytes)
                  << std::setw(12) << std::fixed << std::setprecision(3) << sortMs
                  << std::setw(12) << std::fixed << std::setprecision(3) << sortMeps
                  << "\n\n";
    }
};

static Meta meta_only(const std::vector<i32>& base) {
    i32 minv = base[0], maxv = base[0];
    for (i32 v : base) { if (v < minv) minv = v; if (v > maxv) maxv = v; }
    uint64_t U = (uint64_t)((int64_t)maxv - (int64_t)minv + 1LL);

    bool countingOk =
        U > 0 &&
        U <= (uint64_t)std::numeric_limits<int>::max() &&
        U <= (uint64_t)MAX_U_FOR_COUNTING &&
        U <= (uint64_t)U_TO_N_RATIO * (uint64_t)base.size();

    if (countingOk) return {Mode::COUNTING, minv, maxv, U, bytes_int_array(U)};
    uint64_t mem = bytes_int_array(base.size()) + bytes_int_array(256);
    return {Mode::RADIX, minv, maxv, U, mem};
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

    std::cout << "Warming... (" << title << ")\n";

    for (int i = 0; i < WARM; i++) { workDial = base; sorter.sort_in_place(workDial); }
    for (int i = 0; i < WARM; i++) { workSort = base; std::sort(workSort.begin(), workSort.end()); }

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

// ---------------------------
// Main demo
// ---------------------------
int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    size_t n = 100'000;

    auto small = random_bounded(n, 1000, 123);
    bench_with_report("SmallRange [0..999]", small).print();

    auto full = random_full_range(n, 456);
    bench_with_report("FullRange int32", full).print();

    return 0;
}