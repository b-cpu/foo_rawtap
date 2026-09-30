#define VISUALIZATION_MANAGER_VERSION

#ifdef VISUALIZATION_MANAGER_VERSION

// foo_rawtap - "Raw Sample Tap" (optimised revision)
//
// Streams the float32 PCM that is audible at this moment (visualisation
// stream) to the named pipe \\.\pipe\foobar2000_rawtap while a client is
// connected. The wire format is unchanged: a 20-byte packet_header followed
// by frames * channels interleaved float samples.
//
// Changes relative to the previous revision:
//   1. No timer, no visualisation stream and no work at all while no client
//      is connected (the worker thread tells the main thread when to poll).
//   2. The polling rate drops while playback is stopped or paused.
//   3. Lock-free single-producer/single-consumer ring: the main thread never
//      takes a lock and never waits on the worker thread.
//   4. The pipe is written directly from the ring (no staging copy and no
//      64 KiB scratch buffer).
//   5. One pipe instance is reused across clients, so the pipe name never
//      disappears between two connections.
//   6. FlushFileBuffers removed: it could block shutdown indefinitely.
//   7. std::atomic replaces volatile; correct on ARM64 as well as x64.
//   8. Builds whose audio_sample is double are supported: samples are narrowed
//      to float32 directly into the ring (the wire format stays float32).

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>

#pragma comment(lib, "shared-x64")

#ifdef _DEBUG
#pragma comment(lib, "foobar2000_SDKd")
#pragma comment(lib, "pfcd")
#pragma comment(lib, "foobar2000_component_clientd")
#else
#pragma comment(lib, "foobar2000_SDK")
#pragma comment(lib, "pfc")
#pragma comment(lib, "foobar2000_component_client")
#endif

#include <foobar2000/SDK/foobar2000.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>

DECLARE_COMPONENT_VERSION("Raw Sample Tap", "2.0.0",
    "Streams the float32 PCM that is audible at this moment (visualisation stream) to the named pipe\n"
    "\\\\.\\pipe\\foobar2000_rawtap while audio is playing.");

VALIDATE_COMPONENT_FILENAME("foo_rawtap.dll");

namespace {

    const wchar_t kPipeName[] = L"\\\\.\\pipe\\foobar2000_rawtap";
    const uint32_t kMagic = 0x53524246u;              // 'F','B','R','S'
    const size_t   kRingBytes = 4u * 1024u * 1024u;   // roughly 11 s of 44.1 kHz stereo float
    static_assert((kRingBytes& (kRingBytes - 1)) == 0, "ring size must be power of two");
    const size_t   kRingMask = kRingBytes - 1;
    const size_t   kMaxWrite = 256u * 1024u;          // upper bound for one WriteFile

    // Posted by the worker thread to the message window whenever a client
    // connects or disconnects. The handler re-reads the state, so the message
    // carries no payload and bursts of them are harmless.
    const UINT     kMsgLink = WM_APP + 1;

    // The SDK selects audio_sample (float or double) at build time. The wire
    // format is always float32, so a double build converts while copying into
    // the ring; a float build copies the samples unchanged.
    static_assert(sizeof(audio_sample) == sizeof(float) || sizeof(audio_sample) == sizeof(double),
        "audio_sample must be float or double");

#pragma pack(push, 1)
    struct packet_header {
        uint32_t magic;
        uint32_t sample_rate;
        uint32_t channels;
        uint32_t channel_mask;
        uint32_t frames;
    };
#pragma pack(pop)

    // ---------------------------------------------------------------------------
    // tap_sink: SPSC ring buffer plus the pipe-serving worker thread.
    //
    // Producer: the main thread (audible_tap::tick). Consumer: the worker thread.
    // m_write is stored only by the producer, m_read only by the consumer, and
    // both are monotonic byte counters (offset = counter & kRingMask).
    // ---------------------------------------------------------------------------
    class tap_sink {
    public:
        tap_sink()
            : m_write(0), m_read(0), m_connected(false), m_sleeping(false), m_stop(false)
            , m_notify(NULL), m_thread(NULL), m_dataEvt(NULL), m_stopEvt(NULL), m_ioEvt(NULL) {
        }

        ~tap_sink() {
            stop();
        }

        // 'notify' is the window that receives kMsgLink; it must outlive the thread.
        bool start(HWND notify) {
            if (m_thread) return true;

            m_ring.reset(new (std::nothrow) uint8_t[kRingBytes]);
            if (!m_ring) return false;

            m_notify = notify;
            m_write.store(0);
            m_read.store(0);
            m_connected.store(false);
            m_sleeping.store(false);
            m_stop.store(false);

            m_dataEvt = CreateEventW(NULL, FALSE, FALSE, NULL);  // auto-reset
            m_stopEvt = CreateEventW(NULL, TRUE, FALSE, NULL);   // manual-reset
            m_ioEvt = CreateEventW(NULL, TRUE, FALSE, NULL);     // manual-reset
            if (m_dataEvt && m_stopEvt && m_ioEvt) {
                m_thread = CreateThread(NULL, 0, &tap_sink::thread_proc, this, 0, NULL);
            }
            if (!m_thread) {
                close_events();
                m_ring.reset();
                return false;
            }
            return true;
        }

        void stop() {
            if (!m_thread) return;
            m_stop.store(true, std::memory_order_relaxed);
            SetEvent(m_stopEvt);
            if (WaitForSingleObject(m_thread, 5000) == WAIT_OBJECT_0) {
                CloseHandle(m_thread);
                m_thread = NULL;
                close_events();
                m_ring.reset();
            } else {
                // The worker did not exit in time. Freeing what it still uses
                // would turn a delay into memory corruption, so leak it.
                m_thread = NULL;
                m_dataEvt = m_stopEvt = m_ioEvt = NULL;
                m_ring.release();
            }
        }

        // Cheap check for the main thread. One acquire load, no lock.
        bool wants_data() const {
            return m_connected.load(std::memory_order_acquire);
        }

        // Header and payload are published together, so packets never interleave.
        // 'count' is the number of samples (frames * channels); each occupies
        // four bytes on the wire whatever the type of audio_sample is.
        // Returns false if no client is connected or the ring is full (packet dropped).
        bool push(const void* hdr, size_t hn, const audio_sample* samples, size_t count) {
            if (!m_connected.load(std::memory_order_acquire)) return false;

            const size_t   dn = count * sizeof(float);
            const size_t   total = hn + dn;
            const uint64_t w = m_write.load(std::memory_order_relaxed);
            const uint64_t r = m_read.load(std::memory_order_acquire);
            if (kRingBytes - (size_t)(w - r) < total) return false;

            copy_in(w, hdr, hn);
            copy_samples(w + hn, samples, count);

            // seq_cst on this store and on the m_sleeping load pairs with the
            // consumer's seq_cst store/load in stream(): at least one side
            // observes the other, so a wake-up can never be lost.
            m_write.store(w + total, std::memory_order_seq_cst);
            if (m_sleeping.load(std::memory_order_seq_cst)) SetEvent(m_dataEvt);
            return true;
        }

    private:
        static DWORD WINAPI thread_proc(LPVOID p) {
            static_cast<tap_sink*>(p)->run();
            return 0;
        }

        void close_events() {
            if (m_dataEvt) CloseHandle(m_dataEvt);
            if (m_stopEvt) CloseHandle(m_stopEvt);
            if (m_ioEvt)   CloseHandle(m_ioEvt);
            m_dataEvt = m_stopEvt = m_ioEvt = NULL;
        }

        void notify() {
            if (m_notify) PostMessageW(m_notify, kMsgLink, 0, 0);
        }

        // ---- ring buffer, producer side ----
        void copy_in(uint64_t pos, const void* src, size_t n) {
            const size_t off = (size_t)pos & kRingMask;
            const size_t first = std::min(n, kRingBytes - off);
            memcpy(m_ring.get() + off, src, first);
            if (n > first) memcpy(m_ring.get(), static_cast<const uint8_t*>(src) + first, n - first);
        }

        // audio_sample is float: the samples are already in wire format.
        void copy_samples(uint64_t pos, const float* src, size_t count) {
            copy_in(pos, src, count * sizeof(float));
        }

        // audio_sample is double: narrow to float in small blocks. The block
        // lives on the stack (4 KiB, resident in L1), the loop is trivially
        // vectorisable, and going through copy_in keeps the wrap-around logic
        // in one place and avoids type-punning the byte ring.
        template <class T>
        void copy_samples(uint64_t pos, const T* src, size_t count) {
            float tmp[1024];
            while (count) {
                const size_t k = std::min(count, sizeof tmp / sizeof tmp[0]);
                for (size_t i = 0; i < k; ++i) tmp[i] = static_cast<float>(src[i]);
                copy_in(pos, tmp, k * sizeof(float));
                pos += k * sizeof(float);
                src += k;
                count -= k;
            }
        }

        // ---- pipe I/O (worker thread only) ----
        bool wait_for_client(HANDLE pipe) {
            OVERLAPPED ov = {};
            ov.hEvent = m_ioEvt;
            ResetEvent(m_ioEvt);

            if (ConnectNamedPipe(pipe, &ov)) return true;
            switch (GetLastError()) {
            case ERROR_PIPE_CONNECTED:
                return true;
            case ERROR_IO_PENDING: {
                HANDLE h[2] = { m_ioEvt, m_stopEvt };
                DWORD dummy = 0;
                if (WaitForMultipleObjects(2, h, FALSE, INFINITE) == WAIT_OBJECT_0)
                    return GetOverlappedResult(pipe, &ov, &dummy, FALSE) != FALSE;
                CancelIoEx(pipe, &ov);
                GetOverlappedResult(pipe, &ov, &dummy, TRUE);
                return false;
            }
            default:
                return false;
            }
        }

        bool write_all(HANDLE pipe, const uint8_t* p, DWORD n) {
            OVERLAPPED ov = {};
            ov.hEvent = m_ioEvt;
            ResetEvent(m_ioEvt);
            DWORD written = 0;
            if (!WriteFile(pipe, p, n, NULL, &ov)) {
                if (GetLastError() != ERROR_IO_PENDING) return false;
                HANDLE h[2] = { m_ioEvt, m_stopEvt };
                if (WaitForMultipleObjects(2, h, FALSE, INFINITE) != WAIT_OBJECT_0) {
                    CancelIoEx(pipe, &ov);
                    GetOverlappedResult(pipe, &ov, &written, TRUE);
                    return false;
                }
            }
            if (!GetOverlappedResult(pipe, &ov, &written, FALSE)) return false;
            return written == n;
        }

        void begin_session() {
            // Discard leftovers of an earlier session. Only this thread moves
            // m_read, and nothing new is published while m_connected is false.
            m_read.store(m_write.load(std::memory_order_acquire), std::memory_order_release);
            m_sleeping.store(false, std::memory_order_relaxed);
            m_connected.store(true, std::memory_order_release);
            notify();
        }

        void end_session() {
            m_connected.store(false, std::memory_order_release);
            notify();
        }

        // Drains the ring straight into the pipe. Bytes stay in the ring until
        // the write has completed, so the producer cannot overwrite them.
        void stream(HANDLE pipe) {
            HANDLE h[2] = { m_dataEvt, m_stopEvt };
            uint64_t r = m_read.load(std::memory_order_relaxed);

            for (;;) {
                if (m_stop.load(std::memory_order_relaxed)) return;

                const uint64_t w = m_write.load(std::memory_order_acquire);
                if (w == r) {
                    // Announce the intention to sleep, then look once more.
                    m_sleeping.store(true, std::memory_order_seq_cst);
                    if (m_write.load(std::memory_order_seq_cst) == r) {
                        const DWORD rc = WaitForMultipleObjects(2, h, FALSE, INFINITE);
                        m_sleeping.store(false, std::memory_order_relaxed);
                        if (rc != WAIT_OBJECT_0) return;
                    } else {
                        m_sleeping.store(false, std::memory_order_relaxed);
                    }
                    continue;
                }

                const size_t off = (size_t)r & kRingMask;
                const size_t n = std::min(std::min((size_t)(w - r), kRingBytes - off), kMaxWrite);
                if (!write_all(pipe, m_ring.get() + off, (DWORD)n)) return;   // client left, or stopping
                r += n;
                m_read.store(r, std::memory_order_release);
            }
        }

        void run() {
            HANDLE pipe = INVALID_HANDLE_VALUE;

            while (!m_stop.load(std::memory_order_relaxed)) {
                if (pipe == INVALID_HANDLE_VALUE) {
                    pipe = CreateNamedPipeW(
                        kPipeName,
                        PIPE_ACCESS_OUTBOUND | FILE_FLAG_OVERLAPPED,
                        PIPE_TYPE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                        1, 64 * 1024, 0, 0, NULL);
                    if (pipe == INVALID_HANDLE_VALUE) {
                        if (WaitForSingleObject(m_stopEvt, 1000) == WAIT_OBJECT_0) break;
                        continue;
                    }
                }

                if (!wait_for_client(pipe)) {
                    // Stopping, or the instance became unusable: discard it and retry.
                    CloseHandle(pipe);
                    pipe = INVALID_HANDLE_VALUE;
                    if (WaitForSingleObject(m_stopEvt, 250) == WAIT_OBJECT_0) break;
                    continue;
                }

                begin_session();
                stream(pipe);
                end_session();

                // Return the same instance to the listening state. Deliberately
                // no FlushFileBuffers: it waits for the client to drain the pipe.
                DisconnectNamedPipe(pipe);
            }

            if (pipe != INVALID_HANDLE_VALUE) CloseHandle(pipe);
        }

        std::unique_ptr<uint8_t[]> m_ring;
        std::atomic<uint64_t>      m_write;       // producer-owned
        std::atomic<uint64_t>      m_read;        // consumer-owned
        std::atomic<bool>          m_connected;
        std::atomic<bool>          m_sleeping;    // consumer is (about to be) blocked on m_dataEvt
        std::atomic<bool>          m_stop;
        HWND                       m_notify;
        HANDLE                     m_thread, m_dataEvt, m_stopEvt, m_ioEvt;
    };

    tap_sink g_sink;

    // ---------------------------------------------------------------------------
    // audible_tap: polls the visualisation stream on the main thread, but only
    // while a client is connected.
    // ---------------------------------------------------------------------------
    const UINT_PTR kTimerId = 1;
    const UINT     kActiveMs = 10;       // Windows rounds this to about 15.6 ms
    const UINT     kIdleMs = 50;         // poll period while stopped or paused
    const unsigned kStillTicks = 8;      // empty ticks in a row that mean "paused"
    const double   kBacklogSec = 2.0;    // history requested from the stream
    const double   kMaxCatchUp = 1.5;    // never look further back than this
    const double   kMinRequest = 0.005;  // do not request chunks shorter than this
    const double   kMaxRequest = 0.5;    // upper bound for a single request
    // On (re)synchronisation start this far behind "now". It must exceed the
    // longest possible poll period (kIdleMs after rounding to the system tick),
    // otherwise the first audible samples after a stop would be skipped.
    const double   kResyncLookback = 0.1;

    class audible_tap {
    public:
        audible_tap()
            : m_hwnd(NULL), m_polling(false), m_have(false), m_logged(false)
            , m_still(0), m_curMs(0), m_next(0.0) {
        }

        bool start() {
            WNDCLASSW wc;
            memset(&wc, 0, sizeof wc);
            wc.lpfnWndProc = &audible_tap::wnd_proc;
            wc.hInstance = core_api::get_my_instance();
            wc.lpszClassName = L"foo_rawtap_msgwnd";
            RegisterClassW(&wc);
            m_hwnd = CreateWindowExW(0, wc.lpszClassName, L"", 0, 0, 0, 0, 0,
                HWND_MESSAGE, NULL, wc.hInstance, NULL);
            if (!m_hwnd) return false;
            SetWindowLongPtrW(m_hwnd, GWLP_USERDATA, (LONG_PTR)this);
            return true;                       // no timer until a client connects
        }

        void stop() {
            if (!m_hwnd) return;
            stop_polling();
            DestroyWindow(m_hwnd);
            UnregisterClassW(L"foo_rawtap_msgwnd", core_api::get_my_instance());
            m_hwnd = NULL;
        }

        HWND hwnd() const {
            return m_hwnd;
        }

    private:
        static LRESULT CALLBACK wnd_proc(HWND h, UINT msg, WPARAM w, LPARAM l) {
            audible_tap* self = (audible_tap*)GetWindowLongPtrW(h, GWLP_USERDATA);
            if (self) {
                if (msg == WM_TIMER && w == kTimerId) {
                    self->tick(); return 0;
                }
                if (msg == kMsgLink) {
                    self->sync_link(); return 0;
                }
            }
            return DefWindowProcW(h, msg, w, l);
        }

        // ---- polling lifecycle (main thread) ----
        void sync_link() {
            if (g_sink.wants_data()) start_polling();
            else                     stop_polling();
        }

        void start_polling() {
            if (m_polling) return;
            m_polling = true;
            m_have = false;
            m_logged = false;
            m_still = 0;
            m_curMs = 0;
            set_interval(kActiveMs);
        }

        void stop_polling() {
            if (!m_polling) return;
            m_polling = false;
            KillTimer(m_hwnd, kTimerId);
            m_curMs = 0;
            m_have = false;
            m_stream.release();    // the core stops buffering audio for us
        }

        void set_interval(UINT ms) {
            if (m_curMs == ms) return;
            m_curMs = ms;
            SetTimer(m_hwnd, kTimerId, ms, NULL);   // same id: replaces the running timer
        }

        bool ensure_stream() {
            if (m_stream.is_valid()) return true;
            try {
                visualisation_manager::get()->create_stream(m_stream, 0);
            } catch (...) {
                return false;
            }
            if (m_stream.is_empty()) return false;
            service_ptr_t<visualisation_stream_v2> v2;
            if (m_stream->service_query_t(v2)) {
                v2->request_backlog(kBacklogSec);
                FB2K_console_formatter() << "foo_rawtap 2.0.0 (visualisation stream): stream created, backlog requested";
            } else {
                FB2K_console_formatter() << "foo_rawtap 2.0.0 (visualisation stream): stream created, no backlog control";
            }
            return true;
        }

        void tick() {
            // KillTimer does not remove a WM_TIMER that is already queued.
            if (!m_polling) return;
            // The client left but its notification has not been processed yet.
            if (!g_sink.wants_data()) {
                stop_polling(); return;
            }
            if (!ensure_stream()) return;

            double now = 0.0;
            if (!m_stream->get_absolute_time(now)) {   // stopped
                m_have = false;
                set_interval(kIdleMs);
                return;
            }

            // First tick, new playback, or a seek that reset the clock.
            if (!m_have || now < m_next) {
                m_next = now > kResyncLookback ? now - kResyncLookback : 0.0;
                m_have = true;
                m_still = 0;
            }

            double gap = now - m_next;
            if (gap > kMaxCatchUp) {
                m_next = now - kMaxCatchUp;
                gap = kMaxCatchUp;
            }
            if (gap < kMinRequest) {                   // paused, or the timer fired early
                if (m_still < kStillTicks && ++m_still == kStillTicks) set_interval(kIdleMs);
                return;
            }
            m_still = 0;
            set_interval(kActiveMs);
            if (gap > kMaxRequest) gap = kMaxRequest;

            if (!m_stream->get_chunk_absolute(m_chunk, m_next, gap)) return;

            const size_t   frames = m_chunk.get_sample_count();
            const unsigned ch = m_chunk.get_channel_count();
            const unsigned rate = m_chunk.get_sample_rate();
            if (frames == 0 || ch == 0 || rate == 0) return;

            const size_t n = frames * ch;

            packet_header h;
            h.magic = kMagic;
            h.sample_rate = rate;
            h.channels = ch;
            h.channel_mask = m_chunk.get_channel_config();
            h.frames = (uint32_t)frames;

            // Interleaved samples; the sink writes them as float32 whatever
            // the SDK's audio_sample type is.
            g_sink.push(&h, sizeof h, m_chunk.get_data(), n);

            if (!m_logged) {
                m_logged = true;
                FB2K_console_formatter() << "foo_rawtap: first audible chunk sent: "
                    << (unsigned)rate << " Hz, " << (unsigned)ch << " ch, "
                    << (unsigned)frames << " frames";
            }

            // Advance by what was actually delivered, so consecutive requests abut.
            m_next += (double)frames / (double)rate;
        }

        HWND                                m_hwnd;
        service_ptr_t<visualisation_stream> m_stream;
        bool                                m_polling;  // timer is armed
        bool                                m_have;
        bool                                m_logged;   // first-chunk message already written
        unsigned                            m_still;    // consecutive ticks without new audio
        UINT                                m_curMs;    // currently armed timer period
        double                              m_next;     // absolute time of the next sample wanted
        audio_chunk_impl                    m_chunk;    // reused every tick, avoids realloc
    };

    audible_tap g_tap;

    class rawtap_initquit : public initquit {
    public:
        void on_init() override {
            // The window must exist before the worker starts: the worker posts to it.
            if (!g_tap.start() || !g_sink.start(g_tap.hwnd())) {
                FB2K_console_formatter() << "foo_rawtap: failed to start";
            }
        }
        void on_quit() override {
            // Reverse order: after the worker has joined nothing posts to the window.
            g_sink.stop();
            g_tap.stop();
        }
    };
    initquit_factory_t<rawtap_initquit> g_initquit_factory;

} // namespace
#else

// foo_rawtap.cpp
//
// foobar2000 DSP component that copies every audio chunk passing through the
// DSP chain to a Windows named pipe as raw 32-bit float PCM.
//
// Pipe name : \\.\pipe\foobar2000_rawtap
// Wire format (little-endian), repeated for every chunk:
//     uint32 magic        'FBRS' (0x53524246)
//     uint32 sample_rate  Hz
//     uint32 channels
//     uint32 channel_mask foobar2000 channel configuration bitmask
//     uint32 frames       number of sample frames in this packet
//     float  data[frames * channels]   interleaved, nominal range -1.0 .. +1.0
//
// Design constraints:
//   * on_chunk() runs on the playback thread and must never block. It only
//     copies into a ring buffer under a very short critical section.
//   * A worker thread owns the pipe, waits for a client, and drains the ring.
//   * If no client is connected, on_chunk() returns immediately (zero cost).
//   * If the client is too slow, whole packets are dropped rather than
//     stalling playback.

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>

#pragma comment(lib, "shared-x64")

#ifdef _DEBUG
#pragma comment(lib, "foobar2000_SDKd")
#pragma comment(lib, "pfcd")
#pragma comment(lib, "foobar2000_component_clientd")
#else
#pragma comment(lib, "foobar2000_SDK")
#pragma comment(lib, "pfc")
#pragma comment(lib, "foobar2000_component_client")
#endif

#include <foobar2000/SDK/foobar2000.h>

#include <cstdint>
#include <cstring>
#include <memory>

DECLARE_COMPONENT_VERSION("Raw Sample Tap", "1.0.0",
    "Streams raw float32 PCM from the DSP chain to the named pipe\n"
    "\\\\.\\pipe\\foobar2000_rawtap while audio is playing.");

VALIDATE_COMPONENT_FILENAME("foo_rawtap.dll");

namespace {

    const wchar_t kPipeName[] = L"\\\\.\\pipe\\foobar2000_rawtap";
    const uint32_t kMagic = 0x53524246u;          // 'F','B','R','S'
    const size_t   kRingBytes = 4u * 1024u * 1024u;   // roughly 11 s of 44.1 kHz stereo float
    static_assert((kRingBytes& (kRingBytes - 1)) == 0, "ring size must be power of two");
    const size_t   kRingMask = kRingBytes - 1;

    static_assert(sizeof(audio_sample) == sizeof(float),
        "This component requires 32-bit float audio_sample");

#pragma pack(push, 1)
    struct packet_header {
        uint32_t magic;
        uint32_t sample_rate;
        uint32_t channels;
        uint32_t channel_mask;
        uint32_t frames;
    };
#pragma pack(pop)

    // ---------------------------------------------------------------------------
    // tap_sink: ring buffer plus the pipe-serving worker thread.
    // ---------------------------------------------------------------------------
    class tap_sink {
    public:
        tap_sink()
            : m_ring(new uint8_t[kRingBytes])
            , m_tmp(new uint8_t[64 * 1024])
            , m_head(0), m_used(0), m_connected(0)
            , m_stop(false), m_thread(NULL)
            , m_dataEvt(NULL), m_stopEvt(NULL), m_ioEvt(NULL) {
            InitializeCriticalSection(&m_cs);
        }

        ~tap_sink() {
            stop();
            DeleteCriticalSection(&m_cs);
        }

        void start() {
            if (m_thread) return;
            m_stop = false;
            m_dataEvt = CreateEventW(NULL, FALSE, FALSE, NULL);  // auto-reset
            m_stopEvt = CreateEventW(NULL, TRUE, FALSE, NULL);   // manual-reset
            m_ioEvt = CreateEventW(NULL, TRUE, FALSE, NULL);   // manual-reset
            m_thread = CreateThread(NULL, 0, &tap_sink::thread_proc, this, 0, NULL);
        }

        void stop() {
            if (!m_thread) return;
            m_stop = true;
            SetEvent(m_stopEvt);
            WaitForSingleObject(m_thread, 5000);
            CloseHandle(m_thread);
            CloseHandle(m_dataEvt);
            CloseHandle(m_stopEvt);
            CloseHandle(m_ioEvt);
            m_thread = m_dataEvt = m_stopEvt = m_ioEvt = NULL;
        }

        // Cheap check performed on the playback thread before doing any work.
        // Aligned 32-bit reads are atomic on x86/x64; no interlocked op needed.
        bool wants_data() const {
            return m_connected != 0;
        }

        // Header and payload are enqueued atomically so packets never interleave.
        void push(const void* hdr, size_t hn, const void* data, size_t dn) {
            if (!m_connected) return;    // fast path, no lock taken

            bool ok = false;
            bool was_empty = false;

            EnterCriticalSection(&m_cs);
            if (m_connected && m_used + hn + dn <= kRingBytes) {
                was_empty = (m_used == 0);
                ring_write(hdr, hn);
                ring_write(data, dn);
                ok = true;
            }
            LeaveCriticalSection(&m_cs);

            if (ok && was_empty) SetEvent(m_dataEvt);
        }

    private:
        static DWORD WINAPI thread_proc(LPVOID p) {
            static_cast<tap_sink*>(p)->run();
            return 0;
        }

        // ---- ring buffer (caller holds m_cs) ----
        void ring_write(const void* src, size_t n) {
            const uint8_t* s = static_cast<const uint8_t*>(src);
            size_t tail = (m_head + m_used) & kRingMask;
            size_t first = kRingBytes - tail;
            if (first > n) first = n;
            memcpy(&m_ring[tail], s, first);
            if (n > first) memcpy(&m_ring[0], s + first, n - first);
            m_used += n;
        }

        size_t ring_read(uint8_t* out, size_t max) {
            EnterCriticalSection(&m_cs);
            size_t n = m_used < max ? m_used : max;
            size_t first = kRingBytes - m_head;
            if (first > n) first = n;
            memcpy(out, &m_ring[m_head], first);
            if (n > first) memcpy(out + first, &m_ring[0], n - first);
            m_head = (m_head + n) & kRingMask;
            m_used -= n;
            LeaveCriticalSection(&m_cs);
            return n;
        }

        void ring_reset_and_set_connected(bool connected) {
            EnterCriticalSection(&m_cs);
            m_head = 0;
            m_used = 0;
            m_connected = connected ? 1 : 0;
            LeaveCriticalSection(&m_cs);
        }

        // ---- pipe I/O (worker thread only) ----
        bool write_all(HANDLE pipe, const uint8_t* p, DWORD n) {
            OVERLAPPED ov = {};
            ov.hEvent = m_ioEvt;
            ResetEvent(m_ioEvt);
            DWORD written = 0;
            if (!WriteFile(pipe, p, n, NULL, &ov)) {
                if (GetLastError() != ERROR_IO_PENDING) return false;
                HANDLE h[2] = { m_ioEvt, m_stopEvt };
                if (WaitForMultipleObjects(2, h, FALSE, INFINITE) != WAIT_OBJECT_0) {
                    CancelIoEx(pipe, &ov);
                    GetOverlappedResult(pipe, &ov, &written, TRUE);
                    return false;
                }
            }
            if (!GetOverlappedResult(pipe, &ov, &written, FALSE)) return false;
            return written == n;
        }

        void stream(HANDLE pipe) {
            HANDLE h[2] = { m_dataEvt, m_stopEvt };
            for (;;) {
                size_t n = ring_read(m_tmp.get(), 64 * 1024);
                if (n == 0) {
                    if (WaitForMultipleObjects(2, h, FALSE, INFINITE) != WAIT_OBJECT_0) return;
                    continue;
                }
                if (!write_all(pipe, m_tmp.get(), (DWORD)n)) return;   // client left
            }
        }

        void run() {
            while (!m_stop) {
                HANDLE pipe = CreateNamedPipeW(
                    kPipeName,
                    PIPE_ACCESS_OUTBOUND | FILE_FLAG_OVERLAPPED,
                    PIPE_TYPE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                    1, 64 * 1024, 0, 0, NULL);
                if (pipe == INVALID_HANDLE_VALUE) {
                    if (WaitForSingleObject(m_stopEvt, 1000) == WAIT_OBJECT_0) break;
                    continue;
                }

                OVERLAPPED ov = {};
                ov.hEvent = m_ioEvt;
                ResetEvent(m_ioEvt);

                bool connected = false;
                if (ConnectNamedPipe(pipe, &ov)) {
                    connected = true;
                } else {
                    DWORD e = GetLastError();
                    if (e == ERROR_PIPE_CONNECTED) {
                        connected = true;
                    } else if (e == ERROR_IO_PENDING) {
                        HANDLE h[2] = { m_ioEvt, m_stopEvt };
                        if (WaitForMultipleObjects(2, h, FALSE, INFINITE) == WAIT_OBJECT_0) {
                            connected = true;
                        } else {
                            DWORD dummy;
                            CancelIoEx(pipe, &ov);
                            GetOverlappedResult(pipe, &ov, &dummy, TRUE);
                        }
                    }
                }

                if (connected) {
                    ring_reset_and_set_connected(true);
                    stream(pipe);
                    ring_reset_and_set_connected(false);
                    FlushFileBuffers(pipe);
                }
                DisconnectNamedPipe(pipe);
                CloseHandle(pipe);
            }
        }

        std::unique_ptr<uint8_t[]> m_ring;
        std::unique_ptr<uint8_t[]> m_tmp;
        size_t                     m_head;
        size_t                     m_used;
        volatile LONG              m_connected;
        volatile bool              m_stop;
        CRITICAL_SECTION           m_cs;
        HANDLE                     m_thread, m_dataEvt, m_stopEvt, m_ioEvt;
    };

    tap_sink g_sink;

    // Starts and stops the worker thread with foobar2000 itself.
    class rawtap_initquit : public initquit {
    public:
        void on_init() override {
            g_sink.start();
        }
        void on_quit() override {
            g_sink.stop();
        }
    };
    initquit_factory_t<rawtap_initquit> g_initquit_factory;

    // ---------------------------------------------------------------------------
    // The DSP: passes audio through unmodified and mirrors it to the sink.
    // ---------------------------------------------------------------------------
    class dsp_rawtap : public dsp_impl_base {
    public:
        dsp_rawtap() {
        }

        static GUID g_get_guid() {
            // {6C1D3B52-8E47-4A0B-9F35-2B7A5D91C0E4}
            static const GUID guid =
            { 0x6c1d3b52, 0x8e47, 0x4a0b, { 0x9f, 0x35, 0x2b, 0x7a, 0x5d, 0x91, 0xc0, 0xe4 } };
            return guid;
        }

        static void g_get_name(pfc::string_base& out) {
            out = "Raw sample tap (named pipe)";
        }

        bool on_chunk(audio_chunk* chunk, abort_callback&) override {
            if (!g_sink.wants_data()) return true;

            const size_t   frames = chunk->get_sample_count();
            const unsigned ch = chunk->get_channel_count();
            if (frames == 0 || ch == 0) return true;

            const size_t n = frames * ch;

            packet_header h;
            h.magic = kMagic;
            h.sample_rate = chunk->get_sample_rate();
            h.channels = ch;
            h.channel_mask = chunk->get_channel_config();
            h.frames = (uint32_t)frames;

            // audio_sample is float, so the interleaved data is shipped as-is
            // with no per-chunk scratch buffer or conversion copy.
            g_sink.push(&h, sizeof h, chunk->get_data(), n * sizeof(float));
            return true;   // keep the chunk in the chain, unmodified
        }

        void on_endofplayback(abort_callback&) override {
        }
        void on_endoftrack(abort_callback&) override {
        }
        void flush() override {
        }
        double get_latency() override {
            return 0;
        }
        bool need_track_change_mark() override {
            return false;
        }
    };

    dsp_factory_nopreset_t<dsp_rawtap> g_dsp_factory;

} // namespace

#endif