// foo_rawtap - "Raw Sample Tap" Foobar2000 Plugin (optimised revision)
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

// =============================================================================
// OVERVIEW OF THE DESIGN
// =============================================================================
//
// Data flow:
//
//   foobar2000 playback core
//        |   audio that is audible right now (time-aligned to the output)
//        v
//   visualisation_stream
//        |   get_chunk_absolute(): polled by a timer on the MAIN thread
//        v
//   audible_tap::tick()
//        |   tap_sink::push(): header + float32 samples, no locks
//        v
//   SPSC ring buffer (4 MiB, lock-free)
//        |   WriteFile() straight from the ring, on the WORKER thread
//        v
//   named pipe \\.\pipe\foobar2000_rawtap
//        v
//   client process (for example a visualiser) reading the byte stream
//
// Threads:
//
//   Main thread   Owns the hidden message window and the timer. It is the only
//                 thread that touches the visualisation stream, because that
//                 service is intended for main-thread use. It is the ring's
//                 single PRODUCER and must never block, because a stalled main
//                 thread freezes the foobar2000 user interface.
//   Worker thread Owns the pipe. It is the ring's single CONSUMER. All the
//                 blocking calls (connect, write) live here, so a slow or
//                 absent client can never delay playback or the UI.
//
// Coordination between the two threads:
//
//   * The ring's two counters and a few std::atomic flags (see tap_sink).
//   * The worker posts a private window message (kMsgLink) to the main thread
//     whenever a client connects or disconnects; the main thread then starts
//     or stops its timer. While nobody is connected the plugin does no work.
//
// Packet format on the wire (little-endian, repeated for every chunk):
//
//   uint32 magic         'FBRS' (0x53524246)
//   uint32 sample_rate   Hz
//   uint32 channels
//   uint32 channel_mask  foobar2000 channel configuration bitmask
//   uint32 frames        number of sample frames in this packet
//   float  data[frames * channels]   interleaved, nominal range -1.0 .. +1.0
//
// The header is repeated in every packet because the format can change in the
// middle of a stream (sample rate or channel layout differs between tracks).
// The pipe is a byte stream, so a packet may be split across several reads on
// the client side; the client must reassemble it using the header.

// windows.h configuration. These two macros must precede the include.
//   WIN32_LEAN_AND_MEAN  leaves out rarely used parts of the Windows headers
//                        (and avoids winsock header conflicts).
//   NOMINMAX             stops windows.h from defining min/max macros, which
//                        would break the std::min calls further down.
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>

// Import library of foobar2000's shared.dll. The component client code calls
// functions that foobar2000 exports from that DLL (for example the crash-report
// hooks); without this library the linker reports unresolved __imp_ symbols.
// This name is for x64 builds only: x86 and ARM64 need their own variants.
#pragma comment(lib, "shared-x64")

// The SDK static libraries. Debug builds use the "d" variants so that the
// C runtime and _DEBUG settings of every library match this component.
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

#include <algorithm>   // std::min
#include <atomic>      // std::atomic and memory orders
#include <cstdint>     // fixed-width integer types
#include <cstring>     // memcpy, memset
#include <memory>      // std::unique_ptr
#include <new>         // std::nothrow

// Registers the component's name, version and "about" text. foobar2000 shows
// these in Preferences > Components. The version is deliberately different
// from the older DSP-based build so the two can be told apart.
DECLARE_COMPONENT_VERSION("Raw Sample Tap", "2.0.0",
    "Streams the float32 PCM that is audible at this moment (visualisation stream) to the named pipe\n"
    "\\\\.\\pipe\\foobar2000_rawtap while audio is playing.");

// foobar2000 refuses to load the component if its DLL file name differs from
// this, which prevents two copies from being installed under different names.
VALIDATE_COMPONENT_FILENAME("foo_rawtap.dll");

// Everything below is in an anonymous namespace: the names get internal
// linkage and cannot collide with symbols of other components in the process.
namespace {

    // Name of the pipe that clients open (L"" = wide string for the W API).
    const wchar_t kPipeName[] = L"\\\\.\\pipe\\foobar2000_rawtap";

    // First field of every packet; lets a client detect a desynchronised stream.
    const uint32_t kMagic = 0x53524246u;              // 'F','B','R','S'

    // Capacity of the ring buffer. When the client is too slow and the ring
    // fills up, whole packets are dropped rather than stalling playback.
    const size_t   kRingBytes = 4u * 1024u * 1024u;   // roughly 11 s of 44.1 kHz stereo float

    // The size must be a power of two so that "position modulo size" can be
    // computed with a cheap bit mask (see kRingMask).
    static_assert((kRingBytes& (kRingBytes - 1)) == 0, "ring size must be power of two");

    // Converts a monotonically increasing byte counter into a ring offset.
    const size_t   kRingMask = kRingBytes - 1;

    // Largest single WriteFile call. Bounds how long the worker is busy before
    // it frees ring space and re-checks the stop flag.
    const size_t   kMaxWrite = 256u * 1024u;          // upper bound for one WriteFile

    // Posted by the worker thread to the message window whenever a client
    // connects or disconnects. The handler re-reads the state, so the message
    // carries no payload and bursts of them are harmless.
    // WM_APP + n is the range reserved for private messages of an application.
    const UINT     kMsgLink = WM_APP + 1;

    // The SDK selects audio_sample (float or double) at build time. The wire
    // format is always float32, so a double build converts while copying into
    // the ring; a float build copies the samples unchanged.
    static_assert(sizeof(audio_sample) == sizeof(float) || sizeof(audio_sample) == sizeof(double),
        "audio_sample must be float or double");

    // Disable structure padding so the in-memory layout is exactly the wire
    // layout: five consecutive 32-bit fields, 20 bytes in total.
#pragma pack(push, 1)
    struct packet_header {
        uint32_t magic;          // kMagic
        uint32_t sample_rate;    // samples per second per channel
        uint32_t channels;       // number of interleaved channels
        uint32_t channel_mask;   // foobar2000 channel configuration bitmask
        uint32_t frames;         // sample frames following this header
    };
#pragma pack(pop)

    // ---------------------------------------------------------------------------
    // tap_sink: SPSC ring buffer plus the pipe-serving worker thread.
    //
    // Producer: the main thread (audible_tap::tick). Consumer: the worker thread.
    // m_write is stored only by the producer, m_read only by the consumer, and
    // both are monotonic byte counters (offset = counter & kRingMask).
    //
    // Ownership of the state:
    //   producer (main) writes : m_write, ring bytes in the FREE area, m_stop
    //   consumer (worker) writes: m_read, m_connected, m_sleeping
    //   The producer reads m_read (to compute free space), m_connected and
    //   m_sleeping; the consumer reads m_write and the ring bytes in the USED
    //   area. Because each variable has exactly one writer, no mutex is needed.
    //
    // The ring holds bytes between m_read (inclusive) and m_write (exclusive).
    // Counters never wrap in practice (64 bits) and are never reset while the
    // worker runs, so "bytes in use" is simply m_write - m_read.
    // ---------------------------------------------------------------------------
    class tap_sink {
    public:
        tap_sink()
            : m_write(0), m_read(0), m_connected(false), m_sleeping(false), m_stop(false)
            , m_notify(NULL), m_thread(NULL), m_dataEvt(NULL), m_stopEvt(NULL), m_ioEvt(NULL) {
        }

        // Normally on_quit() has already called stop() and this does nothing.
        // It is a safety net for the static object's destruction at DLL unload.
        ~tap_sink() {
            stop();
        }

        // 'notify' is the window that receives kMsgLink; it must outlive the thread.
        // Allocates the ring, creates the events and launches the worker thread.
        bool start(HWND notify) {
            if (m_thread) return true;   // already running

            // nothrow: report failure through the return value instead of an exception.
            m_ring.reset(new (std::nothrow) uint8_t[kRingBytes]);
            if (!m_ring) return false;

            // Start from a clean state (also covers a restart after stop()).
            m_notify = notify;
            m_write.store(0);
            m_read.store(0);
            m_connected.store(false);
            m_sleeping.store(false);
            m_stop.store(false);

            // Three events, each with a distinct purpose:
            //   m_dataEvt  auto-reset: "new data was published", wakes the worker.
            //   m_stopEvt  manual-reset: "shut down", stays signalled so every
            //              wait in the worker notices it.
            //   m_ioEvt    manual-reset: completion event for overlapped
            //              connect/write operations (reset before each use).
            m_dataEvt = CreateEventW(NULL, FALSE, FALSE, NULL);  // auto-reset
            m_stopEvt = CreateEventW(NULL, TRUE, FALSE, NULL);   // manual-reset
            m_ioEvt = CreateEventW(NULL, TRUE, FALSE, NULL);     // manual-reset
            if (m_dataEvt && m_stopEvt && m_ioEvt) {
                m_thread = CreateThread(NULL, 0, &tap_sink::thread_proc, this, 0, NULL);
            }
            if (!m_thread) {
                // Roll back everything that was created.
                close_events();
                m_ring.reset();
                return false;
            }
            return true;
        }

        // Asks the worker to exit and waits for it. Called from on_quit().
        void stop() {
            if (!m_thread) return;   // never started, or already stopped

            // Relaxed is enough for the flag itself: SetEvent below provides the
            // ordering, and the worker re-checks the flag after every wake-up.
            m_stop.store(true, std::memory_order_relaxed);
            SetEvent(m_stopEvt);   // interrupts every wait in the worker

            if (WaitForSingleObject(m_thread, 5000) == WAIT_OBJECT_0) {
                // Clean exit: nothing uses the resources any more, release them.
                CloseHandle(m_thread);
                m_thread = NULL;
                close_events();
                m_ring.reset();
            } else {
                // The worker did not exit in time. Freeing what it still uses
                // would turn a delay into memory corruption, so leak it.
                // release() gives up ownership of the ring without freeing it.
                m_thread = NULL;
                m_dataEvt = m_stopEvt = m_ioEvt = NULL;
                m_ring.release();
            }
        }

        // Cheap check for the main thread. One acquire load, no lock.
        // True while a client is connected and the ring accepts data.
        bool wants_data() const {
            return m_connected.load(std::memory_order_acquire);
        }

        // Header and payload are published together, so packets never interleave.
        // 'count' is the number of samples (frames * channels); each occupies
        // four bytes on the wire whatever the type of audio_sample is.
        // Returns false if no client is connected or the ring is full (packet dropped).
        //
        // Called only by the producer (main thread).
        bool push(const void* hdr, size_t hn, const audio_sample* samples, size_t count) {
            if (!m_connected.load(std::memory_order_acquire)) return false;

            // Total bytes this packet will occupy in the ring.
            const size_t   dn = count * sizeof(float);
            const size_t   total = hn + dn;

            // m_write is ours, so a relaxed load suffices. m_read belongs to the
            // consumer: the acquire load pairs with its release store, so by the
            // time we see an advanced m_read the consumer has finished reading
            // those bytes and it is safe to overwrite them.
            const uint64_t w = m_write.load(std::memory_order_relaxed);
            const uint64_t r = m_read.load(std::memory_order_acquire);

            // Free space = capacity - bytes in use. If the packet does not fit,
            // drop it whole; a partial packet would corrupt the stream framing.
            if (kRingBytes - (size_t)(w - r) < total) return false;

            // Copy the header, then the samples directly behind it. Nothing is
            // visible to the consumer yet: it only reads up to m_write.
            copy_in(w, hdr, hn);
            copy_samples(w + hn, samples, count);

            // seq_cst on this store and on the m_sleeping load pairs with the
            // consumer's seq_cst store/load in stream(): at least one side
            // observes the other, so a wake-up can never be lost.
            // The store also publishes the copied bytes (release semantics).
            m_write.store(w + total, std::memory_order_seq_cst);

            // Only signal the event if the consumer is (about to be) asleep.
            // This avoids a system call for every packet while the consumer is
            // busy. A spurious signal is harmless: the consumer just loops once.
            if (m_sleeping.load(std::memory_order_seq_cst)) SetEvent(m_dataEvt);
            return true;
        }

    private:
        // Thread entry point: adapts the Win32 signature to a member function.
        static DWORD WINAPI thread_proc(LPVOID p) {
            static_cast<tap_sink*>(p)->run();
            return 0;
        }

        // Closes whichever events exist and clears the handles.
        void close_events() {
            if (m_dataEvt) CloseHandle(m_dataEvt);
            if (m_stopEvt) CloseHandle(m_stopEvt);
            if (m_ioEvt)   CloseHandle(m_ioEvt);
            m_dataEvt = m_stopEvt = m_ioEvt = NULL;
        }

        // Tells the main thread that the connection state changed.
        // PostMessage never blocks and is safe to call from any thread.
        void notify() const {
            if (m_notify) PostMessageW(m_notify, kMsgLink, 0, 0);
        }

        // ---- ring buffer, producer side ----

        // Copies n bytes into the ring at the logical position 'pos' (a
        // monotonic counter), splitting the copy in two when it crosses the end
        // of the buffer. The caller guarantees that n bytes of free space exist.
        void copy_in(uint64_t pos, const void* src, size_t n) {
            const size_t off = (size_t)pos & kRingMask;           // physical offset
            const size_t first = std::min(n, kRingBytes - off);   // bytes before the end
            memcpy(m_ring.get() + off, src, first);
            // Wrapped: the remainder continues at the start of the buffer.
            if (n > first) memcpy(m_ring.get(), static_cast<const uint8_t*>(src) + first, n - first);
        }

        // audio_sample is float: the samples are already in wire format.
        // (When both overloads match, C++ prefers this non-template one.)
        void copy_samples(uint64_t pos, const float* src, size_t count) {
            copy_in(pos, src, count * sizeof(float));
        }

        // audio_sample is double: narrow to float in small blocks. The block
        // lives on the stack (4 KiB, resident in L1), the loop is trivially
        // vectorisable, and going through copy_in keeps the wrap-around logic
        // in one place and avoids type-punning the byte ring.
        template <class T>
        void copy_samples(uint64_t pos, const T* src, size_t count) {
            float tmp[1024]{};   // staging block: 1024 floats = 4 KiB
            while (count) {
                // Convert at most one block per iteration.
                const size_t k = std::min(count, sizeof tmp / sizeof tmp[0]);
                for (size_t i = 0; i < k; ++i) tmp[i] = static_cast<float>(src[i]);
                copy_in(pos, tmp, k * sizeof(float));
                pos += k * sizeof(float);   // 'pos' counts bytes, not samples
                src += k;
                count -= k;
            }
        }

        // ---- pipe I/O (worker thread only) ----

        // Blocks until a client opens the pipe, or until stop is requested.
        // Returns true when a client is connected.
        bool wait_for_client(HANDLE pipe) const {
            // The pipe was created with FILE_FLAG_OVERLAPPED, so the call is
            // asynchronous and completion is signalled through m_ioEvt.
            OVERLAPPED ov = {};
            ov.hEvent = m_ioEvt;
            ResetEvent(m_ioEvt);

            // For an overlapped pipe this normally fails with ERROR_IO_PENDING.
            if (ConnectNamedPipe(pipe, &ov)) return true;
            switch (GetLastError()) {
            case ERROR_PIPE_CONNECTED:
                // The client connected between CreateNamedPipe and now.
                return true;
            case ERROR_IO_PENDING: {
                // Wait for either a client (m_ioEvt) or shutdown (m_stopEvt).
                HANDLE h[2] = { m_ioEvt, m_stopEvt };
                DWORD dummy = 0;
                if (WaitForMultipleObjects(2, h, FALSE, INFINITE) == WAIT_OBJECT_0)
                    return GetOverlappedResult(pipe, &ov, &dummy, FALSE) != FALSE;
                // Shutting down: cancel the pending connect and wait until the
                // cancellation completes, because 'ov' lives on this stack frame
                // and the system must not touch it after we return.
                CancelIoEx(pipe, &ov);
                GetOverlappedResult(pipe, &ov, &dummy, TRUE);
                return false;
            }
            default:
                // Any other error: the instance is unusable (the caller recreates it).
                return false;
            }
        }

        // Writes exactly n bytes, or fails. Returns false when the client has
        // gone away, an error occurred, or shutdown was requested.
        bool write_all(HANDLE pipe, const uint8_t* p, DWORD n) const {
            OVERLAPPED ov = {};
            ov.hEvent = m_ioEvt;
            ResetEvent(m_ioEvt);
            DWORD written = 0;
            if (!WriteFile(pipe, p, n, NULL, &ov)) {
                // Anything other than "still in progress" is a real failure
                // (typically ERROR_NO_DATA or ERROR_BROKEN_PIPE: client left).
                if (GetLastError() != ERROR_IO_PENDING) return false;
                // The pipe's buffer is full: wait for the client to read, or stop.
                HANDLE h[2] = { m_ioEvt, m_stopEvt };
                if (WaitForMultipleObjects(2, h, FALSE, INFINITE) != WAIT_OBJECT_0) {
                    // Shutdown: cancel and wait for the cancellation to finish
                    // so that 'ov' and the source buffer are no longer in use.
                    CancelIoEx(pipe, &ov);
                    GetOverlappedResult(pipe, &ov, &written, TRUE);
                    return false;
                }
            }
            // Collect the result; a byte-mode pipe write is expected to be complete.
            if (!GetOverlappedResult(pipe, &ov, &written, FALSE)) return false;
            return written == n;
        }

        // Called once per client, right after it has connected.
        void begin_session() {
            // Discard leftovers of an earlier session. Only this thread moves
            // m_read, and nothing new is published while m_connected is false.
            m_read.store(m_write.load(std::memory_order_acquire), std::memory_order_release);
            m_sleeping.store(false, std::memory_order_relaxed);
            // Publish "connected" last, so a producer that sees true (acquire)
            // also sees the reset state above.
            m_connected.store(true, std::memory_order_release);
            notify();   // main thread: start polling
        }

        // Called once per client, after streaming has ended for any reason.
        void end_session() {
            // Stops the producer from pushing; data still in the ring is
            // discarded by the next begin_session().
            m_connected.store(false, std::memory_order_release);
            notify();   // main thread: stop polling and release the stream
        }

        // Drains the ring straight into the pipe. Bytes stay in the ring until
        // the write has completed, so the producer cannot overwrite them.
        // Returns when the client leaves, an error occurs, or stop is requested.
        void stream(HANDLE pipe) {
            HANDLE h[2] = { m_dataEvt, m_stopEvt };
            // m_read is ours, so a local copy is kept and written back after
            // each completed write.
            uint64_t r = m_read.load(std::memory_order_relaxed);

            for (;;) {
                if (m_stop.load(std::memory_order_relaxed)) return;

                // Acquire: pairs with the producer's publishing store, so the
                // bytes up to 'w' are fully written and visible to us.
                const uint64_t w = m_write.load(std::memory_order_acquire);
                if (w == r) {
                    // Ring is empty. Sleep until the producer signals, using a
                    // handshake that cannot miss a wake-up:
                    //   consumer: set m_sleeping, then re-check m_write
                    //   producer: publish m_write, then check m_sleeping
                    // Announce the intention to sleep, then look once more.
                    m_sleeping.store(true, std::memory_order_seq_cst);
                    if (m_write.load(std::memory_order_seq_cst) == r) {
                        // Still empty: block until data arrives or stop is requested.
                        const DWORD rc = WaitForMultipleObjects(2, h, FALSE, INFINITE);
                        m_sleeping.store(false, std::memory_order_relaxed);
                        if (rc != WAIT_OBJECT_0) return;   // stop requested
                    } else {
                        // Data arrived in the meantime: do not sleep after all.
                        m_sleeping.store(false, std::memory_order_relaxed);
                    }
                    continue;
                }

                // Write the largest contiguous run: no more than what is
                // available, no further than the physical end of the buffer
                // (a wrapped remainder is sent on the next iteration), and no
                // more than kMaxWrite. A packet can therefore be split across
                // several writes; the pipe is a byte stream, so that is fine.
                const size_t off = (size_t)r & kRingMask;
                const size_t n = std::min(std::min((size_t)(w - r), kRingBytes - off), kMaxWrite);
                if (!write_all(pipe, m_ring.get() + off, (DWORD)n)) return;   // client left, or stopping

                // Only now hand the bytes back to the producer (release pairs
                // with the producer's acquire load of m_read in push()).
                r += n;
                m_read.store(r, std::memory_order_release);
            }
        }

        // Worker thread body: owns one pipe instance and serves clients one
        // after another until shutdown.
        void run() {
            HANDLE pipe = INVALID_HANDLE_VALUE;

            while (!m_stop.load(std::memory_order_relaxed)) {
                // (Re)create the pipe instance only if there is none. It is
                // reused across clients, so the pipe name stays valid between
                // two connections and a client never sees "file not found".
                if (pipe == INVALID_HANDLE_VALUE) {
                    pipe = CreateNamedPipeW(
                        kPipeName,
                        // Server writes, client reads; overlapped so that every
                        // wait can also be interrupted by m_stopEvt.
                        PIPE_ACCESS_OUTBOUND | FILE_FLAG_OVERLAPPED,
                        // Byte stream, blocking semantics (the overlapped flag
                        // makes the actual calls asynchronous), and no
                        // connections from other machines.
                        PIPE_TYPE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                        // One instance only (hence one client at a time);
                        // 64 KiB outbound buffer hint, no inbound buffer,
                        // default timeout, default security descriptor.
                        1, 64 * 1024, 0, 0, NULL);
                    if (pipe == INVALID_HANDLE_VALUE) {
                        // Creation failed (for example another process owns the
                        // name). Wait a second, remaining responsive to shutdown.
                        if (WaitForSingleObject(m_stopEvt, 1000) == WAIT_OBJECT_0) break;
                        continue;
                    }
                }

                // Block until a client connects (or shutdown is requested).
                if (!wait_for_client(pipe)) {
                    // Stopping, or the instance became unusable: discard it and retry.
                    CloseHandle(pipe);
                    pipe = INVALID_HANDLE_VALUE;
                    if (WaitForSingleObject(m_stopEvt, 250) == WAIT_OBJECT_0) break;
                    continue;
                }

                // One client session: announce it, stream until it ends,
                // announce the end.
                begin_session();
                stream(pipe);
                end_session();

                // Return the same instance to the listening state. Deliberately
                // no FlushFileBuffers: it waits for the client to drain the pipe.
                DisconnectNamedPipe(pipe);
            }

            // Shutting down: release the pipe instance.
            if (pipe != INVALID_HANDLE_VALUE) CloseHandle(pipe);
        }

        std::unique_ptr<uint8_t[]> m_ring;        // the ring's storage, kRingBytes long
        std::atomic<uint64_t>      m_write;       // producer-owned
        std::atomic<uint64_t>      m_read;        // consumer-owned
        std::atomic<bool>          m_connected;   // a client is connected (producer may push)
        std::atomic<bool>          m_sleeping;    // consumer is (about to be) blocked on m_dataEvt
        std::atomic<bool>          m_stop;        // shutdown requested
        HWND                       m_notify;      // window that receives kMsgLink
        HANDLE                     m_thread, m_dataEvt, m_stopEvt, m_ioEvt;
    };

    // The single sink instance. It exists for the life of the DLL; its worker
    // thread is started in on_init() and joined in on_quit().
    tap_sink g_sink;

    // ---------------------------------------------------------------------------
    // audible_tap: polls the visualisation stream on the main thread, but only
    // while a client is connected.
    //
    // How the visualisation stream is used: it keeps a short history of the
    // audio that is audible, addressed by "absolute time" (seconds since the
    // playback started or was last seeked). get_absolute_time() tells us what
    // is audible now; get_chunk_absolute() returns the audio for a given span
    // of that timeline. A cursor (m_next) remembers where the previous request
    // ended, so consecutive requests abut and the stream has no gaps or overlaps.
    // ---------------------------------------------------------------------------

    const UINT_PTR kTimerId = 1;         // identifier of the polling timer
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
    // (While idle the timer is slow, so the first tick after playback restarts
    // can come up to one slow period late; starting slightly in the past
    // recovers the audio that played in the meantime.)
    const double   kResyncLookback = 0.1;

    class audible_tap {
    public:
        audible_tap()
            : m_hwnd(NULL), m_polling(false), m_have(false), m_logged(false)
            , m_still(0), m_curMs(0), m_next(0.0) {
        }

        // Creates the hidden window. Called on the main thread from on_init().
        bool start() {
            // Register a window class whose only purpose is to receive messages.
            WNDCLASSW wc;
            memset(&wc, 0, sizeof wc);
            wc.lpfnWndProc = &audible_tap::wnd_proc;
            wc.hInstance = core_api::get_my_instance();   // this DLL's module handle
            wc.lpszClassName = L"foo_rawtap_msgwnd";
            RegisterClassW(&wc);
            // HWND_MESSAGE creates a message-only window: invisible, no
            // painting, only message dispatch. Because it is created on the
            // main thread, foobar2000's own message loop delivers its timer
            // and posted messages there, which is where the visualisation
            // stream is meant to be used.
            m_hwnd = CreateWindowExW(0, wc.lpszClassName, L"", 0, 0, 0, 0, 0,
                HWND_MESSAGE, NULL, wc.hInstance, NULL);
            if (!m_hwnd) return false;
            // Let the static window procedure find this object again.
            SetWindowLongPtrW(m_hwnd, GWLP_USERDATA, (LONG_PTR)this);
            return true;                       // no timer until a client connects
        }

        // Stops polling and destroys the window. Called from on_quit().
        void stop() {
            if (!m_hwnd) return;
            stop_polling();
            DestroyWindow(m_hwnd);
            UnregisterClassW(L"foo_rawtap_msgwnd", core_api::get_my_instance());
            m_hwnd = NULL;
        }

        // The worker thread needs this handle to post kMsgLink.
        HWND hwnd() const {
            return m_hwnd;
        }

    private:
        // Window procedure. Windows requires a static function, so the object
        // pointer stored in GWLP_USERDATA is used to reach the instance.
        static LRESULT CALLBACK wnd_proc(HWND h, UINT msg, WPARAM w, LPARAM l) {
            audible_tap* self = (audible_tap*)GetWindowLongPtrW(h, GWLP_USERDATA);
            // self is NULL for the messages sent during window creation,
            // before start() has stored the pointer; those go to the default handler.
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

        // Reacts to a connect/disconnect notification. The message has no
        // payload; the current state is read from the sink instead, so several
        // queued notifications collapse into the right final state.
        void sync_link() {
            if (g_sink.wants_data()) start_polling();
            else                     stop_polling();
        }

        // A client has connected: arm the timer from a clean state.
        void start_polling() {
            if (m_polling) return;
            m_polling = true;
            m_have = false;      // force a resynchronisation on the first tick
            m_logged = false;    // log the first chunk of this session again
            m_still = 0;
            m_curMs = 0;         // 0 makes set_interval() really arm the timer
            set_interval(kActiveMs);
        }

        // The client has left: no timer, no stream, no work.
        void stop_polling() {
            if (!m_polling) return;
            m_polling = false;
            KillTimer(m_hwnd, kTimerId);
            m_curMs = 0;
            m_have = false;
            m_stream.release();    // the core stops buffering audio for us
        }

        // Changes the timer period (and does nothing if it is already that).
        void set_interval(UINT ms) {
            if (m_curMs == ms) return;
            m_curMs = ms;
            SetTimer(m_hwnd, kTimerId, ms, NULL);   // same id: replaces the running timer
        }

        // Creates the visualisation stream on first use. Returns false if it
        // cannot be created (the next tick tries again).
        bool ensure_stream() {
            if (m_stream.is_valid()) return true;
            try {
                // The manager is a core service; creating a stream makes the
                // core start buffering audio for this consumer.
                visualisation_manager::get()->create_stream(m_stream, 0);
            } catch (...) {
                // Service unavailable (for example during shutdown).
                return false;
            }
            if (m_stream.is_empty()) return false;
            // The v2 interface lets us ask the core to keep more history, so a
            // delayed timer tick can still fetch the audio it missed.
            service_ptr_t<visualisation_stream_v2> v2;
            if (m_stream->service_query_t(v2)) {
                v2->request_backlog(kBacklogSec);
                FB2K_console_formatter() << "foo_rawtap 2.0.0 (visualisation stream): stream created, backlog requested";
            } else {
                FB2K_console_formatter() << "foo_rawtap 2.0.0 (visualisation stream): stream created, no backlog control";
            }
            return true;
        }

        // One poll: fetch whatever audible audio has appeared since the
        // previous poll and hand it to the sink.
        void tick() {
            // KillTimer does not remove a WM_TIMER that is already queued.
            if (!m_polling) return;
            // The client left but its notification has not been processed yet.
            if (!g_sink.wants_data()) {
                stop_polling(); return;
            }
            if (!ensure_stream()) return;

            // Current position on the audible timeline. Fails when playback
            // is stopped.
            double now = 0.0;
            if (!m_stream->get_absolute_time(now)) {   // stopped
                m_have = false;                // resynchronise when playback starts again
                set_interval(kIdleMs);         // nothing to do quickly
                return;
            }

            // First tick, new playback, or a seek that reset the clock.
            // (Absolute time going backwards means the timeline restarted.)
            // m_next is placed slightly in the past (see kResyncLookback), but
            // never before the start of the timeline.
            if (!m_have || now < m_next) {
                m_next = now > kResyncLookback ? now - kResyncLookback : 0.0;
                m_have = true;
                m_still = 0;
            }

            // Amount of audio between the cursor and the present.
            double gap = now - m_next;
            if (gap > kMaxCatchUp) {
                // We fell too far behind (for example the main thread was busy):
                // anything older than this may already be gone from the
                // backlog, so skip ahead and accept a gap in the stream.
                m_next = now - kMaxCatchUp;
                gap = kMaxCatchUp;
            }
            if (gap < kMinRequest) {                   // paused, or the timer fired early
                // Count consecutive empty ticks. After kStillTicks of them
                // playback is considered paused and the timer slows down.
                // (The comparison stops the counter at kStillTicks.)
                if (m_still < kStillTicks && ++m_still == kStillTicks) set_interval(kIdleMs);
                return;
            }
            // New audio is flowing: reset the pause counter and make sure the
            // timer is at its fast rate again.
            m_still = 0;
            set_interval(kActiveMs);
            // Limit one request to a sensible size.
            if (gap > kMaxRequest) gap = kMaxRequest;

            // Ask the stream for the span [m_next, m_next + gap). It can fail
            // when the data is not available yet; the cursor is then left
            // unchanged and the same span is requested again on the next tick.
            if (!m_stream->get_chunk_absolute(m_chunk, m_next, gap)) return;

            // Read the chunk's actual properties. The stream may return a
            // slightly different length than requested, and the format can
            // change between chunks.
            const size_t   frames = m_chunk.get_sample_count();
            const unsigned ch = m_chunk.get_channel_count();
            const unsigned rate = m_chunk.get_sample_rate();
            if (frames == 0 || ch == 0 || rate == 0) return;   // nothing usable

            const size_t n = frames * ch;   // total samples (all channels)

            // Describe this packet to the client.
            packet_header h;
            h.magic = kMagic;
            h.sample_rate = rate;
            h.channels = ch;
            h.channel_mask = m_chunk.get_channel_config();
            h.frames = (uint32_t)frames;

            // Interleaved samples; the sink writes them as float32 whatever
            // the SDK's audio_sample type is.
            // The return value is ignored on purpose: if the ring is full the
            // packet is dropped, and the cursor below still advances, so the
            // stream stays in step with real time instead of falling behind.
            g_sink.push(&h, sizeof h, m_chunk.get_data(), n);

            // One-time confirmation in foobar2000's console (View > Console)
            // that audio is really being delivered, with its format.
            if (!m_logged) {
                m_logged = true;
                FB2K_console_formatter() << "foo_rawtap: first audible chunk sent: "
                    << (unsigned)rate << " Hz, " << (unsigned)ch << " ch, "
                    << (unsigned)frames << " frames";
            }

            // Advance by what was actually delivered, so consecutive requests abut.
            m_next += (double)frames / (double)rate;
        }

        HWND                                m_hwnd;     // message-only window
        service_ptr_t<visualisation_stream> m_stream;   // created lazily, released when idle
        bool                                m_polling;  // timer is armed
        bool                                m_have;     // m_next is valid (synchronised)
        bool                                m_logged;   // first-chunk message already written
        unsigned                            m_still;    // consecutive ticks without new audio
        UINT                                m_curMs;    // currently armed timer period
        double                              m_next;     // absolute time of the next sample wanted
        audio_chunk_impl                    m_chunk;    // reused every tick, avoids realloc
    };

    // The single tap instance; it is driven entirely by the main thread.
    audible_tap g_tap;

    // initquit services are called by foobar2000 on the main thread when the
    // application finishes starting up and just before it shuts down. They
    // define the lifetime of the whole component.
    class rawtap_initquit : public initquit {
    public:
        void on_init() override {
            // The window must exist before the worker starts: the worker posts to it.
            // (|| short-circuits: if the window cannot be created, the worker is not started.)
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
    // Registers the class with foobar2000's service system so that it is
    // instantiated and its callbacks are invoked.
    initquit_factory_t<rawtap_initquit> g_initquit_factory;

} // namespace