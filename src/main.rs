//! Non-blocking dispatch benchmark: full round-trip pipeline.
//!
//!   strategy(N) --submit_q--> orchestrator --gateway_inbox--> gateway(M)
//!   strategy(N) <--return_q-- orchestrator <--completion_q--- gateway(M)
//!
//! submit_q and completion_q (the contended MPSC handoffs) are built with one
//! of four non-blocking techniques and compared. gateway_inbox and return_q are
//! fixed SPSC rings. Each gateway models a venue: it holds a due-heap and delays
//! every order by D ~ Uniform(d_min, d_max), which stands in for the network
//! round-trip plus exchange processing. Every thread busy-polls; none parks or
//! sleeps. Each order carries 8 timestamps; the strategy computes 7 segment
//! latencies on return.
//!
//! Hyperparameters come from config.yaml. Nothing here
//! references a real trading system.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_queue::ArrayQueue;

// ---- core pinning ---------------------------------------------------------
//
// Each thread is pinned to its own physical core so the OS scheduler cannot
// migrate it or stack two busy-poll threads on one core. Linux only; macOS
// has no hard affinity API, so there it is a no-op and the p99.9 tail stays
// polluted by scheduler preemption (see the loop-gap diagnostics).

#[cfg(target_os = "linux")]
const PIN: &str = "on";
#[cfg(not(target_os = "linux"))]
const PIN: &str = "off";

#[cfg(target_os = "linux")]
fn pin_to_core(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        let _ = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}
#[cfg(not(target_os = "linux"))]
fn pin_to_core(_core: usize) {}

// ---- config ---------------------------------------------------------------

struct Config {
    n: usize,           // strategy threads
    m: usize,           // gateway threads (one per venue)
    q: usize,           // orders per strategy
    window: Duration,   // T: orders scheduled uniformly over [0, T]
    d_min: Duration,    // venue delay range
    d_max: Duration,
    repeats: usize,     // runs per technique
    shared_cap: usize,  // submit_q / completion_q ring
    spsc_cap: usize,    // gateway_inbox / return_q / per-producer SPSC
    sample_us: u64,     // in-flight sampling period
    orch_work_sweep: Vec<u64>, // orchestrator per-order work (ns), values to sweep
}
impl Default for Config {
    fn default() -> Self {
        Config {
            n: 3,
            m: 4,
            q: 80_000,
            window: Duration::from_millis(2000),
            d_min: Duration::from_micros(5_000),
            d_max: Duration::from_micros(500_000),
            repeats: 3,
            shared_cap: 8192,
            spsc_cap: 8192,
            sample_us: 50,
            orch_work_sweep: vec![0],
        }
    }
}

/// Parse config.yaml: flat `key: value` lines. Missing file falls back to defaults.
fn load_config() -> Config {
    let mut c = Config::default();
    let text = match std::fs::read_to_string("config.yaml") {
        Ok(t) => t,
        Err(_) => return c,
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once(':') else { continue };
        let v = v.split('#').next().unwrap_or("").trim(); // strip inline comment
        let k = k.trim();
        if k == "orch_work_sweep" {
            let list: Vec<u64> = v.split(',').filter_map(|x| x.trim().parse().ok()).collect();
            if !list.is_empty() {
                c.orch_work_sweep = list;
            }
            continue;
        }
        let val: u64 = match v.parse() {
            Ok(x) => x,
            Err(_) => continue,
        };
        match k {
            "n_strategies" => c.n = val as usize,
            "m_venues" => c.m = val as usize,
            "orders_per_strategy" => c.q = val as usize,
            "window_ms" => c.window = Duration::from_millis(val),
            "d_min_us" => c.d_min = Duration::from_micros(val),
            "d_max_us" => c.d_max = Duration::from_micros(val),
            "repeats" => c.repeats = val as usize,
            "shared_cap" => c.shared_cap = val as usize,
            "spsc_cap" => c.spsc_cap = val as usize,
            "sample_us" => c.sample_us = val,
            _ => {}
        }
    }
    c
}

// ---- deterministic RNG (splitmix64) ---------------------------------------

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn uniform(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.next() % (hi - lo)
    }
}

// ---- order ----------------------------------------------------------------

/// One order. `ts` accumulates the 8 measured instants as it travels:
/// ts[0] strategy_send · ts[1] orch_recv_submit · ts[2] orch_dispatch ·
/// ts[3] gateway_recv · ts[4] gateway_done · ts[5] orch_recv_completion ·
/// ts[6] orch_return · ts[7] strategy_recv.
struct Order {
    strategy: usize, // origin, routes the return
    venue: usize,    // target, routes the dispatch
    due: Instant,    // completion-due instant, set by the gateway
    ts: [Instant; 8],
}
impl Order {
    fn new(strategy: usize, venue: usize) -> Self {
        let now = Instant::now();
        Order { strategy, venue, due: now, ts: [now; 8] }
    }
}

/// Heap wrapper: orders a `BinaryHeap` (a max-heap) so the smallest `due` sits
/// on top, a min-heap keyed on completion-due time.
struct ByDue(Order);
impl PartialEq for ByDue {
    fn eq(&self, o: &Self) -> bool {
        self.0.due == o.0.due
    }
}
impl Eq for ByDue {}
impl PartialOrd for ByDue {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for ByDue {
    fn cmp(&self, o: &Self) -> Ordering {
        o.0.due.cmp(&self.0.due) // reversed → earliest due on top
    }
}

// ---- measurement ----------------------------------------------------------

/// 9 latencies, all in ns.
#[derive(Clone, Copy)]
struct Sample {
    vals: [u64; 9],
}
// short display names; full segment names in the make_sample comments below
const METRIC_NAMES: [&str; 9] = [
    "submit", "orch_pre", "dispatch", "venue_svc",
    "completion", "orch_post", "return", "round_trip", "rt-venue",
];

fn make_sample(o: &Order) -> Sample {
    let d = |a: usize, b: usize| o.ts[b].saturating_duration_since(o.ts[a]).as_nanos() as u64;
    let vservice = d(3, 4);
    let round_trip = d(0, 7);
    Sample {
        vals: [
            d(0, 1),                             // submit_latency
            d(1, 2),                             // orch_pre_dispatch
            d(2, 3),                             // dispatch_latency
            vservice,                            // venue_service
            d(4, 5),                             // completion_latency
            d(5, 6),                             // orch_post_process
            d(6, 7),                             // return_latency
            round_trip,                          // round_trip
            round_trip.saturating_sub(vservice), // net
        ],
    }
}

/// Per-thread loop diagnostics. A gap is the wall time between the start of
/// two consecutive loop iterations. No loop body does milliseconds of work, so
/// a gap over 10ms means the thread was stalled (OS preemption, or a bug).
struct LoopStat {
    max_gap_ns: u64,
    over_1ms: u32,
    over_10ms: u32,
    iters: u64,
}
impl LoopStat {
    fn new() -> Self {
        LoopStat { max_gap_ns: 0, over_1ms: 0, over_10ms: 0, iters: 0 }
    }
    fn tick(&mut self, last: Instant, now: Instant) {
        let g = now.saturating_duration_since(last).as_nanos() as u64;
        if g > self.max_gap_ns {
            self.max_gap_ns = g;
        }
        if g > 1_000_000 {
            self.over_1ms += 1;
        }
        if g > 10_000_000 {
            self.over_10ms += 1;
        }
        self.iters += 1;
    }
}

// ---- non-blocking techniques ----------------------------------------------

#[derive(Clone, Copy)]
enum Tech {
    StdMpsc,
    Crossbeam,
    ArrayQ,
    SpscMux,
}
const TECHS: [(&str, Tech); 4] = [
    ("std::sync::mpsc (unbounded)", Tech::StdMpsc),
    ("crossbeam-channel (bounded)", Tech::Crossbeam),
    ("crossbeam ArrayQueue (MPSC ring)", Tech::ArrayQ),
    ("per-producer SPSC + mux", Tech::SpscMux),
];

/// Producer handle. `try_push` stamps `ts[idx]` and enqueues once; on a full
/// bounded queue it returns the order so the caller can hold and retry.
#[derive(Clone)]
enum Tx {
    Std(mpsc::Sender<Order>),
    Cb(crossbeam_channel::Sender<Order>),
    Aq(Arc<ArrayQueue<Order>>),
}
impl Tx {
    fn try_push(&self, mut o: Order, idx: usize) -> Result<(), Order> {
        match self {
            Tx::Std(s) => {
                o.ts[idx] = Instant::now();
                let _ = s.send(o); // unbounded, only fails if receiver is gone
                Ok(())
            }
            Tx::Cb(s) => {
                o.ts[idx] = Instant::now();
                match s.try_send(o) {
                    Ok(()) => Ok(()),
                    Err(crossbeam_channel::TrySendError::Full(x)) => Err(x),
                    Err(crossbeam_channel::TrySendError::Disconnected(x)) => Err(x),
                }
            }
            Tx::Aq(q) => {
                o.ts[idx] = Instant::now();
                q.push(o)
            }
        }
    }
}

/// Consumer handle.
enum Rx {
    Std(mpsc::Receiver<Order>),
    Cb(crossbeam_channel::Receiver<Order>),
    Aq(Arc<ArrayQueue<Order>>),
    Mux { qs: Vec<Arc<ArrayQueue<Order>>>, cursor: usize },
}
impl Rx {
    fn try_pop(&mut self) -> Option<Order> {
        match self {
            Rx::Std(r) => r.try_recv().ok(),
            Rx::Cb(r) => r.try_recv().ok(),
            Rx::Aq(q) => q.pop(),
            Rx::Mux { qs, cursor } => {
                let n = qs.len();
                for _ in 0..n {
                    let i = *cursor % n;
                    *cursor = cursor.wrapping_add(1);
                    if let Some(o) = qs[i].pop() {
                        return Some(o);
                    }
                }
                None
            }
        }
    }
}

/// Build one contended MPSC handoff with `np` producers using `tech`.
fn build(tech: Tech, np: usize, shared_cap: usize, spsc_cap: usize) -> (Vec<Tx>, Rx) {
    match tech {
        Tech::StdMpsc => {
            let (tx, rx) = mpsc::channel();
            (vec![Tx::Std(tx); np], Rx::Std(rx))
        }
        Tech::Crossbeam => {
            let (tx, rx) = crossbeam_channel::bounded(shared_cap);
            (vec![Tx::Cb(tx); np], Rx::Cb(rx))
        }
        Tech::ArrayQ => {
            let q = Arc::new(ArrayQueue::new(shared_cap));
            (vec![Tx::Aq(q.clone()); np], Rx::Aq(q))
        }
        Tech::SpscMux => {
            let qs: Vec<_> = (0..np).map(|_| Arc::new(ArrayQueue::new(spsc_cap))).collect();
            let tx = qs.iter().map(|q| Tx::Aq(q.clone())).collect();
            (tx, Rx::Mux { qs, cursor: 0 })
        }
    }
}

/// Drain `pending` into per-target producers, keeping orders that did not fit.
fn flush(pending: &mut Vec<Order>, target: impl Fn(&Order) -> usize, tx: &[Tx], idx: usize) -> usize {
    if pending.is_empty() {
        return 0;
    }
    let mut keep = Vec::new();
    let mut sent = 0;
    for o in pending.drain(..) {
        match tx[target(&o)].try_push(o, idx) {
            Ok(()) => sent += 1,
            Err(o) => keep.push(o),
        }
    }
    *pending = keep;
    sent
}

/// Simulate orchestrator per-order work: `iters` read-modify-writes over
/// `scratch`. Models the engine doing real work per order (risk check, routing,
/// book update); it burns CPU and perturbs the orchestrator's own cache, so the
/// next handoff finds colder lines. `scratch` is sized to stay in this core's
/// L1/L2. A fixed iteration count (not a wall-clock deadline) keeps this a CPU
/// cost: a scheduler preemption then shows up in the loop-gap diagnostic
/// instead of being silently absorbed into the work.
fn simulate_work(iters: u64, scratch: &mut [u64]) {
    let len = scratch.len();
    let mut i = 0usize;
    for k in 0..iters {
        scratch[i] = scratch[i].wrapping_add(k | 1);
        i += 1;
        if i == len {
            i = 0;
        }
    }
    std::hint::black_box(scratch);
}

/// Measure scratch-write cost on this machine: ns per `simulate_work` iteration.
/// Min over several trials, so a preempted trial is discounted. Used to convert
/// the configured per-order work (ns) into an iteration count.
fn calibrate_work() -> f64 {
    let mut scratch = vec![0u64; 4096];
    let trial: u64 = 5_000_000;
    let mut ns_per_iter = f64::MAX;
    for _ in 0..5 {
        let t = Instant::now();
        simulate_work(trial, &mut scratch);
        let ns = t.elapsed().as_nanos() as f64;
        ns_per_iter = ns_per_iter.min(ns / trial as f64);
    }
    ns_per_iter
}

// ---- schedule -------------------------------------------------------------

/// Per-strategy fire-time offsets: Q samples from Uniform(0, T), sorted.
fn gen_schedules(cfg: &Config) -> Vec<Vec<Duration>> {
    let window_ns = cfg.window.as_nanos() as u64;
    (0..cfg.n)
        .map(|s| {
            let mut rng = Rng::new(0x5C4E_D000 ^ s as u64);
            let mut v: Vec<Duration> = (0..cfg.q)
                .map(|_| Duration::from_nanos(rng.uniform(0, window_ns)))
                .collect();
            v.sort_unstable();
            v
        })
        .collect()
}

// ---- one run --------------------------------------------------------------

struct RunResult {
    samples: Vec<Sample>,
    inflight: Vec<usize>,
    loopstats: Vec<(String, LoopStat)>,
    full_events: u64, // bounded-queue Full returns; 0 means no backpressure
}

fn run_one(cfg: &Config, tech: Tech, offsets: &[Vec<Duration>], work_iters: u64) -> RunResult {
    let total = cfg.n * cfg.q;
    let inflight = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));
    let sampler_done = Arc::new(AtomicBool::new(false));
    let full_events = Arc::new(AtomicU64::new(0));

    let (submit_tx, submit_rx) = build(tech, cfg.n, cfg.shared_cap, cfg.spsc_cap);
    let (compl_tx, compl_rx) = build(tech, cfg.m, cfg.shared_cap, cfg.spsc_cap);
    let gateway_inbox: Vec<Arc<ArrayQueue<Order>>> =
        (0..cfg.m).map(|_| Arc::new(ArrayQueue::new(cfg.spsc_cap))).collect();
    let return_q: Vec<Arc<ArrayQueue<Order>>> =
        (0..cfg.n).map(|_| Arc::new(ArrayQueue::new(cfg.spsc_cap))).collect();

    // sampler: reads the in-flight counter every sample_us
    let sampler = {
        let inflight = inflight.clone();
        let done = sampler_done.clone();
        let core = cfg.n + 1 + cfg.m;
        let period = cfg.sample_us;
        thread::spawn(move || {
            pin_to_core(core);
            let mut v = Vec::new();
            let mut ls = LoopStat::new();
            let mut gtick = Instant::now();
            while !done.load(Relaxed) {
                let gnow = Instant::now();
                ls.tick(gtick, gnow);
                gtick = gnow;
                v.push(inflight.load(Relaxed));
                let m = Instant::now();
                while m.elapsed() < Duration::from_micros(period) {
                    std::hint::spin_loop();
                }
            }
            (v, ls)
        })
    };

    // gateways: each models one venue via a due-heap
    let mut gateway_handles = Vec::new();
    for v in 0..cfg.m {
        let mut inbox = Rx::Aq(gateway_inbox[v].clone());
        let ctx = vec![compl_tx[v].clone()];
        let sd = shutdown.clone();
        let fe = full_events.clone();
        let d_min = cfg.d_min.as_nanos() as u64;
        let d_max = cfg.d_max.as_nanos() as u64;
        let core = cfg.n + 1 + v;
        gateway_handles.push(thread::spawn(move || {
            pin_to_core(core);
            let mut heap: BinaryHeap<ByDue> = BinaryHeap::new();
            let mut pending: Vec<Order> = Vec::new();
            let mut rng = Rng::new(0xD1CE_0000 ^ v as u64);
            let mut ls = LoopStat::new();
            let mut gtick = Instant::now();
            loop {
                let gnow = Instant::now();
                ls.tick(gtick, gnow);
                gtick = gnow;
                // ① drain inbox → heap, assigning each a Uniform delay
                while let Some(mut o) = inbox.try_pop() {
                    o.ts[3] = Instant::now();
                    let d = rng.uniform(d_min, d_max);
                    o.due = o.ts[3] + Duration::from_nanos(d);
                    heap.push(ByDue(o));
                }
                // ② emit due orders → completion_q immediately (pending only on Full)
                let now = Instant::now();
                while heap.peek().map_or(false, |b| b.0.due <= now) {
                    if let Some(b) = heap.pop() {
                        if let Err(o) = ctx[0].try_push(b.0, 4) {
                            fe.fetch_add(1, Relaxed);
                            pending.push(o);
                        }
                    }
                }
                flush(&mut pending, |_| 0, &ctx, 4);
                if sd.load(Relaxed) && heap.is_empty() && pending.is_empty() {
                    break;
                }
            }
            ls
        }));
    }

    // orchestrator
    let orchestrator = {
        let mut srx = submit_rx;
        let mut crx = compl_rx;
        let gtx: Vec<Tx> = gateway_inbox.iter().map(|q| Tx::Aq(q.clone())).collect();
        let rtx: Vec<Tx> = return_q.iter().map(|q| Tx::Aq(q.clone())).collect();
        let fe = full_events.clone();
        let core = cfg.n;
        thread::spawn(move || {
            pin_to_core(core);
            let mut scratch = vec![0u64; 4096]; // 32 KB engine-state working set
            let mut pend_disp: Vec<Order> = Vec::new();
            let mut pend_ret: Vec<Order> = Vec::new();
            let mut produced = 0usize;
            let mut ls = LoopStat::new();
            let mut gtick = Instant::now();
            loop {
                let gnow = Instant::now();
                ls.tick(gtick, gnow);
                gtick = gnow;
                // ① drain submit_q → dispatch to gateway (pending only on Full)
                while let Some(mut o) = srx.try_pop() {
                    o.ts[1] = Instant::now();
                    simulate_work(work_iters, &mut scratch);
                    let v = o.venue;
                    if let Err(o) = gtx[v].try_push(o, 2) {
                        fe.fetch_add(1, Relaxed);
                        pend_disp.push(o);
                    }
                }
                flush(&mut pend_disp, |o| o.venue, &gtx, 2);
                // ② drain completion_q → return to strategy (pending only on Full)
                while let Some(mut o) = crx.try_pop() {
                    o.ts[5] = Instant::now();
                    let s = o.strategy;
                    match rtx[s].try_push(o, 6) {
                        Ok(()) => produced += 1,
                        Err(o) => {
                            fe.fetch_add(1, Relaxed);
                            pend_ret.push(o);
                        }
                    }
                }
                produced += flush(&mut pend_ret, |o| o.strategy, &rtx, 6);
                if produced == total {
                    break;
                }
            }
            ls
        })
    };

    // strategies
    let t_start = Instant::now() + Duration::from_millis(20);
    let mut strat_handles = Vec::new();
    for s in 0..cfg.n {
        let tx = vec![submit_tx[s].clone()];
        let mut rrx = Rx::Aq(return_q[s].clone());
        let schedule: Vec<Instant> = offsets[s].iter().map(|d| t_start + *d).collect();
        let infl = inflight.clone();
        let fe = full_events.clone();
        let m = cfg.m;
        let core = s;
        strat_handles.push(thread::spawn(move || {
            pin_to_core(core);
            let quota = schedule.len();
            let mut next = 0usize;
            let mut done = 0usize;
            let mut pending: Vec<Order> = Vec::new();
            let mut samples: Vec<Sample> = Vec::with_capacity(quota);
            let mut ls = LoopStat::new();
            let mut gtick = Instant::now();
            loop {
                let gnow = Instant::now();
                ls.tick(gtick, gnow);
                gtick = gnow;
                // ① drain return_q → record
                while let Some(mut o) = rrx.try_pop() {
                    o.ts[7] = Instant::now();
                    samples.push(make_sample(&o));
                    infl.fetch_sub(1, Relaxed);
                    done += 1;
                }
                // ② fire every scheduled order, pushing immediately (pending on Full)
                let now = Instant::now();
                while next < quota && schedule[next] <= now {
                    match tx[0].try_push(Order::new(s, next % m), 0) {
                        Ok(()) => {
                            infl.fetch_add(1, Relaxed);
                        }
                        Err(o) => {
                            fe.fetch_add(1, Relaxed);
                            pending.push(o);
                        }
                    }
                    next += 1;
                }
                let sent = flush(&mut pending, |_| 0, &tx, 0);
                for _ in 0..sent {
                    infl.fetch_add(1, Relaxed);
                }
                if next == quota && done == quota {
                    break;
                }
            }
            (samples, ls)
        }));
    }

    // join
    let mut samples: Vec<Sample> = Vec::with_capacity(total);
    let mut loopstats: Vec<(String, LoopStat)> = Vec::new();
    for (s, h) in strat_handles.into_iter().enumerate() {
        if let Ok((v, ls)) = h.join() {
            samples.extend(v);
            loopstats.push((format!("strategy{s}"), ls));
        }
    }
    if let Ok(ls) = orchestrator.join() {
        loopstats.push(("orchestrator".to_string(), ls));
    }
    shutdown.store(true, Relaxed);
    for (v, h) in gateway_handles.into_iter().enumerate() {
        if let Ok(ls) = h.join() {
            loopstats.push((format!("gateway{v}"), ls));
        }
    }
    sampler_done.store(true, Relaxed);
    let (inflight, sampler_ls) = match sampler.join() {
        Ok(x) => x,
        Err(_) => (Vec::new(), LoopStat::new()),
    };
    loopstats.push(("sampler".to_string(), sampler_ls));

    RunResult {
        samples,
        inflight,
        loopstats,
        full_events: full_events.load(Relaxed),
    }
}

// ---- reporting ------------------------------------------------------------

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(((sorted.len() - 1) as f64) * p).round() as usize]
}

/// Median of per-run values (sorts in place); for an even count this returns
/// the upper of the two middle values.
fn median(v: &mut [u64]) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[v.len() / 2]
}

/// Compact human-readable duration; keeps console lines narrow (mobile).
fn fmt_ns(v: u64) -> String {
    if v < 1_000 {
        format!("{v}ns")
    } else if v < 1_000_000 {
        format!("{:.1}us", v as f64 / 1e3)
    } else if v < 1_000_000_000 {
        format!("{:.1}ms", v as f64 / 1e6)
    } else {
        format!("{:.2}s", v as f64 / 1e9)
    }
}

fn main() {
    let cfg = load_config();
    let cores = thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    let need = cfg.n + cfg.m + 2;
    let total = cfg.n * cfg.q;
    let ns_per_iter = calibrate_work();

    println!("non-blocking dispatch benchmark");
    println!("N={} M={} Q={} total={}", cfg.n, cfg.m, cfg.q, total);
    println!(
        "window {}ms · D~U({},{})ms · repeats {}",
        cfg.window.as_millis(),
        cfg.d_min.as_millis(),
        cfg.d_max.as_millis(),
        cfg.repeats,
    );
    println!(
        "caps: shared_cap {} · spsc_cap {} · sample_us {}",
        cfg.shared_cap, cfg.spsc_cap, cfg.sample_us,
    );
    println!(
        "threads {need} / cores {cores} · pin {PIN}{}",
        if cores != 0 && cores < need { " [over-subscribed]" } else { "" }
    );
    println!(
        "orch_work sweep: {} ns · calibration {:.2} ns/iter",
        cfg.orch_work_sweep.iter().map(|w| w.to_string()).collect::<Vec<_>>().join(", "),
        ns_per_iter,
    );

    let offsets = gen_schedules(&cfg);

    for work_ns in cfg.orch_work_sweep.iter().copied() {
    let work_iters = (work_ns as f64 / ns_per_iter) as u64;
    println!("\n\n▓▓▓ orch_work = {work_ns} ns ▓▓▓");
    for (name, tech) in TECHS {
        let mut p50 = [(); 9].map(|_| Vec::<u64>::new());
        let mut p99 = [(); 9].map(|_| Vec::<u64>::new());
        let mut p999 = [(); 9].map(|_| Vec::<u64>::new());
        let mut peaks: Vec<u64> = Vec::new();
        let mut inflight_n: Vec<u64> = Vec::new();
        let mut stalls_10ms = 0u32;
        let mut worst_gap = 0u64;
        let mut full_total = 0u64;

        for _ in 0..cfg.repeats {
            let r = run_one(&cfg, tech, &offsets, work_iters);
            for m in 0..9 {
                let mut col: Vec<u64> = r.samples.iter().map(|s| s.vals[m]).collect();
                col.sort_unstable();
                p50[m].push(pct(&col, 0.50));
                p99[m].push(pct(&col, 0.99));
                p999[m].push(pct(&col, 0.999));
            }
            peaks.push(r.inflight.iter().copied().max().unwrap_or(0) as u64);
            inflight_n.push(r.inflight.len() as u64);
            for (_, ls) in &r.loopstats {
                stalls_10ms += ls.over_10ms;
                worst_gap = worst_gap.max(ls.max_gap_ns);
            }
            full_total += r.full_events;
        }

        println!("\n═ {name}");
        println!("median of {} runs · count {}/run", cfg.repeats, total);
        println!("{:<11}{:>9}{:>9}{:>9}", "", "p50", "p99", "p99.9");
        for m in 0..9 {
            println!(
                "{:<11}{:>9}{:>9}{:>9}",
                METRIC_NAMES[m],
                fmt_ns(median(&mut p50[m])),
                fmt_ns(median(&mut p99[m])),
                fmt_ns(median(&mut p999[m])),
            );
        }
        println!(
            "in-flight  med {} max {} n {}",
            median(&mut peaks),
            peaks.iter().copied().max().unwrap_or(0),
            median(&mut inflight_n),
        );
        println!(
            "rt-venue spread/{} runs: p50 [{}..{}]  p99 [{}..{}]",
            cfg.repeats,
            fmt_ns(*p50[8].first().unwrap_or(&0)),
            fmt_ns(*p50[8].last().unwrap_or(&0)),
            fmt_ns(*p99[8].first().unwrap_or(&0)),
            fmt_ns(*p99[8].last().unwrap_or(&0)),
        );
        println!(
            "stalls >10ms: {}/{} runs · worst {}  ·  Full events: {}",
            stalls_10ms,
            cfg.repeats,
            fmt_ns(worst_gap),
            full_total,
        );
    }
    }
}
