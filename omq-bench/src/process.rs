use std::collections::HashMap;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

// Optional legacy affinity masks. Disabled unless OMQ_BENCH_TASKSET is set.
pub(crate) const MEASURED_CPU: &str = "0-2";
pub(crate) const OTHER_CPU: &str = "3-5";
const MAX_PROC_LIFETIME: Duration = Duration::from_mins(1);

static LIVE_PROCS: OnceLock<Mutex<HashMap<u32, Instant>>> = OnceLock::new();
static REAPER_INSTALLED: AtomicBool = AtomicBool::new(false);

fn live_procs() -> &'static Mutex<HashMap<u32, Instant>> {
    LIVE_PROCS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_proc(pid: u32) {
    live_procs().lock().unwrap().insert(pid, Instant::now());
}

fn deregister_proc(pid: u32) {
    live_procs().lock().unwrap().remove(&pid);
}

/// Install the process reaper: atexit cleanup + watchdog thread.
pub(crate) fn install_reaper() {
    if REAPER_INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }

    // Watchdog thread kills any process alive longer than MAX_PROC_LIFETIME.
    std::thread::Builder::new()
        .name("watchdog".into())
        .spawn(|| {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let now = Instant::now();
                let pids: Vec<u32> = {
                    let map = live_procs().lock().unwrap();
                    map.iter()
                        .filter(|(_, started)| now.duration_since(**started) > MAX_PROC_LIFETIME)
                        .map(|(pid, _)| *pid)
                        .collect()
                };
                for pid in pids {
                    eprintln!("[watchdog] killing pid {pid} (exceeded {MAX_PROC_LIFETIME:?})");
                    hard_kill_pid(pid);
                    deregister_proc(pid);
                }
            }
        })
        .ok();
}

/// Kill all registered processes. Called on exit.
pub(crate) fn reap_all() {
    let pids: Vec<u32> = {
        let map = live_procs().lock().unwrap();
        map.keys().copied().collect()
    };
    for pid in pids {
        hard_kill_pid(pid);
        deregister_proc(pid);
    }
}

#[allow(clippy::cast_possible_wrap, clippy::similar_names)]
fn hard_kill_pid(pid: u32) {
    #[cfg(unix)]
    unsafe {
        let pgid = libc::getpgid(pid as libc::pid_t);
        if pgid > 0 {
            libc::kill(-pgid, libc::SIGKILL);
        } else {
            libc::kill(pid as libc::pid_t, libc::SIGKILL);
        }
        libc::waitpid(pid as libc::pid_t, std::ptr::null_mut(), libc::WNOHANG);
    }
    #[cfg(not(unix))]
    let _ = pid;
}

/// RAII guard that kills its process on drop.
pub(crate) struct ProcessGuard {
    child: Option<Child>,
    pid: u32,
}

impl ProcessGuard {
    fn new(child: Child) -> Self {
        let pid = child.id();
        register_proc(pid);
        Self {
            child: Some(child),
            pid,
        }
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().unwrap()
    }

    /// Kill the process (SIGTERM, wait, SIGKILL).
    #[allow(clippy::cast_possible_wrap, clippy::similar_names)]
    pub(crate) fn kill(&mut self) {
        if let Some(ref mut child) = self.child {
            #[cfg(unix)]
            {
                let raw_pid = child.id() as libc::pid_t;
                unsafe {
                    let pgid = libc::getpgid(raw_pid);
                    if pgid > 0 {
                        libc::kill(-pgid, libc::SIGTERM);
                    } else {
                        libc::kill(raw_pid, libc::SIGTERM);
                    }
                }
            }
            if let Ok(Some(_)) = child.wait_timeout(Duration::from_secs(5)) {
            } else {
                hard_kill_pid(self.pid);
                child.wait().ok();
            }
        }
        deregister_proc(self.pid);
    }

    /// Wait for the process to finish, returning stdout contents.
    pub(crate) fn wait_with_output(&mut self, timeout: Duration) -> Option<String> {
        let child = self.child.as_mut()?;
        if let Ok(Some(_)) = child.wait_timeout(timeout) {
            let mut out = String::new();
            if let Some(ref mut stdout) = child.stdout {
                stdout.read_to_string(&mut out).ok();
            }
            deregister_proc(self.pid);
            Some(out)
        } else {
            self.kill();
            None
        }
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Spawn a subprocess. CPU pinning is opt-in via `OMQ_BENCH_TASKSET=1`.
pub(crate) fn spawn(cmd: &[&str], env: &[(&str, &str)], cpu: Option<&str>) -> ProcessGuard {
    let mut args: Vec<&str> = Vec::new();
    if std::env::var_os("OMQ_BENCH_TASKSET").is_some()
        && let Some(cpus) = cpu
    {
        args.extend(["taskset", "-c", cpus]);
    }
    args.extend(cmd);

    let mut command = Command::new(args[0]);
    command.args(&args[1..]);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::null());
    for &(k, v) in env {
        command.env(k, v);
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }

    let child = command.spawn().unwrap_or_else(|e| {
        panic!("failed to spawn {args:?}: {e}");
    });
    ProcessGuard::new(child)
}

/// Run a process to completion, capturing stdout and reading CPU time from /proc.
pub(crate) fn capture_with_cpu(
    cmd: &[&str],
    env: &[(&str, &str)],
    cpu: Option<&str>,
    timeout: Duration,
) -> Option<(String, f64)> {
    let mut proc = spawn(cmd, env, cpu);
    let pid = proc.pid();
    let child = proc.child_mut();

    let mut stdout_content = String::new();
    if let Some(ref mut stdout) = child.stdout {
        stdout.read_to_string(&mut stdout_content).ok();
    }

    let cpu_secs = read_proc_cpu(pid);

    if let Ok(Some(_)) = child.wait_timeout(timeout) {
        deregister_proc(pid);
        // Prevent the Drop from killing again.
        std::mem::forget(proc);
        Some((stdout_content, cpu_secs))
    } else {
        drop(proc);
        None
    }
}

/// Run a process to completion, capturing stdout.
pub(crate) fn capture(
    cmd: &[&str],
    env: &[(&str, &str)],
    cpu: Option<&str>,
    timeout: Duration,
) -> Option<String> {
    let mut proc = spawn(cmd, env, cpu);
    proc.wait_with_output(timeout)
}

/// Read CPU time (user + system) for a process from /proc.
pub(crate) fn read_proc_cpu(pid: u32) -> f64 {
    let path = format!("/proc/{pid}/stat");
    let Ok(contents) = std::fs::read_to_string(&path) else {
        return 0.0;
    };
    let fields: Vec<&str> = contents.split_whitespace().collect();
    if fields.len() < 15 {
        return 0.0;
    }
    let utime: f64 = fields[13].parse().unwrap_or(0.0);
    let stime: f64 = fields[14].parse().unwrap_or(0.0);
    let clk_tck = clk_tck();
    (utime + stime) / clk_tck
}

fn clk_tck() -> f64 {
    #[cfg(unix)]
    {
        let tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if tck > 0 {
            return tck as f64;
        }
    }
    100.0
}

/// Cleanup IPC socket files matching the benchmark pattern.
pub(crate) fn cleanup_ipc_sockets() {
    let pattern = "/tmp/omq-bench-cmp-*";
    if let Ok(entries) = glob_paths(pattern) {
        for path in entries {
            std::fs::remove_file(&path).ok();
        }
    }
}

fn glob_paths(pattern: &str) -> Result<Vec<String>, ()> {
    let output = Command::new("sh")
        .args(["-c", &format!("ls -1 {pattern} 2>/dev/null")])
        .output()
        .map_err(|_| ())?;
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(String::from)
        .collect())
}

// std::process::Child doesn't have wait_timeout in stable Rust.
// Polyfill it.
trait WaitTimeout {
    fn wait_timeout(&mut self, dur: Duration) -> std::io::Result<Option<std::process::ExitStatus>>;
}

impl WaitTimeout for Child {
    fn wait_timeout(&mut self, dur: Duration) -> std::io::Result<Option<std::process::ExitStatus>> {
        let start = Instant::now();
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(Some(status));
            }
            if start.elapsed() >= dur {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
