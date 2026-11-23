//! Kernelspec installation and discovery without jupyter_client.
use crate::{Error, ErrorKind, Result};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::{env, fs, path::{Path, PathBuf}};

const FALSY: [&str; 6] = ["no", "n", "false", "off", "0", "0.0"];
#[cfg(not(windows))]
const SYSTEM_DATA_DIRS: [&str; 2] = ["/usr/local/share/jupyter", "/usr/share/jupyter"];

fn var(name: &str) -> Option<PathBuf> { env::var_os(name).filter(|v| !v.is_empty()).map(PathBuf::from) }

fn env_flag(name: &str) -> Option<bool> {
    env::var(name).ok().map(|v| !FALSY.contains(&v.to_lowercase().as_str()))
}

/// The user Jupyter data directory. `JUPYTER_DATA_DIR` overrides the platform default.
pub fn jupyter_data_dir() -> Result<PathBuf> {
    if let Some(dir) = var("JUPYTER_DATA_DIR") { return Ok(dir); }
    let home = var(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).ok_or_else(|| Error::new(ErrorKind::Unavailable, "no home directory"))?;
    Ok(if cfg!(target_os = "macos") {
        if env_flag("JUPYTER_PLATFORM_DIRS") == Some(true) { home.join("Library").join("Application Support").join("jupyter") }
        else { home.join("Library").join("Jupyter") }
    } else if cfg!(windows) { var("APPDATA").unwrap_or(home).join("jupyter") }
    else { var("XDG_DATA_HOME").unwrap_or_else(|| home.join(".local").join("share")).join("jupyter") })
}

/// The kernels directory: `share/jupyter/kernels` under `prefix`, or in the user data directory.
pub fn kernels_dir(prefix: Option<&Path>) -> Result<PathBuf> {
    Ok(match prefix { Some(prefix) => prefix.join("share").join("jupyter"), None => jupyter_data_dir()? }.join("kernels"))
}

/// Resolve a launcher path without following symlinks (so a venv `bin/` symlink keeps its venv prefix).
fn resolve_launcher(p: &Path) -> Option<PathBuf> {
    if p.as_os_str().is_empty() { return None; }
    if p.is_absolute() { return Some(p.to_path_buf()); }
    if p.components().count() > 1 { return env::current_dir().ok().map(|cwd| cwd.join(p)); }
    let path_var = env::var_os("PATH")?;
    for dir in env::split_paths(&path_var) {
        let cand = dir.join(p);
        if fs::metadata(&cand).is_ok_and(|m| m.is_file()) { return Some(cand); }
    }
    None
}

fn prefix_of_exe(exe: &Path) -> Option<PathBuf> {
    let bin = exe.parent()?;
    let prefix = bin.parent()?;
    if prefix.as_os_str().is_empty() { None } else { Some(prefix.to_path_buf()) }
}

/// Python environment prefixes in search order: launcher prefix (preserving venv symlinks),
/// optional `current_exe` fallback when `launcher` is `None`, then `VIRTUAL_ENV` and `CONDA_PREFIX`.
pub fn env_prefixes(launcher: Option<&Path>) -> Vec<PathBuf> {
    let mut envs = Vec::new();
    let mut push = |p: PathBuf| { if !p.as_os_str().is_empty() && !envs.contains(&p) { envs.push(p); } };
    match launcher {
        Some(p) => { if let Some(prefix) = resolve_launcher(p).and_then(|r| prefix_of_exe(&r)) { push(prefix); } }
        None => {
            if let Some(arg0) = env::args_os().next().map(PathBuf::from)
                && let Some(prefix) = resolve_launcher(&arg0).and_then(|r| prefix_of_exe(&r)) { push(prefix); }
            if let Ok(exe) = env::current_exe() && let Some(prefix) = prefix_of_exe(&exe) { push(prefix); }
        }
    }
    for name in ["VIRTUAL_ENV", "CONDA_PREFIX"] { if let Some(p) = var(name) { push(p); } }
    envs
}

fn owned_by_current_user(p: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        unsafe extern "C" { fn geteuid() -> u32; }
        fs::metadata(p).is_ok_and(|m| m.uid() == unsafe { geteuid() })
    }
    #[cfg(not(unix))]
    { p.exists() }
}

fn system_data_dirs() -> Vec<PathBuf> {
    #[cfg(not(windows))]
    { SYSTEM_DATA_DIRS.into_iter().map(PathBuf::from).collect() }
    #[cfg(windows)]
    { var("PROGRAMDATA").into_iter().map(|d| d.join("jupyter")).collect() }
}

/// Jupyter kernelspec directories in `jupyter_core` search order, with legacy IPython last.
/// `launcher` selects the executable path used to infer the owning Python environment prefix
/// without following symlinks (passing `None` uses `argv[0]`, then `current_exe()`).
pub fn kernelspec_dirs(launcher: Option<&Path>) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = env::var_os("JUPYTER_PATH")
        .into_iter()
        .flat_map(|v| env::split_paths(&v).collect::<Vec<_>>())
        .filter(|p| !p.as_os_str().is_empty())
        .collect();
    let user = jupyter_data_dir().ok();
    let system = system_data_dirs();
    let envs = env_prefixes(launcher);
    let env_dirs: Vec<PathBuf> = envs.iter().map(|p| p.join("share").join("jupyter")).filter(|p| !system.contains(p)).collect();
    let conda_prefix = var("CONDA_PREFIX");
    let conda_non_base = env::var("CONDA_DEFAULT_ENV").is_ok_and(|n| n != "base");
    let is_env = |p: &PathBuf| {
        let is_conda = conda_non_base && conda_prefix.as_ref().is_some_and(|c| p.starts_with(c));
        (p.join("pyvenv.cfg").exists() || is_conda) && owned_by_current_user(p)
    };
    let prefer_env = env_flag("JUPYTER_PREFER_ENV_PATH").unwrap_or_else(|| envs.iter().any(is_env));
    if prefer_env {
        dirs.extend(env_dirs);
        if let Some(u) = user { dirs.push(u); }
    } else {
        if let Some(u) = user { dirs.push(u); }
        dirs.extend(env_dirs);
    }
    dirs.extend(system);
    let mut out: Vec<PathBuf> = dirs.into_iter().map(|d| d.join("kernels")).collect();
    if let Some(ipy) = var("IPYTHONDIR").or_else(|| var(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).map(|h| h.join(".ipython"))) {
        out.push(ipy.join("kernels"));
    }
    out
}

/// A discovered kernelspec (`kernel.json` and its containing directory).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KernelSpec {
    pub name: String,
    pub spec: Value,
    pub dir: PathBuf,
}

impl KernelSpec {
    /// Launch command with `{resource_dir}` filled in and bare `python`/`python3` resolved to the owning environment.
    pub fn argv(&self, launcher: Option<&Path>) -> Vec<String> {
        let dir = self.dir.to_string_lossy();
        let mut argv: Vec<String> =
            self.spec["argv"].as_array().into_iter().flatten().filter_map(Value::as_str).map(|a| a.replace("{resource_dir}", &dir)).collect();
        if matches!(argv.first().map(String::as_str), Some("python" | "python3")) {
            let envs = env_prefixes(launcher);
            let own = envs.iter().filter(|e| self.dir.starts_with(e.join("share").join("jupyter")));
            let python_rel = if cfg!(windows) { Path::new("Scripts/python.exe") } else { Path::new("bin/python") };
            if let Some(python) = own.chain(&envs).map(|e| e.join(python_rel)).find(|p| p.exists()) {
                argv[0] = python.to_string_lossy().into_owned();
            }
        }
        argv
    }

    pub fn env(&self) -> Map<String, Value> { self.spec["env"].as_object().cloned().unwrap_or_default() }

    /// Interrupt by a control-channel `interrupt_request` rather than SIGINT?
    pub fn message_interrupt(&self) -> bool { self.spec["interrupt_mode"] == "message" }
}

/// Discover every launchable kernelspec in `kernelspec_dirs(launcher)` order (first match wins).
pub fn discover_kernelspecs(launcher: Option<&Path>) -> BTreeMap<String, KernelSpec> {
    let mut specs = BTreeMap::new();
    for kernels in kernelspec_dirs(launcher) {
        let Ok(entries) = fs::read_dir(&kernels) else { continue };
        for dir in entries.flatten().map(|e| e.path()) {
            let Ok(text) = fs::read_to_string(dir.join("kernel.json")) else { continue };
            let name = dir.file_name().unwrap_or_default().to_string_lossy().to_lowercase();
            match serde_json::from_str(&text) {
                Ok(spec) => _ = specs.entry(name.clone()).or_insert(KernelSpec { name, spec, dir }),
                Err(error) => eprintln!("ignoring kernelspec {}: {error}", dir.display()),
            }
        }
    }
    specs
}

/// Replace kernelspec `name` with an empty directory, returning it.
fn fresh(name: &str, prefix: Option<&Path>) -> Result<PathBuf> {
    let dest = kernels_dir(prefix)?.join(name);
    if dest.exists() { fs::remove_dir_all(&dest)?; }
    fs::create_dir_all(&dest)?;
    Ok(dest)
}

/// Write a `kernel.json` for `argv`, which must include `{connection_file}`, replacing kernelspec `name`.
/// `extra` adds or replaces fields, such as `interrupt_mode`. Returns the kernelspec directory.
pub fn install_kernelspec(
    name: &str, argv: &[String], display_name: &str, language: &str, extra: Map<String, Value>, prefix: Option<&Path>) -> Result<PathBuf> {
    let dest = fresh(name, prefix)?;
    let mut spec = Map::from_iter([("argv".into(), argv.into()), ("display_name".into(), display_name.into()), ("language".into(), language.into())]);
    spec.extend(extra);
    fs::write(dest.join("kernel.json"), serde_json::to_string_pretty(&spec)?)?;
    Ok(dest)
}

/// Copy kernelspec directory `src`, with `kernel.json` and any assets, replacing kernelspec `name`. Returns the destination.
pub fn install_kernelspec_dir(src: &Path, name: &str, prefix: Option<&Path>) -> Result<PathBuf> {
    let dest = fresh(name, prefix)?;
    copy_dir(src, &dest).map_err(|error| Error::from(error).context(format!("copying {}", src.display())))?;
    Ok(dest)
}

/// Copy the contents of `src` into `dest`, following symlinks.
fn copy_dir(src: &Path, dest: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(src)? {
        let from = entry?.path();
        let to = dest.join(from.file_name().unwrap_or_default());
        if fs::metadata(&from)?.is_dir() {
            fs::create_dir_all(&to)?;
            copy_dir(&from, &to)?;
        } else { fs::copy(&from, &to)?; }
    }
    Ok(())
}
