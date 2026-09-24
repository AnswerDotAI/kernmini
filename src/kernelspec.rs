//! Kernelspec installation without jupyter_client: write or copy a kernelspec directory into a Jupyter kernels location.
use anyhow::Context;
use serde_json::{Map, Value};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn var(name: &str) -> Option<PathBuf> { env::var_os(name).filter(|v| !v.is_empty()).map(PathBuf::from) }

/// The user Jupyter data directory. `JUPYTER_DATA_DIR` overrides the platform default.
pub fn jupyter_data_dir() -> anyhow::Result<PathBuf> {
    if let Some(dir) = var("JUPYTER_DATA_DIR") { return Ok(dir); }
    let home = var(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).context("no home directory")?;
    Ok(if cfg!(target_os = "macos") { home.join("Library").join("Jupyter") } else if cfg!(windows) { var("APPDATA").unwrap_or(home).join("jupyter") } else { var("XDG_DATA_HOME").unwrap_or_else(|| home.join(".local").join("share")).join("jupyter") })
}

/// The kernels directory: `share/jupyter/kernels` under `prefix`, or in the user data directory.
pub fn kernels_dir(prefix: Option<&Path>) -> anyhow::Result<PathBuf> {
    Ok(match prefix { Some(prefix) => prefix.join("share").join("jupyter"), None => jupyter_data_dir()? }
    .join("kernels"))
}

/// Replace kernelspec `name` with an empty directory, returning it.
fn fresh(name: &str, prefix: Option<&Path>) -> anyhow::Result<PathBuf> {
    let dest = kernels_dir(prefix)?.join(name);
    if dest.exists() { fs::remove_dir_all(&dest)?; }
    fs::create_dir_all(&dest)?;
    Ok(dest)
}

/// Write a `kernel.json` for `argv`, which must include `{connection_file}`, replacing kernelspec `name`.
/// `extra` adds or replaces fields, such as `interrupt_mode`. Returns the kernelspec directory.
pub fn install_kernelspec(
    name: &str,
    argv: &[String],
    display_name: &str,
    language: &str,
    extra: Map<String, Value>,
    prefix: Option<&Path>,
) -> anyhow::Result<PathBuf> {
    let dest = fresh(name, prefix)?;
    let mut spec = Map::from_iter([("argv".into(), argv.into()), ("display_name".into(), display_name.into()), ("language".into(), language.into())]);
    spec.extend(extra);
    fs::write(dest.join("kernel.json"), serde_json::to_string_pretty(&spec)?)?;
    Ok(dest)
}

/// Copy kernelspec directory `src`, with `kernel.json` and any assets, replacing kernelspec `name`. Returns the destination.
pub fn install_kernelspec_dir(src: &Path, name: &str, prefix: Option<&Path>) -> anyhow::Result<PathBuf> {
    let dest = fresh(name, prefix)?;
    copy_dir(src, &dest).with_context(|| format!("copying {}", src.display()))?;
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
