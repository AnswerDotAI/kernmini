use kernmini::{ConnectionFile, ConnectionInfo, discover_kernelspecs, install_kernelspec, kernelspec_dirs};
use serde_json::Map;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("kernmini-{tag}-{}", Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}
impl Drop for TempDir { fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); } }

#[test]
fn connection_file_allocates_distinct_ports_and_cleans_up() {
    let conn = ConnectionFile::create_local("kernmini-test").unwrap();
    let info = conn.info().clone();
    let ports = HashSet::from([info.shell_port, info.iopub_port, info.stdin_port, info.control_port, info.hb_port]);
    assert_eq!(ports.len(), 5);
    assert!(ports.iter().all(|&p| p > 0));
    assert_eq!(ConnectionInfo::read(conn.path()).unwrap(), info);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(fs::metadata(conn.path()).unwrap().permissions().mode() & 0o777, 0o600);
    }
    let path = conn.path().to_path_buf();
    drop(conn);
    assert!(!path.exists());
}

#[test]
fn symlinked_venv_launcher_preserves_venv_kernelspecs_and_python() {
    let tmp = TempDir::new("kspec");
    let venv = tmp.0.join("venv");
    let cache_bin = tmp.0.join("cache").join("bin");
    fs::create_dir_all(venv.join("bin")).unwrap();
    fs::create_dir_all(&cache_bin).unwrap();
    fs::write(venv.join("pyvenv.cfg"), "home = /usr/bin\n").unwrap();
    let venv_python = venv.join("bin").join("python");
    fs::write(&venv_python, "").unwrap();
    let real_bin = cache_bin.join("jupygate");
    fs::write(&real_bin, "").unwrap();
    let launcher = venv.join("bin").join("jupygate");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&real_bin, &launcher).unwrap();
    #[cfg(not(unix))]
    fs::copy(&real_bin, &launcher).unwrap();

    let spec_name = format!("ktest_{}", Uuid::new_v4().simple());
    install_kernelspec(
        &spec_name,
        &["python".into(), "-m".into(), "demo".into(), "-f".into(), "{connection_file}".into()],
        "Demo",
        "python",
        Map::new(),
        Some(&venv),
    )
    .unwrap();

    let dirs = kernelspec_dirs(Some(&launcher));
    assert!(dirs.contains(&venv.join("share").join("jupyter").join("kernels")));
    let specs = discover_kernelspecs(Some(&launcher));
    let spec = specs.get(&spec_name).expect("discovered symlinked venv kernelspec");
    assert_eq!(spec.argv(Some(&launcher))[0], venv_python.to_string_lossy());
}
