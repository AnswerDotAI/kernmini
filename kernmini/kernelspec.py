"Kernelspec installation without jupyter_client: write or copy a kernelspec dir into a Jupyter kernels location."

import json, os, shutil, sys
from pathlib import Path


def jupyter_data_dir() -> Path:
    "The user Jupyter data directory (respects JUPYTER_DATA_DIR)."
    if (env := os.environ.get("JUPYTER_DATA_DIR")): return Path(env)
    home = Path.home()
    if sys.platform == "darwin": return home / "Library" / "Jupyter"
    if os.name == "nt": return Path(os.environ.get("APPDATA", home)) / "jupyter"
    return Path(os.environ.get("XDG_DATA_HOME") or home / ".local" / "share") / "jupyter"


def kernels_dir(user: bool = True, prefix: str | None = None) -> Path:
    "The kernels directory to install into: the user data dir, or `share/jupyter` under `prefix`."
    base = Path(prefix) / "share" / "jupyter" if prefix else jupyter_data_dir()
    return base / "kernels"


def install_kernelspec_dir(src_dir, name: str, user: bool = True, prefix: str | None = None) -> Path:
    "Copy an existing kernelspec directory (kernel.json plus any assets) into place; returns the destination."
    dest = kernels_dir(user=user, prefix=prefix) / name
    if dest.exists(): shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src_dir, dest)
    return dest


def install_kernelspec(name: str, argv: list[str], display_name: str, language: str,
    user: bool = True, prefix: str | None = None, **spec_kw) -> Path:
    "Write a kernel.json for `argv` (which must include '{connection_file}') and install it; returns the destination."
    dest = kernels_dir(user=user, prefix=prefix) / name
    if dest.exists(): shutil.rmtree(dest)
    dest.mkdir(parents=True)
    spec = dict(argv=argv, display_name=display_name, language=language) | spec_kw
    (dest / "kernel.json").write_text(json.dumps(spec, indent=1))
    return dest
