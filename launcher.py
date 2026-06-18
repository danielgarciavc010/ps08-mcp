import os
import sys
import subprocess
from pathlib import Path

APP_DIR = Path("home/cdsw/mcp-app")

def run_command(command, env=None):
    print(f"Ejecutando comando: {' '.join(command)}", flush=True)
    subprocess.run(command, check=True, env=env)


def install_requirements():
    requirements_path = APP_DIR / "requirements.txt"

    if not requirements_path.exists():
        print("No se ha encontrado requirements.txt. Se continúa sin instalar dependencias.", flush=True)
        return

    print("Instalando dependencias desde requirements.txt...", flush=True)

    run_command([
        sys.executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "pip"
    ])

    run_command([
        sys.executable,
        "-m",
        "pip",
        "install",
        "-r",
        "home/cdsw/mcp-app/requirements.txt"
    ])

    print("Dependencias instaladas correctamente.", flush=True)


def main():
    print("==== Inicio launch_app.py ====", flush=True)
    print(f"Python usado: {sys.executable}", flush=True)
    print(f"Directorio actual: {APP_DIR}", flush=True)
    print("\n".join(str(p) for p in APP_DIR.iterdir()), flush=True)

    app_port = os.environ.get("CDSW_APP_PORT")

    if not app_port:
        raise RuntimeError(
            "No se ha encontrado CDSW_APP_PORT. "
            "Este script debe ejecutarse desde Cloudera AI Workbench Applications."
        )

    install_requirements()

    env = os.environ.copy()
    env["PORT"] = app_port
    env["HOST"] = "127.0.0.1"

    print(f"Lanzando app.py en 127.0.0.1:{app_port}", flush=True)

    run_command([
        sys.executable,
        "home/cdsw/mcp-app/app.py"
    ], env=env)


if __name__ == "__main__":
    main()