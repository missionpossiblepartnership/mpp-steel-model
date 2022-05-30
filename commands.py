import subprocess
from pathlib import Path

import typer

PROJECT_ROOT = str(Path(__file__).parent.resolve())

cli = typer.Typer()


@cli.command()
def mypy():
    """Run Mypy (configured in pyproject.toml)"""
    subprocess.call(["mypy", "."])


@cli.command()
def test():
    subprocess.call(["pytest"])


if __name__ == "__main__":
    cli()
