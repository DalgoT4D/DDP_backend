import shlex
import subprocess


def runcmd(cmd, cwd):
    """runs a shell command in a specified working directory"""
    return subprocess.Popen(shlex.split(cmd), cwd=str(cwd))
