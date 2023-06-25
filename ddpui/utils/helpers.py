import shlex
import subprocess


def runcmd(cmd, cwd):
    """runs a shell command in a specified working directory"""
    return subprocess.run(shlex.split(cmd), cwd=str(cwd), check=True)


def remove_nested_attribute(obj: dict, attr: str) -> dict:
    """
    this function searches for `attr` in the JSON object
    and removes any occurences it finds
    """
    if attr in obj:
        del obj[attr]

    for key in obj:
        assert key != attr
        val = obj[key]

        if isinstance(val, dict):
            obj[key] = remove_nested_attribute(val, attr)

        elif isinstance(val, list):
            for list_idx, list_val in enumerate(val):
                if isinstance(list_val, dict):
                    val[list_idx] = remove_nested_attribute(list_val, attr)

    return obj
