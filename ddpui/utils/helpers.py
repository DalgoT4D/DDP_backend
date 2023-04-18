import shlex
import subprocess
from typing import Union
from typing import List
import humps


def runcmd(cmd, cwd):
    """runs a shell command in a specified working directory"""
    return subprocess.Popen(shlex.split(cmd), cwd=str(cwd))


def api_res_camel_case(api_res: Union[List[dict], dict, str]):
    """Convert strings and dictionary keys to camel case"""
    if isinstance(api_res, str):
        return humps.camelize(api_res)

    if isinstance(api_res, dict):
        camel_dict_obj = {}
        for old_key, value in api_res.items():
            new_key = humps.camelize(old_key)
            camel_dict_obj[new_key] = value

        return camel_dict_obj

    if isinstance(api_res, list):
        ans = []
        for dict_obj in api_res:
            camel_dict_obj = {}
            if isinstance(dict_obj, dict):
                for old_key, value in dict_obj.items():
                    new_key = humps.camelize(old_key)
                    camel_dict_obj[new_key] = value

            ans.append(camel_dict_obj)

        return ans

    return api_res
