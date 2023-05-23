import shlex
import subprocess
from typing import Union
from typing import List
import humps


def runcmd(cmd, cwd):
    """runs a shell command in a specified working directory"""
    return subprocess.run(shlex.split(cmd), cwd=str(cwd), check=True)


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


def remove_nested_attribute(obj: dict, attr: str) -> dict:
    """this function searches for `attr` in the JSON object and removes any occurences it finds"""
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
