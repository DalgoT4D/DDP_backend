import re

with open("pyproject.toml", encoding="utf-8") as f:
    content = f.read()

match = re.search(r'dependencies\s*=\s*\[(.*?)\]', content, re.DOTALL)
if match:
    deps_block = match.group(1)
    deps = re.findall(r'"([^"]+)"', deps_block)
    with open("requirements.txt", "w", encoding="utf-8") as out:
        for dep in deps:
            out.write(dep + "\n")
