import re,sys;

print(
    re.sub(r"rootProject.name\s*=\s*[\'\"](.*?)[\"\']", "rootProject.name = \"\\1-3\"", sys.stdin.read()))
