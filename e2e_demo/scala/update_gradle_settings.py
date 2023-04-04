import re,sys;

print(
    re.sub(r"rootProject.name\s*=\s*[\'\"](.*?)(-3)?(_2.1[12])?[\"\']", "rootProject.name = \"\\1-3\\3\"", sys.stdin.read()))
