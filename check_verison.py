import sys
if sys.version_info.major >= 3 and sys.version_info.minor >= 10:
    sys.exit(0)
else:
    print("Old python")
    sys.exit(1)
