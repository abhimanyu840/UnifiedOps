
import glob
for file in glob.glob("listener/syslog_trap_listener_dell_*.py"):
    with open(file, "r", encoding="utf-8") as f:
        code = f.read()
    
    if "from pysnmp import debug" not in code:
        debug_code = """import os
import json

# Enable pysnmp debugging if requested
if os.environ.get("HITRACK_DEBUG", "").lower() == "true":
    from pysnmp import debug
    debug.setLogger(debug.Debug('all'))"""
        
        code = code.replace("import os\nimport json", debug_code)
        
        with open(file, "w", encoding="utf-8") as f:
            f.write(code)
            
print("Debug patched.")

