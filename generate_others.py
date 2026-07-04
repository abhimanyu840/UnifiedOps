with open("listener/syslog_trap_listener_dell_cdvl.py", "r", encoding="utf-8") as f:
    code = f.read()

import re

for loc in ["BCP", "SIFY"]:
    new_code = code.replace('LOCATION = "CDVL"', f'LOCATION = "{loc}"')
    new_code = new_code.replace('INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "SNMP_DELL_Bucket")', f'INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "Dell_{loc}_Bucket")')
    new_code = new_code.replace('Hi-Track / HDFC -- CDVL Dell pipeline', f'Hi-Track / HDFC -- {loc} Dell pipeline')
    new_code = new_code.replace('CDVL (NTT Chandivali)', loc)
    new_code = new_code.replace('-CDVL"', f'-{loc}"')
    
    with open(f"listener/syslog_trap_listener_dell_{loc.lower()}.py", "w", encoding="utf-8") as f:
        f.write(new_code)

