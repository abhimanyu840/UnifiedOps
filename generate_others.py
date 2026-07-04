with open("listener/syslog_trap_listener_dell_cdvl.py", "r", encoding="utf-8") as f:
    code = f.read()

import re

for loc in ["BCP", "SIFY"]:
    new_code = code.replace('LOCATION = "CDVL"', f'LOCATION = "{loc}"')
    new_code = new_code.replace('INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "SNMP_DELL_Bucket")', f'INFLUX_BUCKET = os.environ.get("HITRACK_INFLUX_BUCKET", "Dell_{loc}_Bucket")')
    new_code = new_code.replace('Hi-Track / HDFC -- CDVL Dell pipeline', f'Hi-Track / HDFC -- {loc} Dell pipeline')
    target_path = f"listener/syslog_trap_listener_dell_{loc.lower()}.py"
    
    # Preserve existing IP dictionaries if target exists
    try:
        with open(target_path, "r", encoding="utf-8") as tf:
            target_code = tf.read()
        target_filter = re.search(r'IP_FILTER: dict\[str, str\] = \{[^}]+\}', target_code).group(0)
        target_names = re.search(r'IP_TO_STORAGE_NAME: dict\[str, str\] = \{[^}]+\}', target_code).group(0)
    except Exception:
        target_filter = 'IP_FILTER: dict[str, str] = {}'
        target_names = 'IP_TO_STORAGE_NAME: dict[str, str] = {}'
    
    new_code = re.sub(r'IP_FILTER: dict\[str, str\] = \{[^}]+\}', target_filter.replace('\\', '\\\\'), new_code)
    new_code = re.sub(r'IP_TO_STORAGE_NAME: dict\[str, str\] = \{[^}]+\}', target_names.replace('\\', '\\\\'), new_code)
    
    with open(target_path, "w", encoding="utf-8") as f:
        f.write(new_code)

