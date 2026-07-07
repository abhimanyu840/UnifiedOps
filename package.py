import os
import shutil

src_dir = os.getcwd()
dest_dir = os.path.join(src_dir, 'UnifiedOpsV2.0.3_package')

if os.path.exists(dest_dir):
    shutil.rmtree(dest_dir)
os.makedirs(dest_dir)

def ignore_patterns(path, names):
    ignored = set()
    for name in names:
        # Ignore virtual environments, cache, node_modules, local data, and git folders
        if name in ('.venv', '__pycache__', 'node_modules', 'data', '.git', 'unifiedopsv2_offline_packages'):
            ignored.add(name)
        # Exclude podman from deploy as requested
        if 'deploy' in path and name == 'podman':
            ignored.add(name)
    return ignored

dirs_to_copy = ['server', 'frontend', 'listener', 'scripts', 'deploy', 'offline_packages_UnifiedOpsv2.0.3']
files_to_copy = ['.env', 'INSTALL.md', 'VERSION']

for d in dirs_to_copy:
    src_path = os.path.join(src_dir, d)
    if os.path.exists(src_path):
        shutil.copytree(src_path, os.path.join(dest_dir, d), ignore=ignore_patterns)

for f in files_to_copy:
    src_path = os.path.join(src_dir, f)
    if os.path.exists(src_path):
        shutil.copy2(src_path, dest_dir)

print(f"Successfully packaged files into {dest_dir}")
