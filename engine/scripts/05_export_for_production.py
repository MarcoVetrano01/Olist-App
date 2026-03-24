import os
import shutil
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
ENGINE_ROOM_DIR = SCRIPT_DIR.parent
PROJECT_ROOT = ENGINE_ROOM_DIR.parent

EXPORTS_DIR = ENGINE_ROOM_DIR / 'exports'
STREAMLIT_DIR = PROJECT_ROOT / 'streamlit_app'

STREAMLIT_DATA = STREAMLIT_DIR / 'data'
STREAMLIT_MODELS = STREAMLIT_DIR / 'models'

def setup_production_folder():
    print("Setting up production folder...")
    STREAMLIT_DIR.mkdir(parents = True, exist_ok=True)
    STREAMLIT_DATA.mkdir(parents = True, exist_ok=True)
    STREAMLIT_MODELS.mkdir(parents = True, exist_ok=True)
    print("Production folders are ready.")

def transfer_files():

    if not EXPORTS_DIR.exists():
        print("exports/ dir does not exists. Run files 03 and 04 first.")
        return

    print("Transferring files to production folder...")

    for csv_file in EXPORTS_DIR.glob('*.csv'):
        shutil.copy2(csv_file, STREAMLIT_DATA / csv_file.name)
        print(f"Copied {csv_file.name} to {STREAMLIT_DATA}")

    models_dir = EXPORTS_DIR / 'models'
    if models_dir.exists():
        for model_folder in models_dir.iterdir():
            if model_folder.name == 'bert':
                print(f"Skipping {model_folder.name} model transfer due to size constraints.")
                continue
            else:
                print(f"Transferring {model_folder.name} model...")
                shutil.copytree(model_folder, STREAMLIT_MODELS / model_folder.name, dirs_exist_ok=True)
    else:
        print("No models/ directory found in exports/. Skipping model transfer.")
    
    print("File transfer complete.")

if __name__ == '__main__':
    setup_production_folder()
    transfer_files()