from ftplib import FTP, error_perm
import os
from pathlib import Path

from dotenv import load_dotenv

# --------------------------------------------------
# Load credentials
# --------------------------------------------------
load_dotenv()

FTP_USERNAME = os.getenv("AVISO_FTP_USERNAME")
FTP_PASSWORD = os.getenv("AVISO_FTP_PASSWORD")

if not FTP_USERNAME or not FTP_PASSWORD:
    raise RuntimeError("FTP credentials not found in .env file")

# --------------------------------------------------
# Configuration
# --------------------------------------------------
FTP_HOST = "ftp-access.aviso.altimetry.fr"
REMOTE_BASE_DIR = "/swot_products/l3_karin_nadir/l3_lr_ssh/v3_0/Expert/reproc"
LOCAL_OUT_DIR = Path("/Users/bertrava/data/SWOT_L3/v3_0/")

CYCLE_START = 1
CYCLE_END = 18
ONLY_PASSES = ()


LOCAL_OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# FTP download logic
# --------------------------------------------------
def download_file(ftp: FTP, remote_filename: str, local_path: Path):
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_filename}", f.write)


with FTP(FTP_HOST) as ftp:
    print(f"Connecting to {FTP_HOST} …")
    ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
    print("Connected.")

    ftp.cwd(REMOTE_BASE_DIR)

    for cycle in range(CYCLE_START, CYCLE_END + 1):
        cycle_dir = f"cycle_{cycle:03d}"
        print(f"\nProcessing {cycle_dir}")

        try:
            ftp.cwd(cycle_dir)
        except error_perm:
            print(f"  ⚠ Directory not found, skipping")
            continue

        try:
            filenames = ftp.nlst()
        except error_perm:
            print(f"  ⚠ Cannot list files, skipping")
            ftp.cwd("..")
            continue
        
        if ONLY_PASSES:
            matched_files = [
                f for f in filenames
                if f.split("_")[6] in ONLY_PASSES
            ]

            if not matched_files:
                print("  No matching files found")
                ftp.cwd("..")
                continue
        else:
            matched_files = filenames

        for filename in matched_files:
            local_file = LOCAL_OUT_DIR / filename

            if local_file.exists():
                print(f"  Skipping existing file: {filename}")
                continue

            print(f"  Downloading {filename}")
            download_file(ftp, filename, local_file)

        ftp.cwd("..")

    print("\nAll done.")
