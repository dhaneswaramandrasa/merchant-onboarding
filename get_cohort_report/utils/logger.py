import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os

def setup_logger():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"cohort_etl_{today}.log")

    logger = logging.getLogger("cohort_etl")
    logger.setLevel(logging.INFO)

    # Hindari duplicate handler kalau logger dipanggil ulang
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    # File handler (rotate per size, tapi tetap per hari)
    fh = RotatingFileHandler(
        log_file,
        maxBytes=5_000_000,   # 5MB
        backupCount=5
    )
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger
