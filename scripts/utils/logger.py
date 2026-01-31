import logging
from pathlib import Path
import sys
from datetime import datetime

class ColoredFormatter(logging.Formatter):
    COLORS = {
        logging.INFO: "\033[36m",    # Cyan
        logging.WARNING: "\033[33m", # Jaune
        logging.ERROR: "\033[31m",   # Rouge
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        message = super().format(record)
        if color:
            message = message.replace(record.levelname, f"{color}{record.levelname}{self.RESET}")
        return message

def setup_colored_logging():
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    main_script = Path(sys.argv[0]).stem
    if main_script == "api_server":
        log_file = log_dir / f"api_server_{now}.log"
    else:
        log_file = log_dir / f"{main_script}_{now}.log"

    # Formatters
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    color_formatter = ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s")

    # Handlers
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(file_formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(color_formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, stream_handler]
    )

setup_colored_logging()
logger = logging.getLogger(__name__)