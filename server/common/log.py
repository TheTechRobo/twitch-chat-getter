import logging

logger = logging.getLogger(f"btt-{__name__}")
logging.basicConfig(level=logging.DEBUG, format="# [%(asctime)s] %(levelname)s %(message)s (%(lineno)d/%(funcName)s/%(filename)s)", encoding="utf-8", errors="backslashreplace")

