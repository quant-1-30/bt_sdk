import logging

logging.basicConfig(
    level=logging.DEBUG,  # 或 INFO
    format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s'
)
log = logging.getLogger(__name__)
