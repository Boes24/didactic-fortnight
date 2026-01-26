import logging
from collections import deque
from flask import Flask, Response
import threading
import sys

log_buffer = deque(maxlen=500)

logging.getLogger("werkzeug").setLevel(logging.CRITICAL)

class BufferHandler(logging.Handler):
    def emit(self, record):
        log_buffer.append(self.format(record))

class StdoutTee:
    def __init__(self, original):
        self.original = original

    def write(self, message):
        if message.strip():
            log_buffer.append(message.strip())
        self.original.write(message)

    def flush(self):
        self.original.flush()

def setup_logging():
    handler = BufferHandler()
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s"
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    sys.stdout = StdoutTee(sys.__stdout__)
    sys.stderr = StdoutTee(sys.__stderr__)

app = Flask(__name__)

@app.route("/")
def show_logs():
    return Response("\n".join(log_buffer), mimetype="text/plain")

def start_webserver():
    thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=8000),
        daemon=True
    )
    thread.start()
