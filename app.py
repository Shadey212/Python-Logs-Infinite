import os
import time
from flask import Flask
from threading import Thread
from faker import Faker
import random
import json
from logtail import LogtailHandler
import logging
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)

running = True  # Start log generation by default
log_count = 0
handler = LogtailHandler(source_token=os.getenv('LOGTAIL_SOURCE_TOKEN'))

# Create a logger and configure it to use the LogtailHandler
logger = logging.getLogger(__name__)
logger.handlers = []
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


def generate_logs():
    global running
    global log_count
    fake = Faker()
    while running:
        log = generate_log(fake)

        log_level = random.choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
        if log_level == 'DEBUG':
            logger.debug(log)
        elif log_level == 'INFO':
            logger.info(log)
        elif log_level == 'WARNING':
            logger.warning(log)
        elif log_level == 'ERROR':
            logger.error(log)
        elif log_level == 'CRITICAL':
            logger.critical(log)

        log_count += 1
        time.sleep(0.03)  # Pause for 30ms


def generate_log(fake):
    method = random.choice(["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
    status = random.choice([200, 201, 204, 301, 302, 400, 401, 403, 404, 405, 500, 501, 502, 503])
    url = fake.uri_path(deep=3)
    ip = fake.ipv4()
    user_agent = fake.user_agent()
    protocol = random.choice(["HTTP/1.0", "HTTP/1.1", "HTTP/2"])
    timestamp = time.strftime('%d/%b/%Y:%H:%M:%S %z')

    log_data = {
        'ip': ip,
        'timestamp': timestamp,
        'request': {
            'method': method,
            'url': url,
            'protocol': protocol
        },
        'status': status,
        'user_agent': user_agent,
        'additional_info': {
            'user_id': random.randint(1, 100),
            'referrer': fake.uri(),
            'response_time': round(random.uniform(0.1, 10.0), 2),
            'bytes_sent': random.randint(100, 10000),
            'cookies': {
                'session_id': fake.uuid4(),
                'visitor_id': fake.uuid4()
            },
            'headers': {
                'Accept-Language': fake.language_code(),
                'X-Forwarded-For': fake.ipv4(),
                'Referer': fake.uri(),
                'User-Agent': fake.user_agent(),
                'Content-Type': fake.mime_type(),
                'Authorization': fake.md5(),
            }
        }
    }

    log_line = json.dumps(log_data)
    return log_line

if __name__ == "__main__":
    app.wsgi_app = ProxyFix(app.wsgi_app)
    app.run()