#!/usr/bin/env python3
import json

# pip install stomp.py
import stomp
import time
import traceback
from base64 import b64decode
from datetime import datetime
from os import makedirs
from os.path import join as path_join

SERVER = 'stream.abusix.net:61612'
SSL = True
CREDENTIALS = '<user>:<pass>'
TOPIC = '<topic>'

WRITE_RAW_FILES = True
# OUTPUT_DIRECTORY supports datetime formatting
# see https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior for details
OUTPUT_DIRECTORY = 'filtered/%Y-%m-%d/%H/'

# Possible fields:
# - content_type
# - data_origin
# - detected_extension
# - filename
# - smtp_mail_from
# - source_ip
# - source_ip_country_iso
# - source_ip_rir
# - source_port
ALLOWLIST_FILTERS = {
    # 'filename': ['attachment.txt']
}
BLOCKLIST_FILTERS = {
    # 'detected_extension': ['exe']
}
LOG_FIELDS = ['filename', 'content_type']

class STOMPListener(stomp.ConnectionListener):
    def on_error(self, frame):
        print('report_error', frame.body)

    def on_message(self, frame):
        msg_json = json.loads(frame.body)

        for field, allowed in ALLOWLIST_FILTERS.items():
            if msg_json.get(field) not in allowed:
                return

        for field, forbidden in BLOCKLIST_FILTERS.items():
            if msg_json.get(field) in forbidden:
                return

        log_str = f'RECEIVED file'
        for field in LOG_FIELDS:
            log_str = log_str + f' {field}: {msg_json.get(field)}'
        if LOG_FIELDS:
            print(log_str)

        msg_date = datetime.now()
        path = msg_date.strftime(OUTPUT_DIRECTORY)
        makedirs(path, exist_ok=True)

        if WRITE_RAW_FILES:
            with open(path_join(path, str(hash(frame.body)) + '.file'), 'wb') as f:
                f.write(b64decode(msg_json['attachment_base64_encoded']))
        else:
            with open(path_join(path, str(hash(frame.body)) + '.json'), 'w') as f:
                json.dump(msg_json, f)


def listen():
    server, port = SERVER.split(':', 1)
    username, password = CREDENTIALS.split(':', 1)

    conn = stomp.Connection([(server, port)], heartbeats=(10000, 10000))

    if SSL:
        conn.set_ssl(for_hosts=[(server, port)])

    conn.set_listener('', STOMPListener())
    conn.connect(username, password, wait=True)

    # Subscribe to topic (default: shared with other connections with the same username)
    conn.subscribe(destination=TOPIC, id='1234')

    # To disable load-balancing (shared subscriptions):
    # conn.subscribe(destination=TOPIC, id='1234', headers={'channel': username + 'something_unique'})

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        pass

    conn.unsubscribe(id='1234')
    conn.disconnect()


if __name__ == '__main__':
    try:
        listen()
    except KeyboardInterrupt:
        pass
    except Exception:
        traceback.print_exc()
