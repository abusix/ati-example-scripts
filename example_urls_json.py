#!/usr/bin/env python3
import json

# pip install stomp.py
import stomp
import time
import traceback
from datetime import datetime
from os import makedirs
from os.path import dirname
from urllib.parse import urlparse

SERVER = 'stream.abusix.net:61612'
SSL = True
CREDENTIALS = '<user>:<password>'
TOPIC = '<topic>'

# OUTPUT_FILE supports datetime formatting
# see https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior for details
OUTPUT_FILE = 'filtered/%Y-%m-%d/%H.txt'

# Possible fields:
# - data_origin
# - detected_text_language
# - source_ip_country_iso
# - url_tld
ALLOWLIST_FILTERS = {
    # 'detected_text_language': ['en']
}
BLOCKLIST_FILTERS = {
    # 'url_tld': ['com']
}


class STOMPListener(stomp.ConnectionListener):
    def on_error(self, frame):
        print('report_error', frame.body)

    def on_message(self, frame):
        msg_json = json.loads(frame.body)

        if 'url_tld' not in msg_json:
            msg_json['url_tld'] = urlparse(msg_json.get('url')).hostname.split('.')[-1]

        for field, allowed in ALLOWLIST_FILTERS.items():
            if msg_json.get(field) not in allowed:
                return

        for field, forbidden in BLOCKLIST_FILTERS.items():
            if msg_json.get(field) in forbidden:
                return

        msg_date = datetime.now()
        path = msg_date.strftime(OUTPUT_FILE)
        makedirs(dirname(path), exist_ok=True)

        with open(path, 'a') as f:
            f.write(msg_json.get('url') + '\n')


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
