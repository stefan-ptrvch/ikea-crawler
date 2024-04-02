"""
Periodically runs IKEA crawler and stores products in mysql.
"""

import os
import time
import sentry_sdk
from dotenv import load_dotenv
from ikea import IKEACrawler
from ikea.pipeline import Pipeline


load_dotenv('config.env', override=True)
sentry_sdk.init(dsn=os.environ['SENTRY_DSN'])

while True:
    try:
        crawler = IKEACrawler()
        pipe = Pipeline()
        crawler.run()
        processed_products = pipe.process_items(crawler.products)
        pipe.save_items(processed_products)
        event = {
            'message': "Crawl finished.",
            'level': "info",
            'extra': pipe.get_report()
        }
        sentry_sdk.capture_event(event)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        time.sleep(60*60)
