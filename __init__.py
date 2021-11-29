import json
import logging
from datetime import datetime

import azure.functions as func


def main(event: func.EventGridEvent, outputblob: func.Out[str]):

    storage_account = "ywstorage1"
    mount_point = "/mnt/data1"
    relative_path = "/eventgrid/source1/"

    data_url = event.get_json()["url"]
    logging.info('Trigger manifest file function, data url = %s', data_url)

    #result = json.dumps({
    #    'id': event.id,
    #    'data': event.get_json(),
    #    'topic': event.topic,
    #    'subject': event.subject,
    #    'event_type': event.event_type,
    #})
    #logging.info('Python EventGrid trigger processed an event: %s', result)

    manifest = {}
    path = data_url.split(f"https://{storage_account}.blob.core.windows.net/data",1)[1] 
    #logging.info("path: %s", path)
    manifest["path"] = mount_point + path
    #logging.info("dbfs path: %s", manifest["path"])
    table_file = path.split(f"{relative_path}",1)[1].split("/")
    manifest["table_name"] = table_file[0]
    #logging.info("table_name: %s", manifest["table_name"])
    manifest["file_name"] = table_file[1]
    #logging.info("file_name: %s", manifest["file_name"])
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S")
    manifest["eventTime"] = dt_string
    #logging.info("eventTime: %s", manifest["eventTime"])
    manifest["size"] = event.get_json()["contentLength"]
    #logging.info("size: %s", manifest["size"])

    logging.info('Manifest File: %s', json.dumps(manifest, indent=None))
    
    outputblob.set(json.dumps(manifest, indent=None))
