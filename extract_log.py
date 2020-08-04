import logging
import logging.handlers
from datetime import datetime 

def listener_configurer(file_name, file_name_append_timestamp):
    # append datetime stamp to file name
    if file_name_append_timestamp:
        time_stamp = datetime.today().strftime('%Y%m%d_%H%M%S')
        file_name = f"{file_name}_{time_stamp}.log"
    else: 
        file_name = f"{file_name}.log"

    # create new filename with datetime stamp
    logger = logging.getLogger()
    file_handler = logging.FileHandler(filename=file_name, mode='a')
    formatter = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def listener(queue, configurer, file_name, file_name_append_timestamp): 
    configurer(file_name, file_name_append_timestamp)
    while True:
        try:
            record = queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                logger.info("Terminate logging.")
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            import sys, traceback
            print('Error:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

def worker_configurer(queue):
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.DEBUG)