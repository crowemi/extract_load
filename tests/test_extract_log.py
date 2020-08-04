import sys, os
import unittest
import logging
import multiprocessing

import extract_log

class TestExtractLog(unittest.TestCase):

    def setUp(self):
        pass

    def test_listener_configurer(self):
        log_file_name = 'test_log'
        extract_log.listener_configurer(log_file_name, False)
        logger = logging.getLogger()
        logger.warn("test warning")
        file_exists = os.path.exists(f'{log_file_name}.log')
        self.assertTrue(file_exists) 
        logging.shutdown()
        os.remove(f'{log_file_name}.log')

    def test_listener(self):
        queue = multiprocessing.Queue(-1)
        listener = multiprocessing.Process(
            target=extract_log.listener,
            args=(queue, extract_log.listener_configurer, 'test_log.log', False)
        )
        listener.start()

        extract_log.worker_configurer(queue)
        logger = logging.getLogger()
        logger.log(logging.DEBUG, "some message from the worker")


if __name__ == '__main__':
    unittest.main()