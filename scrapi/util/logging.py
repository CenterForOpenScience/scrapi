import logging
from datetime import datetime

from fluent import sender


class FluentHandler(logging.Handler):
    '''
    Logging Handler for fluent.
    '''
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 timeout=3.0,
                 verbose=False):

        self.tag = tag
        self.sender = sender.FluentSender(tag,
                                          host=host, port=port,
                                          timeout=timeout, verbose=verbose)
        logging.Handler.__init__(self)

    def emit(self, record):
        data = self.format(record)
        data = {
            'level': record.levelname,
            'message': record.msg,
            'source': record.name,
            'date': datetime.fromtimestamp(record.created).isoformat(),
            'fullPath': record.pathname,
            'uptime': record.relativeCreated
        }
        self.sender.emit(None, data)

    def close(self):
        self.acquire()
        try:
            self.sender._close()
            logging.Handler.close(self)
        finally:
            self.release()
