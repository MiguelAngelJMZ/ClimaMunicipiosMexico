import logging
import uuid
from logging.handlers import TimedRotatingFileHandler

class UUIDFileHandler(TimedRotatingFileHandler):
    def __int__(self, path, fileName, mode):
        fullpath = f"{path}/{fileName}-{uuid.uuid1()}"
        super(UUIDFileHandler, self).__init__(filename=fullpath,when='midnight')
