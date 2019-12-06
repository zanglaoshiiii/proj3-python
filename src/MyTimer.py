from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse
import xmlrpc.client
import random
import time
import threading

class MyTimer():

    def __init__(self):
        self.lower = 250
        self.upper = 500
        self.start = 0
        self.timeout = 0
        self.reset()

    def reset(self):
        self.start = int(time.time() * 1000)
        self.timeout = random.randint(self.lower,self.upper)

    def currentTime(self):
        now = int(time.time() * 1000) - self.start

        return now

    def setTimeout(self, timeout):
        self.timeout = timeout