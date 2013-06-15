#!/usr/bin/python
'''
UPLOAD FILE TO PUBLIC INTERNET SERVER
Moves successful sent files to sent folder

Author: Robert Oliveira <robert@marestelecom.com.br>
To: Fujitec
'''

import os
import sys
import fcntl
import fnmatch
import ftplib
import logging
import tempfile
import datetime

BILLING_PATH="/home/rsync/log"
BILLING_MASK="L*.BIN"
LOCK_FILE="/tmp/upload.lock"
LOG_FILE="/home/rsync/log/upload.log"
LOG_FORMAT="%(asctime)-15s %(message)s"

class Mutex:
    def __init__(self, filename):
        '''class constructor'''
        self.filename = filename

    def __enter__(self):
        '''called when entering on "with" structure'''
        self.lockfile = open(self.filename, "w")
        fcntl.lockf(self.lockfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.lockfile.write("%i\n" % os.getpid())
	return self.lockfile

    def __exit__(self, *args, **kwargs):
        '''called when exiting of "with" structure'''
        self.lockfile.close()


class FtpUpload(object):
    '''Base class for FTP upload'''
    MAX_RETRIES = 3

    def __init__(self, *args, **kwargs):
        # stores parameters to establish ftp new session
        self.ftp_args = args
        self.ftp_kwargs = kwargs
        self.session = None

    def newSession(self):
        self.session = ftplib.FTP(*self.ftp_args, **self.ftp_kwargs)

    def __enter__(self):
        '''called when entering on "with" structure'''
        return self

    def __exit__(self, *args, **kwargs):
        '''called when exiting of "with" structure'''
        if self.session:
            self.session.quit()

    def put(self, fname):
        '''send file'''
        with open(fname) as f:
            host_fname = os.path.basename(fname)
            retry = 0
            success = False
            while retry < self.MAX_RETRIES and not success:
                try:
                    if not self.session:
                        self.newSession()
                    self.session.storbinary("STOR %s" % host_fname, f)
                    success = True
                except ftplib.all_errors as e:
                    logging.info(str(e))
                    retry += 1
                    self.session = None
                    continue
        return success

class PersistentMixin(object):
    '''Generic persistent mixin'''

    def save(self, fname=None):
        if not fname:
            fname = self.fname
        tmpfd, tmpnam = tempfile.mkstemp()
        tmp = os.fdopen(tmpfd, "w")
        for e in self:
             tmp.write("%s\n" % e)
        tmp.close()
        os.rename(tmpnam, fname)
        logging.info("persistent file saved")

    def load(self, fname):
        self.fname = fname
        with open(fname, "r") as f:
            for e in f.xreadlines():
                e = e.split('\n')[0]
                self.add(e)


class PersistentSet(set, PersistentMixin):
    '''Persistent set'''
    pass


class FtpUploadOnce(FtpUpload):
    '''allow just one upload for file'''

    def __init__(self):
        self.uploaded = None

    def loadUploadedSet(self, fname):
        self.uploaded_fname = fname
        self.uploaded = PersistentSet()
        self.uploaded.load(fname)

    def saveUploadedSet(self):
        self.uploaded.save()

    def put(self, fname):
        if self.uploaded==None:
            logging.info("!!! uploaded set not loaded")
            raise RuntimeError("uploaded set not loaded")
        bname = os.path.basename(fname)
        if not bname in self.uploaded:
            if super(FtpUploadOnce, self).put(fname):
                self.uploaded.add(bname)
                logging.info("%s successfully sent" % bname)

    def __exit__(self, *args, **kwargs):
        self.saveUploadedSet()
        super(FtpUploadOnce, self).__exit__(*args, **kwargs)


class MontrealUpload(FtpUploadOnce):
    REMOTE_HOST = "ftp.host.com"
    REMOTE_USER = "ftpuser"
    REMOTE_PASS = "ftppass"
    UPLOADED_FNAME = "/home/rsync/log/uploaded.txt" # WARNING: DO NOT CHANGE THIS NAME

    def __init__(self):
        self.loadUploadedSet(self.UPLOADED_FNAME)
        return super(MontrealUpload, self).__init__(self.REMOTE_HOST, self.REMOTE_USER, self.REMOTE_PASS)


def findFiles(path_name, filter):
    for root, dirnames, filenames in os.walk(path_name):
        for filename in fnmatch.filter(filenames, filter):
            yield os.path.join(root, filename)


def sendBillingFiles():
    with MontrealUpload() as ftp:
        for f in findFiles(BILLING_PATH, BILLING_MASK):
            ftp.put(f)


logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logging.info("*** starting")
try:
    with Mutex(LOCK_FILE):
        sendBillingFiles()
    logging.info("*** normal exit")
except Exception as e:
    logginf.info(str(e))
