__author__ = 'chander.raja@gmail.com'

import urllib.request
import urllib.parse
import shutil
import os

def download(url, out):
    with urllib.request.urlopen(url) as req:
        filename = os.path.basename(url)
        if out.endswith('/'):
            out = os.path.join(out, filename)
        with open(out, 'wb') as f:
            assert isinstance(req, object)
            shutil.copyfileobj(req, f)


