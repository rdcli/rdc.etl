# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
import sys, os, platform
from . import IStatus

def has_ansi_support(handle=None):
    handle = handle or sys.stdout
    if (hasattr(handle, "isatty") and handle.isatty()) or \
            ('TERM' in os.environ and os.environ['TERM']=='ANSI'):
        if platform.system()=='Windows' and not ('TERM' in os.environ and os.environ['TERM']=='ANSI'):
            return False
        else:
            return True
    return False

class ConsoleStatus(IStatus):
    def __init__(self):
        self.ansi = has_ansi_support()

