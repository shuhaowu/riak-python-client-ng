# -*- coding: utf-8 -*-
# Copyright 2012 Shuhao Wu <shuhao@shuhaowu.com>
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import

from .exceptions import *

try:
    from .http import HTTPTransport
except ImportError:
    errormsg = "HTTPTransport is not available most likely due to the lack " + \
               "of the requests module. Try import requests in your python " + \
               "terminal."
    print errormsg
    from .transport import Transport
    class HTTPTransport(Transport):
        fake = True
        def __init__(self, *arg, **kwargs):
            raise RiakngError(errormsg)

try:
    from .pbc import PBCTransport
except ImportError:
    errormsg = "PBCTransport is not available most likely due to the lack of "+\
               "protobuf support or the protobuf messages are not compiled. " +\
               "See README.md for this library for details."
    print errormsg
    from .transport import Transport
    class PBCTransport(Transport):
        fake = True
        def __init__(self, *arg, **kwargs):
            raise RiakngError(errormsg)

if getattr(HTTPTransport, "fake", False) and \
    getattr(PBCTransport, "fake", False):
    raise RiakngError("Both HTTP and PBC Transports are not available.")
