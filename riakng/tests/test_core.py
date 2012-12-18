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

import unittest

from riakng.core.http import HTTPTransport
from riakng.core.exceptions import RequestError

# (bucket, key) pair and to be deleted when all tests are ran.
bucket_key_cleanups = []

class CoreFeatureTests(object):
    """This class is to be extended and the transports are to be filled in.
    """

    def test_ping(self):
        self.assertTrue(self.transport.ping())

    def test_get_put_delete(self):
        bucket, key = "test_bucket", "test_get_key"
        bucket_key_cleanups.append((bucket, key))

        # Put something into the db
        r = self.transport.put(bucket, key, "hello world", "text/plain")
        self.assertTrue("key" in r)
        self.assertEquals(key, r["key"])
        self.assertTrue("status" in r)
        self.assertEquals("no_content", r["status"])
        self.assertTrue("headers" in r)

        # Now retrieve it...
        r = self.transport.get(bucket, key)
        self.assertEquals("ok", r["status"])
        self.assertTrue("headers" in r)
        self.assertEquals("hello world", r["data"])

        self.assertTrue(self.transport.delete(bucket, key))

        with self.assertRaises(RequestError):
            self.transport.get(bucket, key)

class HTTPCoreTests(unittest.TestCase, CoreFeatureTests):
    def setUp(self):
        if not hasattr(self, "transport"):
            self.transport = HTTPTransport()

if __name__ == "__main__":
    unittest.main()
    # clean_up_bucket_keys(bucket_key_cleanups)

