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

from ..core.http import HTTPTransport
from ..core.exceptions import RequestError
from . import cleanup_bucket_keys

# (bucket, key) pair and to be deleted when all tests are ran.
# For the times when shit fails. That and we also need to get the test
# server going
bucket_key_cleanups = [] # mmmm global variables

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

    def test_put_no_key(self):
        bucket, key = "test_bucket", None
        r = self.transport.put(bucket, key, "look ma no key!", "text/plain")
        self.assertTrue("key" in r)
        key = r["key"]
        bucket_key_cleanups.append((bucket, key))
        self.assertEquals("created", r["status"])

        r = self.transport.get(bucket ,key)
        self.assertEquals("ok", r["status"])
        self.assertEquals("look ma no key!", r["data"])

    def test_put_return_body(self):
        bucket, key = "test_bucket", "test_return_body"
        bucket_key_cleanups.append((bucket, key))
        r = self.transport.put(bucket, key, "returning body", "text/plain",
                               returnbody=True)
        self.assertEquals("ok", r["status"])
        self.assertTrue("data" in r)
        self.assertEquals("returning body", r["data"])

    def test_put_with_indexes(self):
        bucket, key = "test_bucket", "test_put_indexes"
        bucket_key_cleanups.append((bucket, key))

        r = self.transport.put(bucket, key, "indexes!", "text/plain",
                indexes=[("field1_bin", "test"), ("field2_int", 2)])

        r = self.transport.get(bucket, key)
        self.assertEquals("indexes!", r["data"])
        self.assertEquals({"field1_bin": ["test"], "field2_int": [2]},
                          r["indexes"])

    def test_put_with_links(self):
        bucket, key1 = "test_bucket", "test_put_links"
        bucket_key_cleanups.append((bucket, key1))

        key2 = "test_linked"
        bucket_key_cleanups.append((bucket, key2))

        self.transport.put(bucket, key2, "linked", "text/plain")

        r = self.transport.put(bucket, key1, "links!", "text/plain",
                links=[(bucket, key2, bucket)])

        r = self.transport.get(bucket, key1)
        self.assertEquals([(bucket, key2, bucket)], r["links"])

    def test_put_with_meta(self):
        bucket, key = "test_bucket", "test_put_meta"
        bucket_key_cleanups.append((bucket, key))

        self.transport.put(bucket, key, "metas!", "text/plain",
                meta={"somemeta": 1, "someothermeta": "lol"})

        r = self.transport.get(bucket, key)
        self.assertEquals("lol", r["meta"]["someothermeta"])
        self.assertEquals("1", r["meta"]["somemeta"]) # An unfornate side effect

    def tearDown(self):
        clean_up_bucket_keys(bucket_key_cleanups)

class HTTPCoreTests(unittest.TestCase, CoreFeatureTests):
    def setUp(self):
        if not hasattr(self, "transport"):
            self.transport = HTTPTransport()

if __name__ == "__main__":
    unittest.main()
    clean_up_bucket_keys(bucket_key_cleanups)

