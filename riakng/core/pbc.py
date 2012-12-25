# -*- coding: utf-8 -*-
# Copyright 2012 Shuhao Wu <shuhao@shuhaowu.com>
# Copyright 2010 Rusty Klophaus <rusty@basho.com>
# Copyright 2010 Justin Sheehy <justin@basho.com>
# Copyright 2009 Jay Baird <jay@mochimedia.com>
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

import errno
import socket
import struct
import json

from . import riakpb
from .transport import Transport
from .exceptions import RiakngError, PBCRequestError

ERROR_RESP = 0
PING_REQ = 1
PING_RESP = 2
GET_CLIENT_ID_REQ = 3
GET_CLIENT_ID_RESP = 4
SET_CLIENT_ID_REQ = 5
SET_CLIENT_ID_RESP = 6
GET_SERVER_INFO_REQ = 7
GET_SERVER_INFO_RESP = 8
GET_REQ = 9
GET_RESP = 10
PUT_REQ = 11
PUT_RESP = 12
DEL_REQ = 13
DEL_RESP = 14
LIST_BUCKETS_REQ = 15
LIST_BUCKETS_RESP = 16
LIST_KEYS_REQ = 17
LIST_KEYS_RESP = 18
GET_BUCKET_REQ = 19
GET_BUCKET_RESP = 20
SET_BUCKET_REQ = 21
SET_BUCKET_RESP = 22
MAPRED_REQ = 23
MAPRED_RESP = 24
INDEX_REQ = 25
INDEX_RESP = 26
SEARCH_QUERY_REQ = 27
SEARCH_QUERY_RESP = 28

ONE = 4294967294
QUORUM = 4294967293
ALL = 4294967292
DEFAULT = 4294967291

# These are a specific set of socket errors
# that could be raised on send/recv that indicate
# that the socket is closed or reset, and is not
# usable. On seeing any of these errors, the socket
# should be closed, and the connection re-established.
CONN_CLOSED_ERRORS = (
                        errno.EHOSTUNREACH,
                        errno.ECONNRESET,
                        errno.EBADF,
                        errno.EPIPE
                     )

class PBCTransport(Transport):
    def __init__(self, client_id=None, host="127.0.0.1", port=8087,
                       max_attempt=1):
        self._client_id = client_id or self.random_client_id()
        self._host = host
        self._port = port
        self._max_attempt = max_attempt
        self._socket = None

    def connect(self):
        if self._socket is None:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self._socket.connect((self._host, self._port))
            except socket.error as e:
                self.close()
                raise

    def close(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def send_packet(self, packet):
        attempt = 0
        for attempt in xrange(self._max_attempt):
            e = None
            try:
                self.connect()
                self._socket.sendall(packet)
            except socket.error as e:
                if e[0] in CONN_CLOSED_ERRORS:
                    self.close()
                    continue
                else:
                    raise
            else:
                break

        if attempt + 1 == self._max_attempt and e is not None:
            raise e

    def encode_message(self, code, message):
        if message is None:
            s = ""
        else:
            s = message.SerializeToString()
        packet = struct.pack("!iB", 1+len(s), code)
        return packet + s

    def recv(self, length):
        # This whole thing looks very fragile.
        if self._socket is None:
            raise RiakngError("Wait what? This is a bug as socket seems closed")
        try:
            res = self._socket.recv(length)

            # Assume the socket is closed if no data is
            # returned on a blocking read.
            if len(res) == 0 and length > 0:
                self.close()

            return res
        except socket.error as e:
            if e[0] in CONN_CLOSED_ERRORS:
                self.close()
            raise

    def recv_packet(self):
        length = self.recv(4)
        if len(length) != 4:
            raise RiakngError(
                "PBC protocol's response does not start with 4 bytes but " +
                "rather {0} bytes".format(len(length))
            )

        length, = struct.unpack("!i", length)
        packet = ""
        while len(packet) < length:
            # I'm not sure why the maximum receive length is 8192 bytes..
            # Though I am too scared to remove it.
            remaining_length = min(8192, length - len(packet))
            buf = self.recv(length)
            if not buf:
                break # Message ended?
            packet += buf

        if len(packet) != length:
            raise RiakngError(
                "Packet length does not agree. Got {0}, expect {1}".format(
                    len(packet), length
                )
            )

        return packet

    def _construct_protobuf_object(self, packet, pbclass):
        if pbclass is None:
            return None

        pbo = pbclass()
        pbo.ParseFromString(packet[1:])
        return pbo

    RESPCODE_TO_CLASS = {
        ERROR_RESP: riakpb.commons.RpbErrorResp,
        PING_RESP: None,
        GET_SERVER_INFO_RESP: riakpb.commons.RpbGetServerInfoResp,
        GET_CLIENT_ID_RESP: riakpb.kv.RpbGetClientIdResp,
        SET_CLIENT_ID_RESP: None,
        GET_RESP: riakpb.kv.RpbGetResp,
        PUT_RESP: riakpb.kv.RpbPutResp,
        DEL_RESP: None,
        LIST_KEYS_RESP: riakpb.kv.RpbListKeysResp,
        LIST_BUCKETS_RESP: riakpb.kv.RpbListBucketsResp,
        GET_BUCKET_RESP: riakpb.kv.RpbGetBucketResp,
        SET_BUCKET_RESP: None,
        MAPRED_RESP: riakpb.kv.RpbMapRedResp,
        INDEX_RESP: riakpb.kv.RpbIndexResp,
        SEARCH_QUERY_RESP: riakpb.search.RpbSearchQueryResp,
    }

    def decode_packet(self, packet):
        code, = struct.unpack("B", packet[:1])
        try:
            pbclass = self.RESPCODE_TO_CLASS[code]
        except KeyError:
            raise RiakngError("Unknown message code: {0}".format(code))
        else:
            pbo = self._construct_protobuf_object(packet, pbclass)
            return code, pbo

    def send_message(self, code, message):
        packet = self.encode_message(code, message)
        self.send_packet(packet)

    def recv_message(self):
        packet = self.recv_packet()
        return self.decode_packet(packet)

    def ping(self):
        self.send_message(PING_REQ, None)
        code, message = self.recv_message()
        if code == PING_RESP:
            return True
        return False

    def _merge_params(self, req, params, attrs):
        for name in attrs:
            if isinstance(name, (tuple, list)):
                name, to_name = name[0], name[1]
            else:
                to_name = name
            if params.get(name):
                setattr(req, to_name, params[name])


    def decode_content(self, content):
        obj = {}
        obj["meta"] = meta = {}
        obj["indexes"] = indexes = {}
        obj["links"] = links = []
        obj["data"] = content.value

        if content.HasField("deleted"):
            obj["deleted"] = True

        if content.HasField("content_type"):
            obj["content-type"] = content.content_type
        else:
            obj["content-type"] = None

        if content.HasField("charset"):
            obj["charset"] = content.charset
        else:
            obj["charset"] = None

        if content.HasField("content_encoding"):
            obj["content-encoding"] = content.content_encoding
        else:
            obj["content-encoding"] = None

        if content.HasField("vtag"):
            obj["vtag"] = content.vtag
        else:
            obj["vtag"] = None

        if content.HasField("last_mod"):
            # This will cause a divergence between the HTTP
            # and PBC api
            obj["last-modified"] = content.last_mod
        else:
            obj["last-modified"] = None

        # TODO: work on links, meta, indexes

    _rw_names = {
        "default": DEFAULT,
        "all": ALL,
        "quorum": QUORUM,
        "one" : ONE
    }
    def get(self, bucket, key, r=None, vclock=None, headers=None, **params):
        """Same as the Transport's get. However, headers is ignored and there
        are some more attributes in the params as defined in:

        http://docs.basho.com/riak/latest/references/apis/protocol-buffers/PBC-Fetch-Object/

        Note that if_modified is a params, not in headers like the HTTP request
        """
        req = riakpb.kv.RpbGetReq()
        if r:
            req.r = self._rw_names.get(r)

        self._merge_params(req, params,
            ("pr", "notfound_ok", "basic_quorum", "head", "deletedvclock", "if_modified")
        )

        req.bucket = bucket
        req.key = key

        self.send_message(GET_REQ, req)
        code, message = self.recv_message()

        r = {}
        if code == GET_RESP:
            if len(message.content):
                raise PBCRequestError("Key {0} not found".format(key), 404)

            if message.HasField("unchanged") and message.unchanged:
                r["status"] = "not_modified"
            else:
                r["status"] = "ok"

            if message.HasField("vclock"):
                vclock = message.vclock
            else:
                vclock = None

            contents = []
            for c in message.content:
                cont = self.decode_content(c)
                c["vclock"] = vclock
                contents.append(self.decode_content(c))

            r["siblings"] = contents

            if len(contents) > 1:
                r["status"] = "multiple_choice"

            return # TODO: stuff
        else:
            raise RiakngError(
                "Expected response code does not match: got {0}".format(code)
            )
