#!/usr/bin/env python3
#
# Copyright © 2020 Collabora Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# SPDX-License-Identifier: MIT

import argparse
import base64
import gzip
import json
import mimetypes
import os
import subprocess
import sys
import tempfile
import time

from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse, quote
from urllib.request import urlopen, Request

def _json_to_b64(j):
    s = json.dumps(j, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(s).decode()

def _jwt_sign_b64(base, key):
    with tempfile.TemporaryFile("w+", encoding="utf-8") as keyfile:
        keyfile.write(key)
        keyfile.flush()
        keyfilename = "/proc/%d/fd/%d" % (os.getpid(), keyfile.fileno())
        cmd = ["openssl", "dgst", "-sign", keyfilename, "-sha256", "-binary"]
        p = subprocess.run(cmd, input=base.encode("utf-8"), capture_output=True)
        return base64.urlsafe_b64encode(p.stdout).decode()

def _urlopen_with_decoded_response(request):
    response = urlopen(request)
    response_data = response.read()

    if response.info().get("Content-Encoding") == "gzip":
        response_data = gzip.decompress(response_data)

    return response_data

def _xdg_cache_home():
    return Path(os.environ.get("XDG_CACHE_HOME", None) or Path.home() / ".cache")

class AccessTokenProvider:
    """A provider of oauth2 access tokens for a Google Cloud Storage account"""
    _TOKEN_URL = "https://oauth2.googleapis.com/token"

    def __init__(self, account, key, kid=None, expire=60, token_url=_TOKEN_URL):
        self.account = account
        self.key = key
        self.kid = kid
        self.expire = expire
        self.token_url = token_url

        self.cached_token = None
        self.cached_token_path = _xdg_cache_home() / "ugcs" / (account + ".token")
        self.cached_token_path.parent.mkdir(parents=True, exist_ok=True)
        if self.cached_token_path.is_file():
            with open(str(self.cached_token_path), "r") as f:
                self.cached_token = json.load(f)

    def from_service_account_json(json_obj):
        if isinstance(json_obj, str):
            try: 
                j = json.loads(json_obj)
            except:
                json_obj = Path(json_obj)

        if isinstance(json_obj, Path):
            j = json.loads(json_obj.read_text())
        else:
            j = json

        return AccessTokenProvider(
            j["client_email"], j["private_key"],
            kid=j["private_key_id"],
            token_url=j["token_uri"]
        )

    def _create_jwt(self):
        unix_time = int(time.time())
        jwt_header = {"alg": "RS256", "typ": "JWT"}
        if self.kid is not None:
            jwt_header["kid"] = self.kid
        jwt_claim = {
            "iss": self.account,
            "scope": "https://www.googleapis.com/auth/devstorage.read_write",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": unix_time,
            "exp": unix_time + self.expire,
        }

        payload = _json_to_b64(jwt_header) + "." + _json_to_b64(jwt_claim)
        sig = _jwt_sign_b64(payload, self.key)

        return payload + "." + sig

    def _request_new_token(self):
        data = {
            "grant_type" : "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion" : self._create_jwt(),
        }

        headers = {
            "Content-Type" : "application/x-www-form-urlencoded",
            "Accept-Encoding" : "gzip, identity"
        }

        request = Request(
            self.token_url,
            headers=headers,
            data=urlencode(data).encode("utf-8")
        )

        response_data = _urlopen_with_decoded_response(request)

        return json.loads(response_data)

    def request_token(self):
        if self.cached_token is not None and time.time() + 3 >= self.cached_token["expires_at"]:
            self.cached_token = None

        if self.cached_token is None:
            self.cached_token = self._request_new_token()
            self.cached_token["expires_at"] = self.cached_token["expires_in"] + int(time.time())
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as f:
                try:
                    json.dump(self.cached_token, f)
                    f.flush()
                    os.fsync(f.fileno())
                    # Do an atomic rename/replace to ensure cached_token_path always
                    # contains valid data.
                    os.replace(f.name, str(self.cached_token_path))
                except:
                    os.unlink(f.name)
                    raise

        return self.cached_token

class Bucket:
    """A Google Cloud Storage bucket"""
    _STORAGE_URL = "https://storage.googleapis.com/storage/v1"
    _UPLOAD_URL = "https://storage.googleapis.com/upload/storage/v1"

    def __init__(self, name, access_token_provider):
        self.name = name
        self.url = name
        self.access_token_provider = access_token_provider

    def _authorized_request(self, url):
        token = self.access_token_provider.request_token()

        headers = {
            "Authorization" : token["token_type"] + " " + token["access_token"],
            "Accept-Encoding" : "gzip, identity"
        }

        return Request(url, headers=headers)

    def list(self, prefix=None):
        url = self._STORAGE_URL + "/b/%s/o" % self.name
        if prefix:
            url = url + "?" + urlencode({"prefix" : prefix})

        request = self._authorized_request(url)
        response_data = _urlopen_with_decoded_response(request)
        return json.loads(response_data)

    def put(self, path, data, content_type):
        request = self._authorized_request(
            self._UPLOAD_URL + "/b/%s/o?%s" %
                (
                    self.name,
                    urlencode({
                        "uploadType" : "media",
                        "name" : path
                    })
                )
        )

        request.add_header("Content-Type", content_type)
        request.add_header("Content-Transfer-Encoding", "binary")
        request.data = data

        response_data = _urlopen_with_decoded_response(request)
        return json.loads(response_data)

    def get(self, path):
        request = self._authorized_request(
            self._STORAGE_URL + "/b/%s/o/%s?alt=media" % (self.name, quote(path, safe=""))
        )

        response_data = _urlopen_with_decoded_response(request)
        return response_data

    def get_metadata(self, path):
        request = self._authorized_request(
            self._STORAGE_URL + "/b/%s/o/%s" % (self.name, quote(path, safe=""))
        )

        response_data = _urlopen_with_decoded_response(request)
        return json.loads(response_data)

    def delete(self, path):
        request = self._authorized_request(
            self._STORAGE_URL + "/b/%s/o/%s" % (self.name, quote(path, safe=""))
        )
        request.method = "DELETE"

        _urlopen_with_decoded_response(request)

def create_bucket_from_args(args):
    atp = AccessTokenProvider.from_service_account_json(args.service_account_file)
    o = urlparse(args.remote)
    return Bucket(o.netloc, atp)

def cmd_list(args):
    bucket = create_bucket_from_args(args)
    prefix = urlparse(args.remote).path[1:] or None

    response = bucket.list(prefix=prefix)
    print(response)

def cmd_put(args):
    bucket = create_bucket_from_args(args)
    content_type = args.content_type or \
                   mimetypes.guess_type(args.local)[0] or \
                   "application/octet-stream"
    path = urlparse(args.remote).path[1:]

    response = bucket.put(path, open(args.local, "rb").read(), content_type)
    print(response)

def cmd_get(args):
    bucket = create_bucket_from_args(args)
    path = urlparse(args.remote).path[1:]

    response = bucket.get(path)
    # Output raw binary data
    sys.stdout.buffer.write(response)

def cmd_get_metadata(args):
    bucket = create_bucket_from_args(args)
    path = urlparse(args.remote).path[1:]

    response = bucket.get_metadata(path)
    print(response)

def cmd_delete(args):
    bucket = create_bucket_from_args(args)
    path = urlparse(args.remote).path[1:]

    bucket.delete(path)

def main():
    parser = argparse.ArgumentParser(description="Micro Google Cloud Storage CLI.")
    parser.add_argument("--service-account-file", required=True,
                        help="the name of a service account json file")

    subparsers = parser.add_subparsers(help="sub-command help")

    parser_list = subparsers.add_parser("list",
                                        description="list objects in a bucket")
    parser_list.add_argument("remote",
                            help="the remote object to upload to (gs://bucket/object_prefix)")
    parser_list.set_defaults(func=cmd_list)

    parser_put = subparsers.add_parser("put", description="upload an object")
    parser_put.add_argument("--content-type", required=False,
                        help="the Content-Type of the uploaded file")
    parser_put.add_argument("local",
                            help="the local file to upload")
    parser_put.add_argument("remote",
                            help="the remote object to upload to (gs://bucket/object_path)")
    parser_put.set_defaults(func=cmd_put)

    parser_get = subparsers.add_parser("get", description="download an object")
    parser_get.add_argument("remote",
                            help="the name of remote object to get (gs://bucket/object_path)")
    parser_get.set_defaults(func=cmd_get)

    parser_get_metadata = subparsers.add_parser("get-metadata",
                                                description="get metadata for an object")
    parser_get_metadata.add_argument("remote",
                                     help="the name of remote object to get metadata for (gs://bucket/object_path)")
    parser_get_metadata.set_defaults(func=cmd_get_metadata)

    parser_delete = subparsers.add_parser("delete")
    parser_delete.add_argument("remote",
                               help="the remote object to delete (gs://bucket/object_path)")
    parser_delete.set_defaults(func=cmd_delete)

    args = parser.parse_args()

    try:
        args.func(args)
        return 0
    except HTTPError as e:
        print("Server error:", file=sys.stderr)
        print(e.read().decode("utf-8"), file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
