ugcs is a python3 library and command-line tool for accessing and manipulating
objects in Google Cloud Storage buckets.

ugcs is distributed under the MIT license.

Dependencies
============

ugcs depends on the `openssl` executable being available for cryptographic
operations.

Quickstart
==========

```python
from ugcs import AccessTokenProvider, Bucket

atp = AccessTokenProvider.from_service_account_json("myaccount.json")
bucket = Bucket("mybucket", atp)

response = bucket.list(prefix="mypathprefix")
response = bucket.put("remote_object_path", my_byte_data, "text/plain")
data = bucket.get("remote_object_path")
metadata = bucket.get_metadata("remote_object_path")
bucket.delete("remote_object_path")
```

Access Token Caching
====================

The `ugcs.AccessTokenProvider` class caches received access tokens to avoid
creating new ones while previous ones are still valid. These tokens are stored
in the `$XDG_CACHE_HOME/ugcs` directory (or `~/.cache/ugcs` if XDG_CACHE_HOME
is unset).

Testing
=======

The `integration_tests.py` file contains integration tests for ugcs. These
tests require access to a real Google Cloud Storage bucket. To run them the
following environment variables need to be set:

`UGCS_TEST_SERVICE_ACCOUNT_FILE`: Path to a service account json file to use
                                  for testing

`UGCS_TEST_BUCKET_NAME`: Name of the bucket to use for testing
