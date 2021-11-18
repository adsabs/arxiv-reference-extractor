"""
A caching store for the ADS reference resolver microservice, which turns textual
references ("refstrings") into resolved bibcodes.
"""

from collections import namedtuple
import dbm
import json
import logging
import os
import requests

__all__ = ["ResolvedRef", "ResolverCache"]

default_logger = logging.getLogger(__name__)


ResolvedRef = namedtuple("ResolvedRef", "bibcode score")


def _get_default_api_token():
    return os.environ["ADS_DEV_KEY"]


def _resolve_references(refstrings, api_token, logger):
    """
    Use the reference resolver service to resolve references. Production has
    better approaches, but we use this for testing reprocessing runs.

    ``refstrings`` should be an iterable of reference text strings.

    This function is a generator that generates a sequence of results for each
    input string. Each result is a dictionary of the form:

    .. code::
        {
            'refstring': <the input reference string>,
            'score': <float>,
            'bibcode': <resolved bibcode string>,
            'comment': <OPTIONAL text string>,
        }

    Where the score is a confidence in the resolution, from 0.0 for unresolved
    reference to 1.0 for highly confident. If the resolution fails, the
    ``comment`` field contains an explanation of the failure.
    """

    BATCH_SIZE_LIMIT = 16

    def resolve_batch(references):
        """
        The resolver service accepts up to 16 references at at time.

        We get 502 Bad Gateway errors often enough that it's worth automating
        retries when they happen.
        """

        data = json.dumps({"reference": references})
        headers = {
            "Authorization": "Bearer " + api_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        for _attempt in range(5):
            response = requests.post(
                url="https://api.adsabs.harvard.edu/v1/reference/text",
                headers=headers,
                data=data,
            )

            if response.status_code == 502:
                logger.warn(
                    f"retrying resolver query after error {response.status_code}"
                )
                continue

            response.raise_for_status()
            return json.loads(response.content)["resolved"]

        response.raise_for_status()
        raise Exception("unreachable")

    batch = []

    for next_ref in refstrings:
        batch.append(next_ref)

        if len(batch) >= BATCH_SIZE_LIMIT:
            for info in resolve_batch(batch):
                yield info
            batch = []

    if len(batch):
        for info in resolve_batch(batch):
            yield info


class ResolverCache(object):
    """
    Helper class for resolving reference strings and caching the results, for
    speed and reducing load in the resolver microservice.
    """

    _handle = None

    def __init__(self, path):
        self._handle = dbm.open(path, "c")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._handle.close()
        self._handle = None

    def _get(self, refstring):
        packed = self._handle.get(refstring.encode("utf-8"))
        if packed is None:
            return None

        bits = packed.decode("utf-8").split("/", 1)
        score = float(bits[0])
        bibcode = bits[1]
        return ResolvedRef(bibcode, score)

    def _save(self, refstring, resolver_info):
        # The score should be a float already, but I got a crash once
        # indicating that it wasn't.
        score = float(resolver_info["score"])
        bibcode = resolver_info["bibcode"]
        packed = f"{score}/{bibcode}"
        self._handle[refstring.encode("utf-8")] = packed.encode("utf-8")
        return ResolvedRef(bibcode, score)

    def resolve(self, refstrings, logger=default_logger, api_token=None, no_rpc=False):
        """
        Resolve a batch of reference strings.

        ``refstrings`` is an iterable of reference text strings

        Returns a dictionary mapping the refstrings to ``ResolvedRef`` named
        tuples.

        The batch should be as large as possible to make optimal use of the
        microservice API.
        """

        resolved = {}
        todo = set()

        if api_token is None:
            api_token = _get_default_api_token()

        for rs in refstrings:
            info = self._get(rs)
            if info is None:
                todo.add(rs)
            else:
                resolved[rs] = info

        if no_rpc:
            logger.warn(f"NOT resolving {len(todo)} reference strings")

            for rs in todo:
                resolved[rs] = ResolvedRef("xxxxxxxxxxxxxxxxxxx", 0.0)
        else:
            logger.warn(f"resolving {len(todo)} reference strings")

            for info in _resolve_references(todo, api_token, logger):
                refstring = info["refstring"]
                resolved[refstring] = self._save(refstring, info)

        return resolved