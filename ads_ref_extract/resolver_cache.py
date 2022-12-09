"""
A caching store for the ADS reference resolver microservice, which turns textual
references ("refstrings") into resolved bibcodes.

The typical throughput of the resolution service is about 1.2 resolutions per
second, which is why caching is so valuable here.
"""

from collections import namedtuple
import dbm
import json
import logging
import os
import requests
import time

__all__ = ["ResolvedRef", "ResolverCache"]

default_logger = logging.getLogger(__name__)


ResolvedRef = namedtuple("ResolvedRef", "bibcode score")


def _get_default_api_token():
    token = os.environ.get("ADS_DEV_KEY")
    if token is not None:
        return token

    token = os.environ.get("API_TOKEN")
    if token is not None:
        if token.startswith("Bearer "):
            return token.split()[1]
        return token

    raise Exception(
        "need an ADS API token but none set in environment; set $ADS_DEV_KEY or $API_TOKEN"
    )


def _resolve_references(refstrings, api_token, logger, level):
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
    t0 = time.time()
    tlast = t0

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

            if response.status_code in (502, 504):
                logger.warn(
                    f"retrying resolver query after error {response.status_code}: {response.content!r}"
                )
                continue

            response.raise_for_status()
            return json.loads(response.content)["resolved"]

        response.raise_for_status()
        raise Exception("unreachable")

    batch = []
    n_resolved = 0

    for next_ref in refstrings:
        batch.append(next_ref)

        if len(batch) >= BATCH_SIZE_LIMIT:
            for info in resolve_batch(batch):
                n_resolved += 1
                yield info
            batch = []

        tnow = time.time()
        if tnow - tlast > 180:
            tp = n_resolved / (tnow - t0)
            logger.log(
                level,
                f"reference resolution status: {n_resolved} resolved, throughput {tp:.2f} resolutions/second",
            )
            tlast = tnow

    if len(batch):
        for info in resolve_batch(batch):
            n_resolved += 1
            yield info

    tnow = time.time()
    tp = n_resolved / (tnow - t0)
    logger.log(
        level,
        f"finished resolving: {n_resolved} resolved, throughput {tp:.2f} resolutions/second",
    )


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

    def count_need_rpc(self, refstrings):
        """
        Count the number of refstrings that would need an RPC call to resolve; i.e.,
        the number that aren't locally cached.
        """
        n = 0

        for rs in refstrings:
            if self._get(rs) is None:
                n += 1

        return n

    def resolve(
        self,
        refstrings,
        logger=default_logger,
        level=logging.WARN,
        api_token=None,
        no_rpc=False,
    ):
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

        for rs in refstrings:
            info = self._get(rs)
            if info is None:
                todo.add(rs)
            else:
                resolved[rs] = info

        if not todo:
            pass
        elif no_rpc:
            logger.log(level, f"NOT resolving {len(todo)} reference strings")

            for rs in todo:
                resolved[rs] = ResolvedRef("xxxxxxxxxxxxxxxxxxx", 0.0)
        else:
            if api_token is None:
                api_token = _get_default_api_token()

            logger.log(level, f"resolving {len(todo)} reference strings")

            for info in _resolve_references(todo, api_token, logger, level):
                refstring = info["refstring"]
                resolved[refstring] = self._save(refstring, info)

        return resolved
