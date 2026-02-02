# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Configure worker environment variables and run pre-exec setup."""
import logging
import os

from jobserver import Jobserver

log = logging.getLogger(__name__)

KEY = "JOBSERVER_EXAMPLE_VAR"


def set_env_via_preexec() -> None:
    os.environ[KEY] = "from_preexec"


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    jobserver = Jobserver(slots=1)

    log.debug("Setting environment variable via env parameter")
    future = jobserver.submit(fn=os.environ.get, args=(KEY,), env={KEY: "hi"})
    assert future.result() == "hi"

    log.debug("Without env, variable is not set")
    future = jobserver.submit(fn=os.environ.get, args=(KEY,))
    assert future.result() is None

    log.debug("Using preexec_fn to run setup code before the function")
    future = jobserver.submit(
        fn=os.environ.get, args=(KEY,), preexec_fn=set_env_via_preexec
    )
    assert future.result() == "from_preexec"

    log.debug("preexec_fn runs after env is applied (preexec_fn wins)")
    future = jobserver.submit(
        fn=os.environ.get,
        args=(KEY,),
        env={KEY: "from_env"},
        preexec_fn=set_env_via_preexec,
    )
    assert future.result() == "from_preexec"

    log.info("ex7_environ: OK")
