"""
Point the whole lakehouse at a fresh temporary directory for the test session.

This runs before any test module imports databricks.utilities.config, so every layer's
paths resolve under the temp root and tests never touch the real delta_lakehouse/.
"""

import os
import shutil
import tempfile

_TEST_LAKEHOUSE = tempfile.mkdtemp(prefix="clinicalflow_test_lakehouse_")
os.environ["CLINICALFLOW_LAKEHOUSE"] = _TEST_LAKEHOUSE


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(_TEST_LAKEHOUSE, ignore_errors=True)
