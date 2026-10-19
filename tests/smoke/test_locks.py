"""Table locks: writers wait for a busy table; maintenance does not."""
import threading
import time
import uuid

import pytest

from loader.state import LockHeld, table_lock


def test_a_writer_waits_for_the_lock_and_maintenance_does_not(s):
    name = f"test.lock.{uuid.uuid4().hex[:8]}"
    held = threading.Event()
    release = threading.Event()

    def holder():
        with table_lock(s, name):
            held.set()
            release.wait(10)

    t = threading.Thread(target=holder)
    t.start()
    held.wait(10)
    with pytest.raises(LockHeld):
        with table_lock(s, name, wait_seconds=0):
            pass
    threading.Timer(1.5, release.set).start()
    start = time.time()
    with table_lock(s, name, wait_seconds=20):
        waited = time.time() - start
    t.join()
    assert 1.0 < waited < 10, f"waited {waited:.1f} s"


def test_a_holder_keeps_the_lock_past_its_lease_by_renewing(s):
    """Long work (a 300,000-member batch took 30 minutes) must not lose the table to another writer."""
    name = f"test.lock.{uuid.uuid4().hex[:8]}"
    with table_lock(s, name, lease_seconds=3, renew_seconds=1):
        time.sleep(7)  # more than twice the lease
        with pytest.raises(LockHeld):
            with table_lock(s, name, wait_seconds=0):
                pass


def test_a_crashed_holder_frees_the_table_after_the_lease(s):
    """A process that dies holding a lock (no release) blocks the table only until its lease ends."""
    import subprocess
    import sys

    name = f"test.lock.{uuid.uuid4().hex[:8]}"
    crash = ("import os, sys; from loader import settings; from loader.state import table_lock\n"
             f"with table_lock(settings.load(), {name!r}, lease_seconds=4, renew_seconds=1):\n"
             "    os._exit(9)  # killed while holding the lock\n")
    assert subprocess.run([sys.executable, "-c", crash]).returncode == 9
    start = time.time()
    with table_lock(s, name, wait_seconds=30):
        waited = time.time() - start
    assert waited < 10, f"waited {waited:.1f} s for a lock whose holder died"


def test_a_holder_that_lost_its_lease_is_told(s):
    """If a holder stalls past its lease (no renewal) and another writer takes the table, leaving the
    block raises LockLost, so the work is redone rather than trusted."""
    from loader.state import LockLost

    name = f"test.lock.{uuid.uuid4().hex[:8]}"
    with pytest.raises(LockLost):
        with table_lock(s, name, lease_seconds=2, renew_seconds=4):  # renews too late: a stall
            time.sleep(3)
            with table_lock(s, name, wait_seconds=0):  # the lease has expired: taken over
                pass
            time.sleep(2)  # the stalled renewal now finds the lock gone
