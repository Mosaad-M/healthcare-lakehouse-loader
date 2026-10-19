"""Processes that load the loader never write memory dumps (they would hold plaintext identifiers)."""
import subprocess
import sys



def test_importing_the_loader_disables_core_dumps():
    out = subprocess.run([sys.executable, "-c", "import resource, loader; print(resource.getrlimit(resource.RLIMIT_CORE))"],
                         capture_output=True, text=True, check=True).stdout
    assert out.strip() == "(0, 0)"
