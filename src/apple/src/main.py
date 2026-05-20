"""Apple connector — discovers DOWNLOAD_SPECS in src/nodes/ and runs the DAG."""
import sys
from pathlib import Path

# Put src/ on sys.path so spawn-context child processes can import nodes.<module>.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from subsets_utils import load_nodes, validate_environment, run_health_tests


def main():
    validate_environment()
    workflow = load_nodes()
    workflow.run()
    # Model-authored health tests run here — post-DAG, in-connector — so data
    # access resolves identically whether the run is local or on GitHub Actions.
    run_health_tests(Path(__file__).resolve().parent.parent)


if __name__ == "__main__":
    main()
