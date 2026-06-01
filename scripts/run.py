import sys
sys.path.insert(0, ".")

import argparse

from orchestration.run_pipeline import run_pipeline, ResourceMonitor


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Text-to-SQL pipeline or experiment from JSON configs.")
    parser.add_argument("--params", type=str, default=None, help="Path to parameters JSON file")
    parser.add_argument("--flow", type=str, default=None, help="Path to flow JSON file")
    parser.add_argument("--experiment", type=str, default=None, help="Path to JSON file with experiment setup")
    args = parser.parse_args()

    if args.experiment is None:
        with ResourceMonitor() as monitor:
            run_pipeline(args.params, args.flow)
    else:
        ...
        