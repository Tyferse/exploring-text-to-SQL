import sys
sys.path.insert(0, ".")

from orchestration.run_experiment import run_experiment
from orchestration.run_pipeline import run_pipeline, ResourceMonitor


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    from src.utils.run_manager import set_global_seeds
    
    load_dotenv(".env")
    set_global_seeds()

    parser = argparse.ArgumentParser(description="Run Text-to-SQL pipeline or experiment from JSON configs.")
    parser.add_argument("--params", type=str, default=None, help="Path to parameters JSON file")
    parser.add_argument("--flow", type=str, default=None, help="Path to flow JSON file")
    parser.add_argument("--experiment", type=str, default=None, help="Path to JSON file with experiment setup")
    parser.add_argument(
        "--no-skip-completed-exp", action="store_true", 
        help="Re-run experiments variants that already completed successfully"
    )
    args = parser.parse_args()

    if args.experiment is None:
        with ResourceMonitor():
            run_pipeline(args.params, args.flow)
    else:
        run_experiment(args.experiment, not args.no_skip_completed_exp)
