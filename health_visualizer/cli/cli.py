import argparse
import logging
from pathlib import Path

from health_visualizer.processor.combined import process_combined_to_mcap
from health_visualizer.processor.ecg import process_ecg_to_mcap
from health_visualizer.processor.gpx import process_gpx_to_mcap
from health_visualizer.processor.xml_export import process_xml_export_to_mcap


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Process health data into MCAP.")
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to a single .gpx file to process",
    )
    parser.add_argument(
        "--input-type",
        choices=["gpx", "ecg", "export", "combined"],
        required=True,
        help="Type of input file to process (gpx, ecg, export, combined)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output"),
        help="Path to output directory (default: ./output)",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing files"
    )
    args = parser.parse_args()

    logger.info(f"Processing input from '{args.input}' into MCAP at '{args.output}'")

    # Create output directory if it doesn't exist
    args.output.mkdir(parents=True, exist_ok=True)

    try:
        if args.input_type == "gpx":
            process_gpx_to_mcap(args.input, args.output, args.overwrite)
        elif args.input_type == "ecg":
            process_ecg_to_mcap(args.input, args.output, args.overwrite)
        elif args.input_type == "export":
            process_xml_export_to_mcap(args.input, args.output, args.overwrite)
        elif args.input_type == "combined":
            process_combined_to_mcap(args.input, args.output, args.overwrite)
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise
