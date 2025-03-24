import argparse
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Process health data into MCAP.')
    parser.add_argument(
        '--input',
        type=Path,
        required=True,
        help='Path to input file or directory. Should be an extracted zip file from Apple Health.'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('output'),
        help='Path to output directory (default: ./output)'
    )
    args = parser.parse_args()

    logger.info(f"Processing input from '{args.input}' into MCAP at '{args.output}'")

    # Create output directory if it doesn't exist
    args.output.mkdir(parents=True, exist_ok=True)

    try:
        pass
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise
