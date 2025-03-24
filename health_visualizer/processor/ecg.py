import foxglove
from foxglove import Channel
import csv
import time
import logging
import os

logger = logging.getLogger(__name__)

# Define schema for ECG data
ecg_schema = {
    "type": "object",
    "properties": {
        "voltage": {"type": "number"},
    },
}


def process_ecg_to_mcap(
    csv_filepath: str, output_dir: str, overwrite: bool = False
) -> str:
    """
    Process an ECG CSV file and convert it to MCAP format.

    Args:
        csv_filepath (str): Path to the input CSV file
        output_dir (str): Directory to save the output MCAP file
        overwrite (bool): Whether to overwrite existing files

    Returns:
        str: Path to the generated MCAP file
    """
    # Create output filename
    existing_name = os.path.splitext(os.path.basename(csv_filepath))[0]
    mcap_filepath = os.path.join(output_dir, existing_name + ".mcap")

    # Create channel for ECG data
    ecg_chan = Channel(topic="/ecg/lead_i", schema=ecg_schema)

    try:
        # Create a new MCAP file for recording
        with foxglove.open_mcap(mcap_filepath, allow_overwrite=overwrite):
            # Read CSV file
            with open(csv_filepath, "r") as csvfile:
                csv_reader = csv.reader(csvfile)

                # read until we find a row that starts with "Sample Rate"
                # then store that as the sample_rate
                sample_rate_hz = None
                for row in csv_reader:
                    if row and row[0].startswith("Sample Rate"):
                        # the sample rate is the second column
                        # and looks like "512 Hertz"
                        sample_rate_hz = int(row[1].split(" ")[0])
                        break

                if sample_rate_hz is None:
                    raise ValueError("Sample rate not found in ecg file")

                # Calculate time between samples (in nanoseconds)
                sample_interval_ns = int(1_000_000_000 / sample_rate_hz)
                start_time = time.time_ns()

                # Process voltage readings
                for i, row in enumerate(csv_reader):
                    if row:  # Skip empty rows
                        try:
                            voltage = float(row[0])
                            log_time = start_time + (i * sample_interval_ns)

                            # Log the ECG data point
                            ecg_chan.log({"voltage": voltage}, log_time=log_time)
                        except ValueError:
                            continue
    except FileExistsError:
        logger.warning(
            f"File {mcap_filepath} already exists. Run with --overwrite to replace the existing file."
        )

    return mcap_filepath
