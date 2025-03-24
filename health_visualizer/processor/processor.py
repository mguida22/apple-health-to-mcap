import foxglove
from foxglove.channels import LogChannel
from foxglove.schemas import Log, LogLevel
import gpxpy
import os
import logging

logger = logging.getLogger(__name__)

def process_gpx_to_mcap(gpx_filepath: str, output_dir: str, overwrite: bool = False) -> str:
    """
    Process a GPX file and convert it to MCAP format.

    Args:
        gpx_filepath (str): Path to the input GPX file

    Returns:
        str: Path to the generated MCAP file
    """
    # existing name of the file, without the extension
    existing_name = os.path.splitext(os.path.basename(gpx_filepath))[0]
    mcap_filepath = os.path.join(output_dir, existing_name + '.mcap')

    # Create a log channel for GPS data
    gps_chan = LogChannel(topic="/gps_data")

    # Parse the GPX file
    with open(gpx_filepath, 'r') as gpx_file:
        gpx = gpxpy.parse(gpx_file)

    try:
        # Create a new MCAP file for recording
        with foxglove.open_mcap(mcap_filepath, allow_overwrite=overwrite):
            # Process each track
            for track in gpx.tracks:
                # Process each segment
                for segment in track.segments:
                    # Process each point
                    for point in segment.points:
                        # Create a log entry for each GPS point
                        gps_chan.log(
                            Log(
                                level=LogLevel.Info,
                                name="GPS Point",
                                message=f"Lat: {point.latitude}, Lon: {point.longitude}, "
                                       f"Elevation: {point.elevation}, Time: {point.time}",
                            ),
                        )
    except FileExistsError:
        logger.warning(f"File {mcap_filepath} already exists. Run with --overwrite to replace the existing file.")

    return mcap_filepath
