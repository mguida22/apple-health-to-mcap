import foxglove
from foxglove.channels import LogChannel
from foxglove.schemas import Log, LogLevel
import gpxpy
from datetime import datetime
import os

def process_gpx_to_mcap(gpx_filepath: str, output_dir: str) -> str:
    """
    Process a GPX file and convert it to MCAP format.

    Args:
        gpx_filepath (str): Path to the input GPX file

    Returns:
        str: Path to the generated MCAP file
    """
    mcap_filepath = os.path.join(output_dir, os.path.splitext(os.path.basename(gpx_filepath))[0] + '.mcap')

    # Create a log channel for GPS data
    gps_chan = LogChannel(topic="/gps_data")

    # Parse the GPX file
    with open(gpx_filepath, 'r') as gpx_file:
        gpx = gpxpy.parse(gpx_file)

    # Create a new MCAP file for recording
    with foxglove.open_mcap(mcap_filepath):
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

    return mcap_filepath
