import foxglove
from foxglove.channels import GeoJsonChannel, LogChannel
from foxglove.schemas import GeoJson
import geojson
import gpxpy
import os
import logging

logger = logging.getLogger(__name__)

foxglove.set_log_level(logging.DEBUG)


def process_gpx_to_mcap(
    gpx_filepath: str, output_dir: str, overwrite: bool = False
) -> str:
    """
    Process a GPX file and convert it to MCAP format.

    Args:
        gpx_filepath (str): Path to the input GPX file

    Returns:
        str: Path to the generated MCAP file
    """
    # existing name of the file, without the extension
    existing_name = os.path.splitext(os.path.basename(gpx_filepath))[0]
    mcap_filepath = os.path.join(output_dir, existing_name + ".mcap")

    # Parse the GPX file
    with open(gpx_filepath, "r") as gpx_file:
        gpx = gpxpy.parse(gpx_file)

    # Create a log channel for GPS data
    geojson_chan = GeoJsonChannel(topic="/geojson")

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
                        geojson_chan.log(
                            GeoJson(
                                geojson=geojson.dumps(
                                    geojson.Feature(
                                        type="Feature",
                                        geometry=geojson.Point(
                                            coordinates=[
                                                point.longitude,
                                                point.latitude,
                                            ]
                                        ),
                                        properties={
                                            "elevation": point.elevation,
                                            "time": point.time.isoformat(),
                                        },
                                    ),
                                ),
                            ),
                            log_time=int(point.time.timestamp() * 1e9),
                        )
    except FileExistsError:
        logger.warning(
            f"File {mcap_filepath} already exists. Run with --overwrite to replace the existing file."
        )

    return mcap_filepath
