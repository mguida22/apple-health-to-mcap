from typing import Optional
import foxglove
from foxglove import Channel, Schema
from foxglove.channels import GeoJsonChannel
from foxglove.schemas import GeoJson
import geojson
import gpxpy
import os
import logging

logger = logging.getLogger(__name__)

foxglove.set_log_level(logging.DEBUG)

gpx_metrics_schema = {
    "type": "object",
    "title": "gpx_metrics",
    "properties": {
        "elevation": {"type": "number"},
        "speed": {"type": "number"},
        "course": {"type": "number"},
        "hAcc": {"type": "number"},
        "vAcc": {"type": "number"},
    },
}


def process_gpx_to_mcap(
    gpx_filepath: str,
    output_dir: str,
    overwrite: bool = False,
    filename: Optional[str] = None,
) -> str:
    """
    Process a GPX file and convert it to MCAP format.

    Args:
        gpx_filepath (str): Path to the input GPX file

    Returns:
        str: Path to the generated MCAP file
    """
    if filename is None:
        existing_name = os.path.splitext(os.path.basename(gpx_filepath))[0]
        mcap_filepath = os.path.join(output_dir, existing_name + ".mcap")
    else:
        mcap_filepath = os.path.join(output_dir, filename)

    # Parse the GPX file
    with open(gpx_filepath, "r") as gpx_file:
        gpx = gpxpy.parse(gpx_file)

    geojson_chan = GeoJsonChannel(topic="/geojson")
    metrics_chan = Channel(topic="/gpx_metrics", schema=gpx_metrics_schema)

    logger.info(f"Processing GPX file at {gpx_filepath}")
    try:
        # Create a new MCAP file for recording
        with foxglove.open_mcap(mcap_filepath, allow_overwrite=overwrite):
            # Process each track
            for track in gpx.tracks:
                # Process each segment
                for segment in track.segments:
                    # Process each point
                    for point in segment.points:
                        log_time = int(point.time.timestamp() * 1e9)
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
                            log_time=log_time,
                        )

                        extension_values = get_extension_values(point)
                        metrics_chan.log(
                            {
                                "elevation": point.elevation,
                                **extension_values,
                            },
                            log_time=log_time,
                        )
    except FileExistsError:
        logger.warning(
            f"File {mcap_filepath} already exists. Run with --overwrite to replace the existing file."
        )

    return mcap_filepath


def get_extension_values(point):
    values = {
        "speed": None,
        "course": None,
        "hAcc": None,
        "vAcc": None,
    }

    for child in point.extensions:
        if child.tag == "speed":
            values["speed"] = child.text
        elif child.tag == "course":
            values["course"] = int(float(child.text))
        elif child.tag == "hAcc":
            values["hAcc"] = child.text
        elif child.tag == "vAcc":
            values["vAcc"] = child.text

    return values
