import foxglove
import geojson
import gpxpy
import logging
import math
import os
from foxglove import Channel
from foxglove.channels import GeoJsonChannel, LocationFixChannel
from foxglove.schemas import GeoJson, LocationFix
from xml.etree import ElementTree as ET
from datetime import datetime

logger = logging.getLogger(__name__)

foxglove.set_log_level(logging.DEBUG)

route_metrics_schema = {
    "type": "object",
    "title": "route_metrics",
    "properties": {
        "elevation": {"type": "number"},
        "speed": {"type": "number"},
        "course": {"type": "number"},
        "hAcc": {"type": "number"},
        "vAcc": {"type": "number"},
    },
}

workout_metrics_schema = {
    "type": "object",
    "title": "workout_metrics",
    "properties": {
        "unit": {"type": "string"},
        "value": {"type": "number"},
        "startDate": {"type": "string"},
        "endDate": {"type": "string"},
        "sourceName": {"type": "string"},
        "sourceVersion": {"type": "string"},
        "device": {"type": "string"},
        "creationDate": {"type": "string"},
        "tag": {"type": "string"},
    },
}

excluded_source_names = ["Whitefish Wave", "WaterMinder"]


def process_combined_to_mcap(
    gpx_filepath: str, output_dir: str, overwrite: bool = False
) -> str:
    """
    Process a GPX file and Apple Health export file and convert the relevant
    data to MCAP format.

    Args:
        gpx_filepath (str): Path to the input GPX file
        output_dir (str): Path to the output directory
        overwrite (bool): Whether to overwrite existing files

    Returns:
        str: Path to the generated MCAP file
    """
    # existing name of the file, without the extension
    existing_name = os.path.splitext(os.path.basename(gpx_filepath))[0]
    mcap_filepath = os.path.join(output_dir, f"{existing_name}-combined.mcap")

    # Parse the GPX file
    with open(gpx_filepath, "r") as gpx_file:
        gpx = gpxpy.parse(gpx_file)

    geojson_chan = GeoJsonChannel(topic="/geojson")
    location_chan = LocationFixChannel(topic="/location")
    metrics_chan = Channel(topic="/metrics", schema=route_metrics_schema)

    channels_by_type = {}

    try:
        # Create a new MCAP file for recording
        with foxglove.open_mcap(mcap_filepath, allow_overwrite=overwrite):
            min_timestamp = math.inf
            max_timestamp = 0
            tz = gpx.tracks[0].segments[0].points[0].time.tzinfo

            # Process each track
            for track in gpx.tracks:
                # Process each segment
                for segment in track.segments:
                    # Process each point
                    for point in segment.points:
                        log_time = int(point.time.timestamp() * 1e9)
                        min_timestamp = min(min_timestamp, log_time)
                        max_timestamp = max(max_timestamp, log_time)

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

                        location_chan.log(
                            LocationFix(
                                latitude=point.latitude,
                                longitude=point.longitude,
                                altitude=point.elevation,
                            ),
                            log_time=log_time,
                        )

                        metrics_chan.log(
                            {
                                "elevation": point.elevation,
                                **extension_values,
                            },
                            log_time=log_time,
                        )

            xml_filepath = "apple_health_export/export.xml"
            min_datetime = datetime.fromtimestamp(min_timestamp / 1e9, tz)
            max_datetime = datetime.fromtimestamp(max_timestamp / 1e9, tz)
            min_datetime_str = min_datetime.strftime("%Y-%m-%d")

            for _, elem in ET.iterparse(xml_filepath):
                # skip if the sourceName is in the excluded list
                if elem.attrib.get("sourceName") in excluded_source_names:
                    continue

                # skip entries until we find the date we care about
                # we compare strings so we don't need to create a datetime object
                # until we're at the right date
                start_date = elem.attrib.get("startDate")
                if not start_date or not start_date.startswith(min_datetime_str):
                    continue

                # now that we're at the right date, skip over entries before the
                # start_time we care about
                start_datetime = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S %z")
                if start_datetime < min_datetime:
                    continue
                # and stop processing once we're past the end_time we care about
                if start_datetime > max_datetime:
                    break

                raw_type = elem.attrib.get("type")
                if raw_type is None:
                    logger.warning(f"No type for {elem.attrib}")
                    continue

                formatted_type = raw_type.replace("HKQuantityTypeIdentifier", "")
                curr_channel = channels_by_type.get(formatted_type)
                if curr_channel is None:
                    curr_channel = Channel(
                        topic=f"/{formatted_type}", schema=workout_metrics_schema
                    )
                    channels_by_type[formatted_type] = curr_channel

                log_time = None
                try:
                    log_time = int(
                        datetime.strptime(
                            elem.attrib.get("creationDate"), "%Y-%m-%d %H:%M:%S %z"
                        ).timestamp()
                        * 1e9
                    )
                except Exception as e:
                    logger.warning(
                        f"Invalid creationDate: {elem.attrib.get('creationDate')}"
                    )
                    continue

                curr_channel.log(
                    {
                        "unit": elem.attrib.get("unit"),
                        "value": elem.attrib.get("value"),
                        "startDate": elem.attrib.get("startDate"),
                        "endDate": elem.attrib.get("endDate"),
                        "sourceName": elem.attrib.get("sourceName"),
                        "sourceVersion": elem.attrib.get("sourceVersion"),
                        "device": elem.attrib.get("device"),
                        "creationDate": elem.attrib.get("creationDate"),
                        "tag": elem.tag,
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
