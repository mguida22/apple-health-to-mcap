import foxglove
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from foxglove import Channel
from typing import Dict, Optional, Tuple
from xml.etree import ElementTree


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkoutSummary:
    workout_type: str
    source_name: str
    source_version: str
    duration: float
    duration_unit: str
    start_date: str
    end_date: str
    device: Optional[str]


start_stop_schema = {
    "type": "object",
    "title": "start_stop",
    "properties": {
        "event": {"type": "string"},
        "reason": {"type": "string"},
    },
}

hk_metrics_schema = {
    "type": "object",
    "title": "hk_metrics",
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


def fmt_time(time_str: str) -> int:
    return int(datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S %z").timestamp() * 1e9)


def process_xml_export_to_mcap(
    workout: WorkoutSummary,
    xml_filepath: str,
    output_dir: str,
    overwrite: bool = False,
    filename: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    """
    Process an Apple Health XML export file and convert it to MCAP format.

    Args:
        xml_filepath (str): Path to the input XML file
        output_dir (str): Directory to save the output MCAP file
        overwrite (bool): Whether to overwrite existing files

    Returns:
        Tuple[str, Optional[str]]: Path to the generated MCAP file and GPX file path
    """
    if filename is None:
        existing_name = os.path.splitext(os.path.basename(xml_filepath))[0]
        mcap_filepath = os.path.join(output_dir, existing_name + ".mcap")
    else:
        mcap_filepath = os.path.join(output_dir, filename)

    gpx_path = None
    try:
        # Create a new MCAP file for recording
        with foxglove.open_mcap(mcap_filepath, allow_overwrite=overwrite):
            logger.info(f"Processing workout from export file at {xml_filepath}")
            # process the workout element
            root = ElementTree.parse(xml_filepath).getroot()
            for elem in root.iterfind("Workout"):
                # we don't have ids so we check by start/end date and source name/version
                # to be reasonably sure we're processing the right workout
                if (
                    elem.attrib.get("startDate") != workout.start_date
                    or elem.attrib.get("endDate") != workout.end_date
                    or elem.attrib.get("sourceName") != workout.source_name
                    or elem.attrib.get("sourceVersion") != workout.source_version
                ):
                    continue

                for child in elem:
                    path = process_workout_child_elem(child)
                    if path:
                        gpx_path = path

            # process other data that we find by time in the export.xml file,
            # like heart rate, etc.
            # TODO: there's probably a way to do all this in the same pass
            logger.info(
                f"Processing other metrics that were logged during the workout from export file at {xml_filepath}"
            )
            workout_start_date = datetime.strptime(
                workout.start_date, "%Y-%m-%d %H:%M:%S %z"
            )
            channels: Dict[str, Channel] = {}
            for _, elem in ElementTree.iterparse(xml_filepath):
                # narrow down by the date before we parse the string into a datetime
                start_date = elem.attrib.get("startDate")
                if start_date and start_date.startswith(
                    workout_start_date.strftime("%Y-%m-%d")
                ):
                    raw_type = elem.attrib.get("type")
                    if raw_type is None:
                        logger.warning(f"No type for {elem.tag} {elem.attrib}")
                        continue

                    formatted_type = raw_type.replace("HKQuantityTypeIdentifier", "")
                    curr_channel = channels.get(formatted_type)
                    if curr_channel is None:
                        curr_channel = Channel(
                            topic=f"/{formatted_type}", schema=hk_metrics_schema
                        )
                        channels[formatted_type] = curr_channel

                    log_time = None
                    try:
                        log_time = fmt_time(elem.attrib.get("endDate"))
                    except:
                        logger.warning(f"Invalid endDate: {elem.attrib.get('endDate')}")
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

    logger.info(f"Finished processing workout. GPX path: {gpx_path}")
    return (mcap_filepath, gpx_path)


def process_workout_child_elem(
    elem: ElementTree.Element, channels: Dict[str, Channel]
) -> Optional[str]:
    tag = elem.tag
    attrs = elem.attrib

    gpx_path = None

    if tag == "MetadataEntry":
        # metadata about the entire workout (not time-specific)
        pass

    elif tag == "WorkoutStatistics":
        # statistics about the entire workout (not time-specific)
        pass

    elif tag == "WorkoutEvent":
        event_type = attrs.get("type")
        if event_type == "HKWorkoutEventTypePause":
            # handles Slopes trigger reason
            child = elem.find(
                "MetadataEntry[@key='com.consumedbycode.slopes.hk.trigger_reason']"
            )
            reason = child.attrib.get("value") if child else None
            curr_channel = channels.get("start-stop")
            if curr_channel is None:
                curr_channel = Channel(topic=f"/start-stop", schema=start_stop_schema)
                channels["start-stop"] = curr_channel

            logger.info(f"Logging pause event: {reason}")
            curr_channel.log(
                {"event": "Pause", "reason": reason},
                log_time=fmt_time(attrs.get("date")),
            )
        elif event_type == "HKWorkoutEventTypeResume":
            child = elem.find(
                "MetadataEntry[@key='com.consumedbycode.slopes.hk.trigger_reason']"
            )
            reason = child.attrib.get("value") if child else None
            curr_channel = channels.get("start-stop")
            if curr_channel is None:
                curr_channel = Channel(topic=f"/start-stop", schema=start_stop_schema)
                channels["start-stop"] = curr_channel

            logger.info(f"Logging resume event: {reason}")
            curr_channel.log(
                {"event": "Resume", "reason": reason},
                log_time=fmt_time(attrs.get("date")),
            )
        elif event_type == "HKWorkoutEventTypeSegment":
            # segment metadata. not clear how segments are split up.
            pass

    elif tag == "WorkoutRoute":
        for child in elem:
            if child.tag == "FileReference":
                path = child.attrib.get("path")
                if path:
                    gpx_path = path

    return gpx_path
