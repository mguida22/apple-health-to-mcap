from xml.etree import ElementTree
import foxglove
from foxglove import Channel
import os
import logging
import datetime

logger = logging.getLogger(__name__)

excluded_source_names = []

metrics_schema = {
    "type": "object",
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


def process_xml_export_to_mcap(
    xml_filepath: str, output_dir: str, overwrite: bool = False
) -> str:
    """
    Process an Apple Health XML export file and convert it to MCAP format.

    Args:
        xml_filepath (str): Path to the input XML file
        output_dir (str): Directory to save the output MCAP file
        overwrite (bool): Whether to overwrite existing files

    Returns:
        str: Path to the generated MCAP file
    """
    # Create output filename
    existing_name = os.path.splitext(os.path.basename(xml_filepath))[0]
    mcap_filepath = os.path.join(output_dir, existing_name + ".mcap")

    channels_by_type = {}
    try:
        # Create a new MCAP file for recording
        with foxglove.open_mcap(mcap_filepath, allow_overwrite=overwrite):
            for _, elem in ElementTree.iterparse(xml_filepath):
                # skip if the sourceName is in the excluded list
                if elem.attrib.get("sourceName") in excluded_source_names:
                    continue

                start_date = elem.attrib.get("startDate")
                if start_date and start_date.startswith("2025-03-22"):
                    raw_type = elem.attrib.get("type")
                    if raw_type is None:
                        logger.warning(f"No type for {elem.attrib}")
                        continue

                    formatted_type = raw_type.replace("HKQuantityTypeIdentifier", "")
                    curr_channel = channels_by_type.get(formatted_type)
                    if curr_channel is None:
                        curr_channel = Channel(
                            topic=f"/{formatted_type}", schema=metrics_schema
                        )
                        channels_by_type[formatted_type] = curr_channel

                    log_time = None
                    try:
                        log_time = int(
                            datetime.datetime.strptime(
                                elem.attrib.get("endDate"), "%Y-%m-%d %H:%M:%S %z"
                            ).timestamp()
                            * 1e9
                        )
                    except Exception:
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
