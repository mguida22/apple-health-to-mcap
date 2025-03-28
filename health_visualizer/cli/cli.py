import argparse
from dataclasses import asdict
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Optional


from health_visualizer.processor.combined import process_combined_to_mcap
from health_visualizer.processor.gpx import process_gpx_to_mcap
from health_visualizer.processor.xml_export import (
    WorkoutSummary,
    process_xml_export_to_mcap,
)


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Process Apple Health Kit data into MCAP."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("./apple_health_export"),
        help="Path to the apple health export directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./output"),
        help="Directory to export the selected route file to .",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing files."
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all GPX files in the workout-routes directory.",
    )
    parser.add_argument(
        "--by-route",
        action="store_true",
        help="Process data by workout route.",
    )
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.by_route:
        # process by a workout_route
        handle_by_route(args)
    else:

        # process by workouts from the export.xml file
        handle_by_workouts_from_export(args)


def handle_by_route(args: argparse.Namespace):
    workout_routes_dir = os.path.join(args.input_dir, "workout-routes")
    gpx_files = [f for f in os.listdir(workout_routes_dir) if f.endswith(".gpx")]

    if not gpx_files:
        print(
            f"No GPX files found in the {workout_routes_dir} directory. Specify a different Apple Health Kit export directory with the --input-dir flag."
        )
        return

    # Process all GPX files in the workout-routes directory.
    if args.all:
        print(
            f"Processing all ({len(gpx_files)}) GPX files in the workout-routes directory."
        )
        for file in gpx_files:
            process_gpx_to_mcap(
                os.path.join(workout_routes_dir, file),
                args.output_dir,
                args.overwrite,
            )

        print(f"Finished processing {len(gpx_files)} GPX files.")
        return

    # pull datetime from each filename
    datetime_data: list[tuple[str, datetime]] = []
    for filename in gpx_files:
        dt = parse_route_datetime(filename)
        if dt:
            datetime_data.append((filename, dt))

    # Sort by the datetime
    datetime_data.sort(key=lambda x: x[1], reverse=True)

    if not datetime_data:
        print("No workout route files matched the expected format.")
        return

    # show the 10 most recent workout routes
    print("Most recent workout routes:")
    for idx, (filename, dt) in enumerate(datetime_data[:10], start=1):
        print(f"{idx}) {dt.strftime('%Y-%m-%d %I:%M %p')}")

    # ask the user to pick a workout route by number
    # or type 'list' to see all workout routes
    while True:
        choice = input(
            "Enter the number of the workout route you want to process (or 'list' to see all routes): "
        )
        if choice == "list":
            for idx, (filename, dt) in enumerate(datetime_data, start=1):
                print(f"{idx}) {dt.strftime('%Y-%m-%d %I:%M %p')}")
        elif choice.isdigit() and 1 <= int(choice) <= len(datetime_data):
            break
        else:
            print("Invalid input. Please enter a number or 'list'.")

    # we have a valid choice now
    chosen_file = datetime_data[int(choice) - 1]
    print(f"Processing {chosen_file}...")
    output_filepath = process_combined_to_mcap(
        os.path.join(workout_routes_dir, chosen_file),
        args.output_dir,
        args.overwrite,
    )
    print(f"Finished processing. Saved to {output_filepath}")


def parse_route_datetime(filename: str) -> Optional[datetime]:
    """
    Attempt to parse a datetime from a filename of the form:
    route_2020-11-30-3.53pm.gpx

    Returns a datetime object if parsing is successful;
    otherwise returns None.
    """
    # For example, filename might be: route_2020-11-30_3.53pm.gpx
    # We'll try to capture groups: year, month, day, hour, minute, am/pm
    # route_YYYY-MM-DD-H.MM(am|pm).gpx
    pattern = r"route_(\d{4})-(\d{2})-(\d{2})_(\d{1,2})\.(\d{2})(am|pm)\.gpx"
    match = re.match(pattern, filename, re.IGNORECASE)
    if not match:
        return None

    year, month, day, hour_str, minute_str, ampm = match.groups()
    year = int(year)
    month = int(month)
    day = int(day)
    hour = int(hour_str)
    minute = int(minute_str)

    # Convert 12-hour format to 24-hour format
    if ampm.lower() == "pm" and hour < 12:
        hour += 12
    elif ampm.lower() == "am" and hour == 12:
        hour = 0

    try:
        return datetime(year=year, month=month, day=day, hour=hour, minute=minute)
    except ValueError:
        return None


def handle_by_workouts_from_export(args: argparse.Namespace):
    workouts = get_workouts_by_type_from_apple_health_export(args.input_dir)

    if not workouts:
        print("No workouts found in the export.xml file.")
        return

    # show the 10 most recent workouts
    print("Most recent workouts:")
    for idx, workout in enumerate(workouts[:10], start=1):
        print(
            f"{idx}) {workout.workout_type} ({workout.source_name}) - {workout.start_date} ({round(workout.duration)} {workout.duration_unit})"
        )

    # ask the user to pick a workout by number
    # or type 'list' to see all workouts
    while True:
        choice = input(
            "Enter the number of the workout you want to process (or 'list' to see all workouts): "
        )
        if choice == "list":
            for idx, workout in enumerate(workouts, start=1):
                print(
                    f"{idx}) {workout.workout_type} ({workout.source_name}) - {workout.start_date} ({round(workout.duration)} {workout.duration_unit})"
                )
        elif choice.isdigit() and 1 <= int(choice) <= len(workouts):
            break
        else:
            print("Invalid input. Please enter a number or 'list'.")

    # we have a valid choice now
    chosen_workout = workouts[int(choice) - 1]
    print(f"Processing workout...")

    # get the workout details from the export.xml file
    filename = f"{chosen_workout.workout_type}-{chosen_workout.start_date}.mcap"
    output_filepath, gpx_path = process_xml_export_to_mcap(
        chosen_workout,
        os.path.join(args.input_dir, "export.xml"),
        args.output_dir,
        args.overwrite,
        filename,
    )

    if gpx_path:
        # process the gpx file
        process_gpx_to_mcap(gpx_path, args.output_dir, args.overwrite)

    print(f"Finished processing. Saved to {output_filepath}")


def get_workouts_by_type_from_apple_health_export(
    input_dir: Path,
) -> list[WorkoutSummary]:
    """
    Get all workouts by type from the Apple Health export directory.
    """
    # check if the cache file exists
    cache_path = os.path.join(input_dir, "workout_cache.json")
    try:
        if os.path.exists(cache_path):
            logger.info(f"Loading workouts from cache file {cache_path}")
            with open(cache_path, "r") as f:
                return [WorkoutSummary(**workout) for workout in json.load(f)]
    except Exception as e:
        logger.error(f"Invalid cache file. Deleting {cache_path}: {e}")
        os.remove(cache_path)

    workouts: list[WorkoutSummary] = []
    export_path = os.path.join(input_dir, "export.xml")
    logger.info(f"Building cache file from {export_path}")
    if not os.path.exists(export_path):
        raise FileNotFoundError(f"Export file not found at {export_path}")

    for _, elem in ET.iterparse(export_path):
        if elem.tag != "Workout":
            continue

        if elem.tag == "Workout":
            workout_type = elem.attrib.get("workoutActivityType", "")
            parsed_workout_type = workout_type.replace("HKWorkoutActivityType", "")
            workouts.append(
                WorkoutSummary(
                    workout_type=parsed_workout_type,
                    source_name=elem.attrib.get("sourceName", ""),
                    source_version=elem.attrib.get("sourceVersion", ""),
                    duration=float(elem.attrib.get("duration", 0)),
                    duration_unit=elem.attrib.get("durationUnit", ""),
                    start_date=elem.attrib.get("startDate", ""),
                    end_date=elem.attrib.get("endDate", ""),
                    device=elem.attrib.get("device", None),
                )
            )

    sorted_workouts = sorted(workouts, key=lambda x: x.start_date, reverse=True)

    # write to cache file
    with open(cache_path, "w") as f:
        logger.info(f"Writing workouts to cache file {cache_path}")
        json.dump([asdict(workout) for workout in sorted_workouts], f)

    return sorted_workouts
