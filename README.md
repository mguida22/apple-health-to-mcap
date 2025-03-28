# Foxglove SDK Hack Week

Process data from an Apple Health export and visualize it in [Foxglove](https://foxglove.dev/) using the [Foxglove SDK](https://docs.foxglove.dev/docs/sdk/introduction?lang=python).

## Downloading Apple Health Data

1. Open Health app and click on your profile icon in the top right.
2. Scroll down and select "Export All Health Data". It will generate a zip file.
3. Share the zip file to your computer.

## Running

```sh
poetry run health_visualizer
```

## Supported Data Types

- [x] workouts from the export.xml file
- [x] .gpx files
- [x] ecg .csv files

## Visualizing

Open Foxglove and import your mcap file to view.
