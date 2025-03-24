# Foxglove SDK Hack Week

Take data from Apple Health (and maybe Strava) and visualize it in [Foxglove](https://foxglove.dev/).

## Downloading Apple Health Data

1. Open Health app and click on your profile icon in the top right.
2. Scroll down and select "Export All Health Data". It will generate a zip file.
3. Share the zip file to your computer.

## Supported Data Types

- [x] .gpx files
- [ ] export.xml file from Apple Health export
- [ ] ecg .csv files

## Running

```sh
poetry run health_visualizer --input apple_health_export/workout-routes/route_2025-03-22_1.07pm.gpx --overwrite
```

## Visualizing

Open Foxglove and import your mcap file.
