# Apple Health Visualizer

Process data from an Apple Health export and visualize it in [Foxglove](https://foxglove.dev/). This tool uses the [Foxglove SDK](https://docs.foxglove.dev/docs/sdk/introduction?lang=python) to produce [MCAP](https://mcap.dev/) files that can be visualized in Foxglove.

## Downloading Apple Health Data

1. Open Health app and click on your profile icon in the top right.
2. Scroll down and select "Export All Health Data". It will generate a zip file.
3. Share the zip file to your computer.

## Setup

```sh
poetry install
```

## Running

To process workouts from the export file run

```sh
poetry run health_visualizer
```

## Visualizing

Open Foxglove and import your mcap file to view. You can import the layout file [apple_health_route.json](layouts/apple_health_route.json) as a starting point.
