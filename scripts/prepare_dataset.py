import argparse
import pandas as pd
import os

DEFAULT_INPUT_FILE = "data/raw/NEWC_2025.csv"
DEFAULT_OUTPUT_FILE = "data/processed/clean_air.csv"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE)
    args = parser.parse_args()

    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(
        args.input,
        skiprows=4
    )

    df = df.rename(columns={
        "PM<sub>10</sub> particulate matter (Hourly measured)": "PM10",
        "Nitric oxide": "NO",
        "Nitrogen dioxide": "NO2",
        "Ozone": "O3",
        "PM<sub>2.5</sub> particulate matter (Hourly measured)": "PM25"
    })

    required_columns = ["PM10", "NO", "NO2", "O3", "PM25"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing expected columns: {missing_columns}")

    df = df[required_columns]
    df = df.dropna()
    df.to_csv(args.output, index=False)
    print("dataset cleaned:", df.shape)


if __name__ == "__main__":
    main()
