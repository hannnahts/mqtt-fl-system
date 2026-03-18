import argparse
import pandas as pd
import numpy as np
import os

DEFAULT_INPUT = "data/processed/clean_air.csv"
DEFAULT_OUTPUT_DIR = "data/splits"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--clients", type=int, default=5)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    os.makedirs(args.output_dir, exist_ok=True)
    splits = np.array_split(df, args.clients)

    for i, part in enumerate(splits, start=1):
        path = os.path.join(args.output_dir, f"client_{i}.csv")
        part.to_csv(path, index=False)
        print("saved", path)


if __name__ == "__main__":
    main()
