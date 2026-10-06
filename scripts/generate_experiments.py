"""Write a reproducible config for the official experiment producer."""

import argparse
import json


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--sensors", type=int, default=2)
    parser.add_argument("--sample-rate-ms", type=int, default=1000)
    parser.add_argument("--samples", type=int, default=20)
    arguments = parser.parse_args()
    if min(
        arguments.count,
        arguments.sensors,
        arguments.sample_rate_ms,
        arguments.samples,
    ) < 1:
        parser.error("all numeric values must be positive")

    config = [
        {
            "start_time": 0,
            "researcher": "d.landau@uu.nl",
            "num_sensors": arguments.sensors,
            "sample_rate": arguments.sample_rate_ms,
            "stabilization_samples": 2,
            "carry_out_samples": arguments.samples,
            "start_temperature": 16,
            "temp_range": {
                "lower_threshold": 25.5,
                "upper_threshold": 26.5,
            },
        }
        for _ in range(arguments.count)
    ]
    print(json.dumps(config))


if __name__ == "__main__":
    main()
