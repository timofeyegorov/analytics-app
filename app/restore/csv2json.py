#!/usr/bin/env python

import json
import pandas

from pathlib import Path


def run():
    files = ["1.csv", "2.csv"]
    for file in files:
        csvpath = Path(__file__).parent / file
        jsonpath = Path(__file__).parent / f'{"".join(file.split(".")[:-1])}.json'
        with open(csvpath) as csv_ref, open(jsonpath, "w") as json_ref:
            reader = pandas.read_csv(csv_ref)
            data = reader.to_dict(orient="records")
            for item in data:
                item.update(
                    {
                        "sys": {
                            "tilda_created": item.get("sent"),
                            "tilda_tranid": item.get("requestid"),
                        }
                    }
                )
            data = {"leads": data}
            json.dump(data, json_ref)


if __name__ == "__main__":
    run()
