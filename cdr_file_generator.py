#!/usr/bin/env python3
"""Continuously generates synthetic voice CDR files (pipe-separated, no header)
matching VOICE_SCHEMA.txt, for Apache NiFi to pick up."""

import os
import random
import string
import time
from datetime import datetime, timedelta

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "voice_cdrs")
RECORDS_PER_SECOND = 20
RECORDS_PER_FILE = 20
DATE_FMT = "%Y%m%d%H%M"

CDR_TYPES = ["01", "02", "03"]
STATUS_CODES = ["0"] * 8 + ["1", "2"]  # mostly successful calls
MSISDN_PREFIX = "252"

_cdr_counter = random.randint(2000000, 2999999)
_callref_counter = random.randint(900000000, 999999999)


def next_cdr_id():
    global _cdr_counter
    _cdr_counter += 1
    return str(_cdr_counter)


def next_call_reference():
    global _callref_counter
    _callref_counter += 1
    return str(_callref_counter)


def random_digits(n):
    return "".join(random.choices(string.digits, k=n))


def random_msisdn():
    return MSISDN_PREFIX + random_digits(9)


def random_imsi():
    return "63201" + random_digits(10)


def random_imei():
    return random.choice(string.digits[1:]) + random_digits(14)


def random_cellid():
    return random_digits(6)


def generate_record():
    now = datetime.now()
    duration = random.randint(5, 600)
    start_date = now - timedelta(seconds=duration)
    end_date = now
    create_date = end_date

    calling_number = random_msisdn()

    fields = [
        next_cdr_id(),
        random.choice(CDR_TYPES),
        random.choice(STATUS_CODES),
        create_date.strftime(DATE_FMT),
        start_date.strftime(DATE_FMT),
        end_date.strftime(DATE_FMT),
        calling_number,
        str(duration),
        calling_number,
        random_msisdn(),
        random_imsi(),
        random_imsi(),
        random_msisdn(),
        random_cellid(),
        str(duration),
        str(random.randint(0, 10)),
        next_call_reference(),
        random_imei(),
        random_msisdn(),
    ]
    return "|".join(fields)


def new_filename():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    suffix = random_digits(3)
    return f"cdr_voice_{timestamp}_{suffix}.p"


def write_file(records):
    if not records:
        return
    path = os.path.join(OUTPUT_DIR, new_filename())
    with open(path, "w") as f:
        f.write("\n".join(records) + "\n")
    print(f"Wrote {len(records)} records to {path}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    interval = 1.0 / RECORDS_PER_SECOND
    buffer = []
    try:
        while True:
            start = time.monotonic()
            buffer.append(generate_record())
            if len(buffer) >= RECORDS_PER_FILE:
                write_file(buffer)
                buffer = []
            elapsed = time.monotonic() - start
            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        write_file(buffer)
        print("Stopped.")


if __name__ == "__main__":
    main()
