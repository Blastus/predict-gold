#! /usr/bin/env python3
import csv
import datetime
import pathlib


def date(date_string):
    return datetime.datetime.strptime(date_string, '%m/%d/%y').date()


CSV = '.CSV'.casefold()
CSV_TYPES = dict(Close=float,
                 High=float,
                 Low=float,
                 Open=float,
                 Volume=float,
                 Date=date)
DEPENDENT_VARIABLES = 'Dependent Variables'
INDEPENDENT_VARIABLES = 'Independent Variables'


def main():
    path = pathlib.Path(INDEPENDENT_VARIABLES)
    count = 0
    field_names = set()
    for item in path.iterdir():
        if item.suffix.lower() == CSV:
            with item.open('rt', newline='') as file:
                reader = csv.DictReader(file)
                for row in map(cast_row, map(strip_all, reader)):
                    pass
                print(row)
                field_names.update(reader.fieldnames)
    print(count)
    print(sorted(field_names))


def strip_all(dictionary):
    return {key.strip(): value.strip() for key, value in dictionary.items()}


def cast_row(dictionary):
    return {key: CSV_TYPES[key](value) for key, value in dictionary.items()}


if __name__ == '__main__':
    main()
