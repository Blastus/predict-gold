#! /usr/bin/env python3
import csv
import datetime
import pathlib
import statistics


def date_type_1(date_string):
    return datetime.datetime.strptime(date_string, '%m/%d/%y').date()


def date_type_2(date_string):
    return datetime.datetime.strptime(date_string, '%Y-%m-%d').date()


CSV = '.CSV'.casefold()
CSV_TYPES = dict(Close=float,
                 High=float,
                 Low=float,
                 Open=float,
                 Volume=float,
                 Date=date_type_1,
                 date=date_type_2,
                 value=float)
DAY = datetime.timedelta(1)

DEPENDENT_FILES = {('Gold Price 1', 'BUNDESBANK-BBK01_WT5511.csv'),
                   ('Gold Price 2', 'GOLDPMGBD228NLBM.csv')}
DEPENDENT_COLUMN = 'value'
DEPENDENT_VARIABLES = 'Dependent Variables'
INDEPENDENT_COLUMN = 'Close'
INDEPENDENT_VARIABLES = 'Independent Variables'

HISTORICAL_START_DATE = datetime.date(2015, 1, 1)
HISTORICAL_STOP_DATE = datetime.date(2016, 1, 1)
HISTORICAL_OUTPUT = 'historical_values.csv'
FIELD_NAMES = 'field_names.txt'

CURRENT_START_DATE = HISTORICAL_STOP_DATE
CURRENT_STOP_DATE = datetime.date(2016, 4, 1)


def main():
    valid_date = is_weekday('Tuesday', 'Thursday')
    historical_dates = tuple(date_range(HISTORICAL_START_DATE,
                                        HISTORICAL_STOP_DATE,
                                        DAY))
    historical_table = {}
    # collection all historical, independent values
    for path in pathlib.Path(INDEPENDENT_VARIABLES).iterdir():
        if path.suffix.lower() == CSV:
            with path.open('rt', newline='') as file:
                table = build_table('Date', map(
                    cast_row, map(strip_all, csv.DictReader(file))))
            historical_data = extract_data(table, historical_dates,
                                           INDEPENDENT_COLUMN)
            if any(value is not None for value in historical_data):
                patch_range(historical_data)
                final_data = [item for item, date in
                              zip(historical_data, historical_dates)
                              if valid_date(date)]
                historical_table[path.stem] = final_data
    # collect all historical, dependent values
    for name, item in DEPENDENT_FILES:
        with pathlib.Path(DEPENDENT_VARIABLES, item).open(
                'rt', newline='') as file:
            table = build_table('date', filter_map(
                cast_row, map(lower_keys, csv.DictReader(file))))
        historical_data = extract_data(table, historical_dates,
                                       DEPENDENT_COLUMN)
        if any(value is not None for value in historical_data):
            patch_range(historical_data)
            final_data = [item for item, date in
                          zip(historical_data, historical_dates)
                          if valid_date(date)]
            historical_table[name] = final_data
    # write out to a file
    transposed_table = pivot_from_columns_to_rows(historical_table)
    with pathlib.Path(HISTORICAL_OUTPUT).open('wt', newline='') as file:
        writer = csv.DictWriter(file, sorted(next(transposed_table)))
        writer.writeheader()
        writer.writerows(transposed_table)
    with pathlib.Path(FIELD_NAMES).open('wt') as file:
        print(*writer.fieldnames, sep='\n', file=file, flush=True)


def strip_all(dictionary):
    return {key.strip(): value.strip() for key, value in dictionary.items()}


def cast_row(dictionary):
    return {key: CSV_TYPES[key](value) for key, value in dictionary.items()}


def build_table(primary_key, rows):
    return {row.pop(primary_key): row for row in rows}


def extract_data(table, dates, column):
    return [table[date][column] if date in table else None
            for date in dates]


def date_range(start, stop, step):
    while start < stop:
        yield start
        start += step


def patch_range(column):
    if column[0] is None:
        mean = statistics.mean(filter(lambda item: item is not None, column))
        column[0] = mean
    last_value = None
    for offset, value in enumerate(column):
        if value is None:
            column[offset] = last_value
        else:
            last_value = value


def is_weekday(*args):
    weekdays = frozenset(map(str.casefold, args))

    def validator(date):
        return any(map(date.strftime('%A').casefold().startswith, weekdays))
    return validator


def lower_keys(dictionary):
    return {key.lower(): value for key, value in dictionary.items()}


def filter_map(function, iterable):
    for item in iterable:
        try:
            yield function(item)
        except ValueError:
            pass


def pivot_from_columns_to_rows(table):
    field_names, columns = zip(*table.items())
    yield field_names
    for row in zip(*columns):
        yield dict(zip(field_names, row))


if __name__ == '__main__':
    main()
