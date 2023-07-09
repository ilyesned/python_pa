import statistics
import csv
import json
import series as ser
from typing import List, Dict, Callable, Any, Union


class DataFrame:
    def __init__(self, data=None, columns=None):
        if isinstance(data, list) and all(isinstance(i, ser) for i in data):
            self.data = {series.name: series.data for series in data}
        elif isinstance(columns, list) and isinstance(data, list):
            self.data = {col: vals for col, vals in zip(columns, data)}
        else:
            self.data = data or {}

    @property
    def iloc(self):
        return self.data

    def max(self):
        return DataFrame({col: [max(vals)] for col, vals in self.data.items()})

    def min(self):
        return DataFrame({col: [min(vals)] for col, vals in self.data.items()})

    def mean(self):
        return DataFrame({col: [statistics.mean(vals)] for col, vals in self.data.items()})

    def std(self):
        return DataFrame({col: [statistics.stdev(vals)] for col, vals in self.data.items()})

    def count(self):
        return DataFrame({col: [len(vals)] for col, vals in self.data.items()})

    @staticmethod
    def read_csv(path: str, delimiter: str = ",") -> 'DataFrame':
        with open(path, 'r') as f:
            reader = csv.reader(f, delimiter=delimiter)
            data = list(reader)
            columns = data[0]
            data = list(map(list, zip(*data[1:])))
            return DataFrame(data, columns)

    @staticmethod
    def read_json(path: str, orient: str = "records") -> 'DataFrame':
        with open(path, 'r') as f:
            data = json.load(f)
            if orient == 'records':
                columns = data[0].keys()
                data = [list(item.values()) for item in data]
                data = list(map(list, zip(*data)))
                return DataFrame(data, columns)
            elif orient == 'columns':
                return DataFrame([list(vals) for vals in data.values()], list(data.keys()))

    def groupby(self, by: Union[List[str], str], agg: Dict[str, Callable[[List[Any]], Any]]) -> 'DataFrame':
        if isinstance(by, str):
            by = [by]

        result = {}
        for column in by:
            for name, function in agg.items():
                if name not in result:
                    result[name] = [function(self.data[name])]
                else:
                    result[name].append(function(self.data[name]))

        return DataFrame(result)

    def join(self, other: 'DataFrame', left_on: Union[List[str], str], right_on: Union[List[str], str],
             how: str = "left") -> 'DataFrame':
        # left join only for simplicity
        if isinstance(left_on, str):
            left_on = [left_on]

        if isinstance(right_on, str):
            right_on = [right_on]

        # create the result
        result = {}
        for column in self.data:
            if column in left_on:
                result[column] = self.data[column]
            else:
                result[column] = self.data[column] + [None] * len(other.data[right_on[0]])

        for column in other.data:
            if column in right_on:
                if column not in result:
                    result[column] = [None] * len(self.data[left_on[0]]) + other.data[column]
            else:
                result[column] = [None] * len(self.data[left_on[0]]) + other.data[column]

        return DataFrame(result)