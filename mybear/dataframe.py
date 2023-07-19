import statistics
import csv
import json
from typing import List, Dict, Callable, Any, Union
from series import Series
import numpy as np

class DataFrame:
    def __init__(self, data=None, columns=None):
        if isinstance(data, list) and all(isinstance(i, Series) for i in data):
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
        return DataFrame({col: [statistics.mean(vals)] for col, vals in self.data.items() if isinstance(vals[0], (int, float))})

    def std(self):
        return DataFrame({col: [statistics.stdev(vals)] for col, vals in self.data.items() if isinstance(vals[0], (int, float))})

    def count(self):
        return DataFrame({col: [len(vals)] for col, vals in self.data.items()})

    @staticmethod
    def read_csv(path: str, delimiter: str = ",") -> 'DataFrame':
        with open(path, 'r') as f:
            reader = csv.reader(f, delimiter=delimiter)
            data = list(reader)
            columns = data[0]
            data = [[int(value) if value.isdigit() else value for value in values] for values in data[1:]]
            data = list(map(list, zip(*data)))
            return DataFrame(data, columns)

    @staticmethod
    def read_json(path: str, orient: str = "records") -> 'DataFrame':
        with open(path, 'r') as f:
            data = json.load(f)
            if orient == 'records':
                columns = list(data[0].keys())
                data = [[int(value) if isinstance(value, int) else value for value in item.values()] for item in data]
                data = list(map(list, zip(*data)))
                return DataFrame(data, columns)
            elif orient == 'columns':
                return DataFrame([list(vals) for vals in data.values()], list(data.keys()))

    def groupby(self, by: Union[List[str], str], agg: Dict[str, Callable[[List[Any]], Any]]) -> 'DataFrame':
        #Test pour savoir si by est un string pour la convertir en liste
        if isinstance(by, str):
            by = [by]


        if len(by) != 1:
            raise ValueError("Pour l'instant le groupement ne fonctionne que sur une seule colonne")

        group_col = by[0]
        unique_vals = list(set(self.data[group_col]))
        result = {group_col: unique_vals}

        for agg_col, agg_func in agg.items():
            agg_results = []
            for val in unique_vals:
                matching_rows = [i for i, x in enumerate(self.data[group_col]) if x == val]
                agg_data = [self.data[agg_col][i] for i in matching_rows]
                agg_results.append(agg_func(agg_data))
            result[agg_col] = agg_results

        return DataFrame(result)

    def join(self, other: 'DataFrame', left_on: Union[List[str], str], right_on: Union[List[str], str],
             how: str = "left") -> 'DataFrame':
        if isinstance(left_on, str):
            left_on = [left_on]

        if isinstance(right_on, str):
            right_on = [right_on]

        result = {}
        for column in self.data:
            if column in left_on:
                result[column] = self.data[column]
            else:
                result[column] = self.data[column]

        for column in other.data:
            if column in right_on and column not in result:
                result[column] = other.data[column]

        return DataFrame(result)
