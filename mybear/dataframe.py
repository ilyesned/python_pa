import numpy as np
import pandas as pd

class DataFrame:
    def __init__(self, data):
        self.data = {col: series(data[col]) for col in data}

    def max(self):
        return {col: series.max() for col, series in self.data.items()}

    def min(self):
        return {col: series.min() for col, series in self.data.items()}

    def mean(self):
        return {col: series.mean() for col, series in self.data.items()}

    def std(self):
        return {col: series.std() for col, series in self.data.items()}

    def count(self):
        return {col: series.count() for col, series in self.data.items()}

    @staticmethod
    def read_csv(file_path):
        data = pd.read_csv(file_path)
        return DataFrame(data.to_dict())

    @staticmethod
    def read_json(file_path):
        data = pd.read_json(file_path)
        return DataFrame(data.to_dict())

    def groupby(self, column_name):
        # Vérifier que la colonne existe
        assert column_name in self.data, f"Unknown column {column_name}"

        # Créer un dictionnaire pour stocker les groupes
        groups = {}

        # Parcourir les données et les regrouper par la colonne choisie
        for i, value in enumerate(self.data[column_name]):
            if value not in groups:
                groups[value] = DataFrame({k: [v[i]] for k, v in self.data.items()})
            else:
                for k, v in self.data.items():
                    groups[value].data[k].append(v[i])

        return groups

    def join(self, other, on):
        # Vérifier que la colonne existe dans les deux DataFrames
        assert on in self.data, f"Unknown column {on} in self"
        assert on in other.data, f"Unknown column {on} in other"

        # Créer un nouveau DataFrame pour stocker le résultat
        result_data = {k: [] for k in self.data.keys()}
        result_data.update({f"other_{k}": [] for k in other.data.keys() if k != on})

        # Parcourir les lignes du premier DataFrame
        for i, value in enumerate(self.data[on]):
            # Si la valeur est aussi dans l'autre DataFrame
            if value in other.data[on]:
                # Ajouter la ligne à result_data
                for k, v in self.data.items():
                    result_data[k].append(v[i])
                for k, v in other.data.items():
                    if k != on:
                        result_data[f"other_{k}"].append(v[other.data[on].index(value)])

        return DataFrame(result_data)