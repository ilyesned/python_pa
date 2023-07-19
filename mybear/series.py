import numpy as np

class Series:
    class Iloc:
        def __init__(self, series):
            self.series = series

        def __getitem__(self, index):
            return self.series.data[index]

    def __init__(self, data, name=None):
        self.data = np.array(data)
        self.name = name
        self.iloc = self.Iloc(self)

    def __eq__(self, other):
        if not isinstance(other, Series):
            return NotImplemented
        return np.array_equal(self.data, other.data) and self.name == other.name

    def __repr__(self):
        return f"Series({list(self.data)}, name={self.name})"

    def max(self):
        return np.max(self.data)

    def min(self):
        return np.min(self.data)

    def mean(self):
        return np.mean(self.data)

    def std(self):
        return np.std(self.data)

    def count(self):
        data = np.array(self.data, dtype=float)
        return np.sum(~np.isnan(data))