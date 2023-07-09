import numpy as np
class Series:
    class Iloc:
        def __init__(self, series):
            self.series = series

        def __getitem__(self, index):
            return self.series.data[index]

    def __init__(self, data):
        self.data = np.array(data)
        self.iloc = self.Iloc(self)

    def max(self):
        return np.max(self.data)

    def min(self):
        return np.min(self.data)

    def mean(self):
        return np.mean(self.data)

    def std(self):
        return np.std(self.data)

    def count(self):
        return len(self.data)