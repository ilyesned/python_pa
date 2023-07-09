import numpy as np
class Series:
    def __init__(self, data):
        def __init__(self, data):
            self.data = np.array(data)

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