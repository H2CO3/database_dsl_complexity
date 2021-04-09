import json
from collections import namedtuple

def json_approx_equal(v1, v2):
    if type(v1) != type(v2):
        return False

    # Approximate equality for floating-point numbers.
    # Only round to 7 decimal places because MongoDB
    # doesn't seem to directly support computing Julian
    # days, so the naive approach of dividing by 24 * 3600
    # doesn't yield exactly the same values, the
    # error being approximately 10 parts per billion.
    if isinstance(v1, float):
        return abs(v1 - v2) <= 1e-7

    if isinstance(v1, list):
        if len(v1) != len(v2):
            return False

        for x, y in zip(v1, v2):
            if not json_approx_equal(x, y):
                return False

        return True

    if isinstance(v1, dict):
        if len(v1) != len(v2):
            return False

        for kx, vx in v1.items():
            try:
                vy = v2[kx]

                if not json_approx_equal(vx, vy):
                    return False

            except KeyError:
                return False

        return True

    return v1 == v2

class TestCase(namedtuple('TestCase', ['params', 'expected'])):
    def violation_indexes(self, actual):
        len_actual = len(actual)
        len_expected = len(self.expected)

        if len_actual != len_expected:
            start, end = min(len_actual, len_expected), max(len_actual, len_expected)
            return list(range(start, end))

        return [
            i for i, (val_act, val_exp)
            in enumerate(zip(actual, self.expected))
            if not json_approx_equal(val_act, val_exp)
        ]

class TestCases:
    def __init__(self, stream):
        self.test_cases = {
            name: TestCase(tc['params'], tc['results'])
            for name, tc in json.load(stream).items()
        }

    def __getitem__(self, name):
        return self.test_cases[name]

    def __len__(self):
        return len(self.test_cases)
