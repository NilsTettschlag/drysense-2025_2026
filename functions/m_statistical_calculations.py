import numpy as np

def logistic_decay(x: float, L: float, k: float, x0: float, C: float) -> float:
    return L / (1 + np.exp(k*(x-x0))) + C


def exponential_decay(x: float, A: float, k: float, C: float) -> float:
    return 1/A * np.exp(-k * x) + C