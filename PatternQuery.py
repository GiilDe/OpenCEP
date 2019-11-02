from typing import List
from enum import Enum


class Operator(Enum):
    SEQ = 1
    AND = 2


class PatternQuery:
    def __init__(self, event_types: List, conditions: List, operator: Operator, time_limit):
        self.operator = operator

