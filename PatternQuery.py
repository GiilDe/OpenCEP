from typing import List, Callable
from enum import Enum


class Operator(Enum):
    SEQ = 1
    AND = 2


class OperatorApplication:
    """
    this class represents an operator application on some event types.
    for example if A, B, C are event types than this class might represent SEQ(A, B, C)
    the elements in event_types can be event types or OperatorApplication so we can represent recursive operator application
    (such as SEQ(A, B, AND(C,D)))
    """
    def __init__(self, event_types: List, operator: Operator):
        self.operator = operator
        self.event_types = event_types


class Condition:
    """
    this class represents a predicate (for example for events A, B: A.x > B.x)
    """
    def __init__(self, condition_apply_function: Callable, event_types_indices):
        """
        Parameters
        ----------
        condition_apply_function: a boolean function that gets the relevant event and applies the condition
        event_types_indices: the indices of the events in the PatternQuery event_types list
            to be checked by this condition
        """
        self.condition_apply_function = condition_apply_function
        self.event_types_indices = event_types_indices

    def check_condition(self, events: List) -> bool:
        relevant_events = [events[i] for i in self.event_types_indices]
        return self.condition_apply_function(*relevant_events)


class PatternQuery:
    def __init__(self, operators: OperatorApplication, conditions: List[Condition], time_limit):
        self.event_types =
