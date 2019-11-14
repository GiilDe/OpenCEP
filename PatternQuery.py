from typing import List, Callable
from enum import Enum


class Operator(Enum):
    SEQ = 1
    AND = 2


class EventPattern:
    """
    This class represents an operator application on some event types.
    for example if A, B, C are event types than this class might represent SEQ(A, B, C)
    the elements in event_types can be event types or OperatorApplication so we can represent recursive operator
    application (such as SEQ(A, B, AND(C, D)))
    """
    def __init__(self, operands: List, operator: Operator):
        """
        :param operands:
        :param operator:
        """
        self.operator = operator
        self.operands = operands


class Condition:
    """
    this class represents a predicate (for example for events A, B: A.x > B.x)
    """
    def __init__(self, condition_apply_function: Callable, event_indices: List[int]):
        """
        :param condition_apply_function: a boolean function that gets the relevant event and applies the condition
        :param event_indices: the indices of the events in the PatternQuery event_types list
            to be checked by this condition. Note that event_indices needs to be ordered in the order arguments should
            be passed to the condition_apply_function!
        """
        self.condition_apply_function = condition_apply_function
        self.event_indices = event_indices

    def check_condition(self, events: List) -> bool:
        """
        :param events: the list of relevant events in the order they need to be called in the condition_apply_function
        :return: true if the condition holds for the relevant events
        """
        return self.condition_apply_function(*events)


class PatternQuery:
    def __init__(self, event_pattern: EventPattern, conditions: List[Condition], time_limit):
        self.event_pattern = event_pattern
        self.conditions = conditions
        self.time_limit = time_limit
