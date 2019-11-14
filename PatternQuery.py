from typing import List, Callable
from enum import Enum


class Operator:
    def __init__(self, unitary: bool):
        self.unitary = unitary


class Operators(Enum):
    SEQ = Operator(False)
    AND = Operator(False)
    OR = Operator(False)
    class KLEIN(Operator):
        def __init__(self, occurence_num):
            super().__init__(False)
            self.occurence_num = occurence_num


# class OperatorApplication:
#     """
#     this class represents an operator application on some event types.
#     for example if A, B, C are event types than this class might represent SEQ(A, B, C)
#     the elements in event_types can be event types or OperatorApplication so we can represent recursive operator
#     application (such as SEQ(A, B, AND(C,D)))
#     """
#     def __init__(self, event_types: List, operator: Operator):
#         self.operator = operator
#         self.event_types = event_types
#
#
# class Condition:
#     """
#     this class represents a predicate (for example for events A, B: A.x > B.x)
#     """
#     def __init__(self, condition_apply_function: Callable, event_indices: List[int]):
#         """
#         Parameters
#         ----------
#         condition_apply_function: a boolean function that gets the relevant event and applies the condition
#         event_indices: the indices of the events in the PatternQuery event_types list
#             to be checked by this condition. Note that event_indices needs to be ordered in the order arguments should
#             be passed to the condition_apply_function!
#         """
#         self.condition_apply_function = condition_apply_function
#         self.event_indices = event_indices
#
#
#     def check_condition(self, events: List) -> bool:
#         """
#         Parameters
#         ----------
#         events: the list of all events (or at least a list that contains the relevant indices)
#
#         Returns
#         -------
#         Returns True if the condition holds for the relevant events
#         """
#         relevant_events = [events[i] for i in self.event_indices]
#         return self.condition_apply_function(*relevant_events)
#
#
# class PatternQuery:
#     def __init__(self, operators: OperatorApplication, conditions: List[Condition], time_limit):
#         self.event_types =


if __name__ == "__main__":
    x = Operators.AND
    y = Operators.KLEIN(3)
    z = Operators.OR
    if x == Operators.KLEIN:
        print("bad")
    if x == Operators.AND:
        print("good")
