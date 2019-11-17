from typing import List, Union
from PatternQuery import Condition, Operator, PatternQuery
import heapq
from Processor import Event


class PartialResultIterator:
    def __init__(self, partial_result):
        self._partial_result = partial_result

    def __next__(self):
        index = 0
        length = len(self._partial_result.events)
        if index < length:
            result = self._partial_result.events[index]
            index += 1
            return result
        # End of Iteration
        raise StopIteration


class PartialResult:
    """
    just a possible implementation, not finished
    """
    def __init__(self, events: List[Event]):
        self.events = events
        self.start_time = events[0].time
        self.end_time = events[-1].time

    def __init__(self, partial_results: List):
        events = heapq.merge(*partial_results)

    def __iter__(self):
        return PartialResultIterator(self)


class Node:
    """
    abstract class, predecessor of ConditionNode and EventNode
    """
    def __init__(self, conditions: List[Condition] = None, parent=None):
        if conditions is None:
            self.conditions = []
        self.conditions = conditions
        self.parent = parent
        self.partial_results_buffer = []

    @staticmethod
    def is_event_node() -> bool:
        pass

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)

    def set_parent(self, parent):
        self.parent = parent

    def apply_conditions(self, events: Union[List[Event], Event]):
        for condition in self.conditions:
            if not condition.check_condition(events):
                return False
        return True


class ConditionNode(Node):
    """
    represents an inner node in the graph that holds an operator and a condition list and partial results
    """
    def __init__(self, children: List[Node], operator: Operator, conditions: List[Condition] = None, parent=None):
        """
        :param children:
        :param conditions:
        :param operator:
        :param parent:
        """
        super().__init__(conditions, parent)
        self.children = children
        self.operator = operator

    @staticmethod
    def is_event_node() -> bool:
        return False

    def add_partial_result(self, partial_result: List[Event]):
        """
        adds a partial result
        :param partial_result: a list contains all the events included in the partial result
        :return: self
        """
        self.partial_results_buffer.append(partial_result)
        return self


class EventNode(Node):
    """
    represents a leaf node in the graph that holds events
    """
    def __init__(self, event_type, parent: ConditionNode = None, conditions: List[Condition] = None):
        super().__init__(conditions, parent)
        self.event_type = event_type

    @staticmethod
    def is_event_node() -> bool:
        return True

    def add_partial_result(self, partial_result: Event):
        """
        adds a partial result
        :param partial_result: an event corresponding to this leaf node
        :return: self
        """
        self.partial_results_buffer.append(partial_result)
        return self


class PatternQueryGraph:
    """
    a graph that represents a pattern query without operator nesting
    """
    def __init__(self, root_node: ConditionNode, event_nodes: List[EventNode]):
        self.root_node = root_node
        self.event_nodes = event_nodes


