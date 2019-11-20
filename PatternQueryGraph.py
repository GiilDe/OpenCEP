from typing import List, Union
from PatternQuery import Condition, Operator, PatternQuery, _time_limit
from Processor import Event
from itertools import product, chain


class PartialResult:
    """
    just a possible implementation, not finished
    """
    def __init__(self, events: Union[List[Event], Event]):
        self.events = [events] if type(events) == Event else events
        self.start_time = min(self.events, key=lambda event: event.get_time())
        self.end_time = max(self.events, key=lambda event: event.get_time())

    def __getitem__(self, i):
        return self.events[i]

    def __iter__(self):
        return self.events

    @staticmethod
    def init_with_partial_results(partial_results):
        new_events_lists = [partial_result.events for partial_result in partial_results]
        new_events = list(chain(*new_events_lists))
        return PartialResult(new_events)


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

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)

    def set_parent(self, parent):
        self.parent = parent

    def _check_conditions(self, partial_result: Union[PartialResult, Event]) -> bool:
        if partial_result.end_time - partial_result.start_time > _time_limit:
            return False
        return all(condition.check_condition(partial_result) for condition in self.conditions)

    def get_results(self):
        return self.partial_results_buffer


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

    def try_add_partial_result(self, partial_result: PartialResult, diffuser_child: Node):
        """
        :param partial_result:
        :param diffuser_child:
        :return:
        """
        results_from_relevant_children = [child.get_results if child != diffuser_child else [partial_result]
                                          for child in self.children]
        for partial_results in product(*results_from_relevant_children):
            if not self.operator.check_operator(partial_results):
                continue
            new_result = PartialResult.init_with_partial_results(partial_results)
            if self._check_conditions(new_result):
                self.partial_results_buffer.append(new_result)
                if self.parent:
                    self.parent.try_add_partial_result(new_result, self)
        return self


class EventNode(Node):
    """
    represents a leaf node in the graph that holds events
    """
    def __init__(self, event_type, parent: ConditionNode = None, conditions: List[Condition] = None):
        super().__init__(conditions, parent)
        self.event_type = event_type

    def try_add_partial_result(self, event: Event):
        """
        adds a partial result
        :param event: an event corresponding to this leaf node
        :return: self
        """
        partial_result = PartialResult(event)
        if self._check_conditions(partial_result):
            self.partial_results_buffer.append(partial_result)
            self.parent.try_add_partial_result(partial_result, self)
        return self


class PatternQueryGraph:
    """
    a graph that represents a pattern query without operator nesting
    """
    def __init__(self, root_node: ConditionNode, event_nodes: List[EventNode]):
        self.root_node = root_node
        self.event_nodes = event_nodes


