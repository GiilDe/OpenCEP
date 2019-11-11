from typing import List
from PatternQuery import Condition, Operator, PatternQuery


class Node:
    @staticmethod
    def is_event_node() -> bool:
        pass


class ConditionNode:
    """
    represents an inner node in the graph that holds an operator and a condition list and partial results
    """
    def __init__(self, children: List[Node], operator: Operator, conditions: List[Condition]=None, parent: Node=None):
        """
        :param children:
        :param conditions:
        :param operator:
        :param parent:
        """
        if conditions is None:
            self.conditions = []
        self.children = children
        self.conditions = conditions
        self.operator = operator
        self.parent = parent
        self.partial_results_buffer = []

    @staticmethod
    def is_event_node() -> bool:
        return False

    def add_partial_result(self, partial_result: List):
        """
        adds a partial result
        :param partial_result: a list contains all the events included in the partial result
        :return: self
        """
        self.partial_results_buffer.append(partial_result)
        return self

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)


class EventNode(Node):
    """
    represents a leaf node in the graph that holds events
    """
    def __init__(self, event_type, parent: ConditionNode=None, conditions: List[Condition]=None):
        if conditions is None:
            self.conditions = []
        self.event_type = event_type
        self.parent = parent
        self.partial_results_buffer = []
        self.conditions = conditions

    @staticmethod
    def is_event_node() -> bool:
        return True

    def set_parent(self, parent):
        self.parent = parent

    def add_partial_result(self, partial_result):
        """
        adds a partial result
        :param partial_result: an event corresponding to this leaf node
        :return: self
        """
        self.partial_results_buffer.append(partial_result)
        return self

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)


class PatternQueryGraph:
    """
    a graph that represents a pattern query without operator nesting
    """
    def __init__(self, root_node: ConditionNode):
        self.root_node = root_node


