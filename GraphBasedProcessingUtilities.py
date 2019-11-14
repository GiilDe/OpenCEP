from PatternQuery import PatternQuery
from PatternQueryGraph import PatternQueryGraph, EventNode, ConditionNode
from Processor import EvaluationModel


class GraphInitializer:
    def get_graph(self, pattern_query: PatternQuery) -> PatternQueryGraph:
        pass


class LeftDeepTreeInitializer(GraphInitializer):
    """
    This class receives a PatternQuery and generates a trivial left deep tree representing the pattern query.
    This class cannot implement operator nesting.
    """
    def __init__(self, pattern_query: PatternQuery):
        """
        :param pattern_query: a PatternQuery
        """
        self.pattern_query = pattern_query

    def get_graph(self, pattern_query: PatternQuery) -> PatternQueryGraph:
        """
        assumes no operator nesting
        :param pattern_query:
        :return: PatternQueryGraph that is a left deep tree representing the pattern query
        """
        operator = pattern_query.event_pattern.operator
        events = pattern_query.event_pattern.operands
        conditions = pattern_query.conditions
        parent = EventNode(events[0])
        inner_nodes = []
        leaves = [parent]
        if len(events) != 1:
            for event_type in events[1:]:
                right_child = EventNode(event_type)
                leaves.append(right_child)
                parent = ConditionNode([parent, right_child], operator)
                inner_nodes.append(parent)

        root_node = parent
        for condition in conditions:
            if len(condition.event_indices) == 1:
                leaves[condition.event_indices[0]].add_condition(condition)
            else:
                max_index = max(condition.event_indices)
                inner_nodes[max_index - 1].add_condition(condition)

        pattern_query_graph = PatternQueryGraph(root_node)
        return pattern_query_graph


class GraphBasedProcessing(EvaluationModel):
    """
    This class receives a graph that represents the pattern the user is looking for.
    It iterates the events one by one and tries to build the graph that it received.
    It saves its partial graphs explicitly in memory.
    """
    def __init__(self, graph_initializer: GraphInitializer):
        """
        :param graph_initializer: receives PatternQuery and returns a graph that represents the pattern query
        :param pattern_query: PatternQuery
        """
        self.graph_initializer = graph_initializer

    def set_pattern_query(self, pattern_query: PatternQuery):
        self.graph = self.graph_initializer.get_graph(pattern_query)

    def handle_event(self, event):