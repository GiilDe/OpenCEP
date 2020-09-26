import processor
import processing_utilities
from graph_based_processing import graph_based_processing_utilities
import data_formats

stock_types = ['AAME', 'INTC', 'MCRS', 'ZHNE']


def condition1(A: processing_utilities.Event, B: processing_utilities.Event) -> bool:
    return A.volumes > B.volumes


if __name__ == "__main__":
    # Naive seq, and test
    condition = processing_utilities.Condition(condition1, [0, 1])
    processor = processor.Processor("sorted_NASDAQ_20080201_1.txt", data_formats.metastock7_attributes,
                                    data_formats.metastock7_time_index, data_formats.metastock7_type_index)
    or_event_pattern = processing_utilities.EventPattern(
        [processing_utilities.EventTypeOrPatternAndIdentifier(stock_types[0], 0),
         processing_utilities.EventTypeOrPatternAndIdentifier(stock_types[1], 1)],
        processing_utilities.Or()
    )
    stock_types_with_identifiers = \
        [processing_utilities.EventTypeOrPatternAndIdentifier(type, i) for i, type in enumerate(stock_types)]
    # shuffle order for testing purposes
    temp = stock_types_with_identifiers[2]
    stock_types_with_identifiers[2] = stock_types_with_identifiers[0]
    stock_types_with_identifiers[0] = temp
    seq_event_pattern = processing_utilities.EventPattern(
        [processing_utilities.EventTypeOrPatternAndIdentifier(or_event_pattern, 2),
         processing_utilities.EventTypeOrPatternAndIdentifier(stock_types[2], 3),
         processing_utilities.EventTypeOrPatternAndIdentifier(stock_types[3], 4)],
        processing_utilities.Seq([2, 3, 4])
    )
    seq_pattern_query = processing_utilities.CleanPatternQuery(seq_event_pattern, [condition], 16)
    pattern_queries = [seq_pattern_query]
    left_deep_initializer = graph_based_processing_utilities.LeftDeepTreeInitializer()
    left_deep_tree_processor = graph_based_processing_utilities.NaiveMultipleTreesGraphBasedProcessing(
        left_deep_initializer)
    processor.query(pattern_queries, left_deep_tree_processor, processing_utilities.TrivialInputInterface(),
                    [processing_utilities.FileOutputInterface("seq_test_results.txt")])
