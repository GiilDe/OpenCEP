import processor
import processing_utilities
from graph_based_processing import graph_based_processing_utilities
import data_formats

stock_types = ['AAME', 'AAME', 'MCRS', 'ZHNE']


def condition1(A: processing_utilities.Event, B: processing_utilities.Event) -> bool:
    return A.volumes > B.volumes


if __name__ == "__main__":
    # Naive seq, and test
    condition = processing_utilities.Condition(condition1, [0, 1])
    processor = processor.TimeCalcProcessor(data_formats.metastock7_attributes,
                                            data_formats.metastock7_time_index, data_formats.metastock7_type_index)
    stock_types_with_identifiers = \
        [processing_utilities.EventTypeOrPatternAndIdentifier(type, i) for i, type in enumerate(stock_types)]
    # shuffle order for testing purposes
    temp = stock_types_with_identifiers[2]
    stock_types_with_identifiers[2] = stock_types_with_identifiers[0]
    stock_types_with_identifiers[0] = temp
    seq_event_pattern = processing_utilities.EventPattern(stock_types_with_identifiers,
                                                          processing_utilities.Seq(range(4)))
    seq_pattern_query = processing_utilities.CleanPatternQuery(seq_event_pattern, [condition], 16)
    and_event_pattern = processing_utilities.EventPattern(stock_types_with_identifiers, processing_utilities.And())
    and_pattern_query = processing_utilities.CleanPatternQuery(and_event_pattern, [condition], 16)
    pattern_queries = [seq_pattern_query, and_pattern_query]
    left_deep_initializer = graph_based_processing_utilities.LeftDeepTreeInitializer()
    left_deep_tree_processor = graph_based_processing_utilities.NaiveMultipleTreesGraphBasedProcessing(left_deep_initializer)
    events = open('sorted_NASDAQ_20080201_1.txt', 'r').read()
    time = processor.query(events, pattern_queries, left_deep_tree_processor, processing_utilities.TrivialInputInterface(),
                    [processing_utilities.FileOutputInterface("seq_test_results.txt"),
                     processing_utilities.FileOutputInterface("and_test_results.txt")])
    print(time)


