
import processor
import processing_utilities
from graph_based_processing import graph_based_processing_utilities
import data_formats

if __name__ == "__main__":
    processor = processor.Processor("sorted_NASDAQ_20080201_1.txt", data_formats.metastock7_attributes,
                                    data_formats.metastock7_time_index, data_formats.metastock7_type_index)
    full_deep_initializer = graph_based_processing_utilities.TestingTree()
    full_tree_processor = graph_based_processing_utilities.NaiveMultipleTreesGraphBasedProcessing(full_deep_initializer)
    processor.query([processing_utilities.CleanPatternQuery(None, [], 50)], full_tree_processor,
                    processing_utilities.TrivialInputInterface(),
                    [processing_utilities.FileOutputInterface("full_tree_test_results.txt")])
