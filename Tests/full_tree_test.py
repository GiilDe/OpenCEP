
import Processor
import ProcessingUtilities
from GraphBasedProcessing import GraphBasedProcessingUtilities
import data_formats

if __name__ == "__main__":
    processor = Processor.Processor("sorted_NASDAQ_20080201_1.txt", data_formats.metastock7_attributes,
                                    data_formats.metastock7_time_index, data_formats.metastock7_type_index)
    full_deep_initializer = GraphBasedProcessingUtilities.TestingTree()
    full_tree_processor = GraphBasedProcessingUtilities.NaiveMultipleTreesGraphBasedProcessing(full_deep_initializer)
    processor.query([ProcessingUtilities.CleanPatternQuery(None, [], 50)], full_tree_processor,
                    ProcessingUtilities.TrivialInputInterface(),
                    [ProcessingUtilities.FileOutputInterface("full_tree_test_results.txt")])
