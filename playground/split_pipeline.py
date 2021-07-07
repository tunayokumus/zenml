from playground.base_pipeline import BasePipeline
from playground.split_step import SplitStep


class SplitPipeline(BasePipeline):
    def __init__(self,
                 split_map):
        super(SplitPipeline, self).__init__()

        self.split_step = SplitStep(split_map=split_map)

    def connect(self, datasource):
        split_step = self.split_step(datasource)