from .dummy_dataset import DummyDataset


class PostProcessDataset(DummyDataset):

    def postprocess_batch(self, input_batch, output_batch):
        output_batch['output'][0] *= self._post_process_factor
        return output_batch
