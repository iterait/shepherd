import json
import cxflow as cx


class DummyDataset(cx.BaseDataset):

    def _configure_dataset(self, post_process_factor: int=1, **kwargs):
        self._post_process_factor = post_process_factor

    def predict_stream(self, payload):
        yield payload

    def production_stream(self, payload):
        for b in self.predict_stream(payload):
            yield b
