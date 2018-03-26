import json
import cxflow as cx


class DummyDataset(cx.BaseDataset):

    def _configure_dataset(self, **kwargs):
        pass

    def predict_stream(self, payload):
        yield json.loads(payload)
