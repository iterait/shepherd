import cxflow as cx


class DummyModel(cx.AbstractModel):

    def __init__(self, factor: int=2, **kwargs):
        super().__init__(**kwargs)
        self._factor = factor
        pass

    def input_names(self):
        pass

    def output_names(self):
        pass

    def save(self, name_suffix: str):
        pass

    def run(self, batch: cx.Batch, train: bool, stream):
        batch['output'] = [batch['key'][0]*self._factor]
        return batch

    @property
    def restore_fallback(self):
        return None
