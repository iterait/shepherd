import emloop as el


class DummyModel(el.AbstractModel):

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

    def run(self, batch: el.Batch, train: bool, stream):
        batch['output'] = [batch['key'][0]*self._factor]
        return batch

    @property
    def restore_fallback(self):
        return None
