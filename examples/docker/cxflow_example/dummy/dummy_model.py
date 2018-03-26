import cxflow as cx


class DummyModel(cx.AbstractModel):

    def __init__(self, **kwargs):
        pass

    def input_names(self):
        pass

    def output_names(self):
        pass

    def save(self, name_suffix: str):
        pass

    def run(self, batch: cx.Batch, train: bool, stream):
        batch['output'] = [999]
        return batch

    @property
    def restore_fallback(self):
        return None
