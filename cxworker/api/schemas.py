from marshmallow import Schema, fields


class StartJobRequestSchema(Schema):
    id = fields.String()
    container_id = fields.String()
    source_url = fields.String()
    result_url = fields.String()
    status_url = fields.Url(missing=None, allow_none=True)
    refresh_model = fields.Boolean(missing=False)


class InterruptJobRequestSchema(Schema):
    container_id = fields.String()


class ModelSchema(Schema):
    name = fields.String()
    version = fields.String()


class ReconfigureRequestSchema(Schema):
    model = fields.Nested(ModelSchema)
    container_id = fields.String()
    slave_container_ids = fields.List(fields.String(), required=False)
