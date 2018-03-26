from marshmallow import Schema, fields


class StartJobRequestSchema(Schema):
    id = fields.String(required=True)
    container_id = fields.String(required=True)
    source_url = fields.String(required=True)
    result_url = fields.String(required=True)
    status_url = fields.Url(missing=None, allow_none=True, required=True)
    refresh_model = fields.Boolean(missing=False)


class InterruptJobRequestSchema(Schema):
    container_id = fields.String(required=True)


class ModelSchema(Schema):
    name = fields.String(required=True)
    version = fields.String(required=True)


class ReconfigureRequestSchema(Schema):
    model = fields.Nested(ModelSchema, required=True)
    container_id = fields.String(required=True)
    slave_container_ids = fields.List(fields.String())
