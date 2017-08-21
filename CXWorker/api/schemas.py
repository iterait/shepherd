from marshmallow import Schema, fields


class StartJobRequestSchema(Schema):
    id = fields.String()
    container_id = fields.String()
    source_url = fields.Url()
    result_url = fields.Url()


class InterruptJobRequestSchema(Schema):
    container_id = fields.String()


class ModelSchema(Schema):
    name = fields.String()
    version = fields.String()


class ReconfigureRequestSchema(Schema):
    model = fields.Nested(ModelSchema)
    container_id = fields.String()
    slave_container_ids = fields.List(fields.String(), required=False)
