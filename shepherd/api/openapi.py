from apistrap.aiohttp import AioHTTPApistrap

oapi = AioHTTPApistrap()
oapi.title = "Shepherd"
oapi.use_default_error_handlers = False
oapi.ui_url = "/apidocs"
oapi.redoc_url = "/redoc"
