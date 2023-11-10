class MockSocketManager(object):
    def init_app(self, app, extension):
        app.extensions = getattr(app, "extensions", {})
        if extension not in app.extensions:
            app.extensions[extension] = self

    # Just a request sink, don't do anything
    def send_request(self, request):
        pass
