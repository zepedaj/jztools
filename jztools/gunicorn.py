import gunicorn.app.base
from multiprocessing import cpu_count


class GunicornServer(gunicorn.app.base.BaseApplication):
    _dflt_options = {"bind": "0.0.0.0:8060", "workers": cpu_count() * 2 + 1}

    def __init__(self, app, options={}):
        self.options = dict(self._dflt_options)
        self.options.update(options)
        self.application = app
        super().__init__()

    def load_config(self):
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application
