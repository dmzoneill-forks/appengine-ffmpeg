from flask import Flask
monitor_app = Flask(__name__)

@monitor_app.route('/_ah/health')
def health():
    return 'healthy', 200


@monitor_app.route('/')
def index():
    return health()

if __name__ == '__main__':
    monitor_app.run('0.0.0.0', 8080)
