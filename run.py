"""
Options Signal Scanner Pro - Application Entry Point
"""
from app import create_app, socketio
import os

app = create_app()

if __name__ == '__main__':
    # Run with SocketIO for WebSocket support
    debug_flag = os.getenv('FLASK_DEBUG', '1') in ('1', 'true', 'True')
    use_reloader = os.getenv('FLASK_USE_RELOADER', '0') in ('1', 'true', 'True')
    socketio.run(
        app,
        host='0.0.0.0',
        port=5000,
        debug=debug_flag,
        use_reloader=use_reloader,
        log_output=True
    )
