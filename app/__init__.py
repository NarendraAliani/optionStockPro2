"""
Flask Application Factory
"""
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_migrate import Migrate
from flask_socketio import SocketIO
from flask_wtf.csrf import CSRFProtect
import logging
from logging.handlers import RotatingFileHandler
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.exc import OperationalError
from app.services.angel_api import download_scrip_master, get_scrip_master_status

# Initialize extensions
db = SQLAlchemy()
login_manager = LoginManager()
migrate = Migrate()
socketio = SocketIO()
csrf = CSRFProtect()

def create_app(config_name='default'):
    """Application factory pattern"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    app = Flask(
        __name__,
        static_folder=os.path.join(base_dir, 'static'),
        static_url_path='/static'
    )
    
    # Load configuration
    from app.config import config
    app.config.from_object(config[config_name])
    
    # Add ProxyFix middleware for ngrok support
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    
    # Initialize extensions
    db.init_app(app)
    login_manager.init_app(app)
    migrate.init_app(app, db)
    socketio.init_app(app, cors_allowed_origins="*", async_mode='eventlet')
    csrf.init_app(app)
    
    # Configure login manager
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'
    login_manager.login_message_category = 'info'
    
    # User loader
    from app.models.user import User

    def _is_disconnect_error(error):
        message = str(error).lower()
        disconnect_markers = (
            'lost connection to mysql server during query',
            'server has gone away',
            'connection was forcibly closed by the remote host',
            'broken pipe',
            'connection reset by peer'
        )
        return any(marker in message for marker in disconnect_markers)

    @login_manager.user_loader
    def load_user(user_id):
        try:
            user_id = int(user_id)
        except (TypeError, ValueError):
            return None

        # Retry once if the pooled connection is stale or transiently dropped.
        for attempt in range(2):
            try:
                return db.session.get(User, user_id)
            except OperationalError as exc:
                if not _is_disconnect_error(exc):
                    app.logger.exception('Failed to load user %s', user_id)
                    return None

                app.logger.warning(
                    'Transient DB disconnect while loading user %s (attempt %s/2): %s',
                    user_id, attempt + 1, exc
                )
                try:
                    db.session.rollback()
                except Exception:
                    pass
                db.session.remove()

        return None
    
    # Register blueprints
    from app.routes.auth import auth_bp
    from app.routes.dashboard import dashboard_bp
    from app.routes.scanner import scanner_bp
    from app.routes.settings import settings_bp
    from app.routes.api import api_bp
    
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(dashboard_bp, url_prefix='/dashboard')
    app.register_blueprint(scanner_bp, url_prefix='/scanner')
    app.register_blueprint(settings_bp, url_prefix='/settings')
    app.register_blueprint(api_bp, url_prefix='/api')
    
    # Root route
    from flask import redirect, url_for
    @app.route('/')
    def index():
        return redirect(url_for('auth.login'))
    
    # Configure logging
    if not app.debug:
        if not os.path.exists('logs'):
            os.mkdir('logs')
        file_handler = RotatingFileHandler(
            'logs/app.log',
            maxBytes=10240000,
            backupCount=10
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)
        app.logger.setLevel(logging.INFO)
        app.logger.info('Options Signal Scanner Pro startup')

    _start_scrip_master_scheduler(app)
    
    return app


def _start_scrip_master_scheduler(app):
    enabled = os.getenv('SCRIP_MASTER_REFRESH_ENABLED', 'true').lower() == 'true'
    if not enabled:
        return

    # Avoid double scheduling with reloader
    if app.debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        return

    hour = int(os.getenv('SCRIP_MASTER_REFRESH_HOUR', '5'))
    minute = int(os.getenv('SCRIP_MASTER_REFRESH_MINUTE', '15'))

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=lambda: _refresh_scrip_master(app),
        trigger=CronTrigger(hour=hour, minute=minute),
        id='scrip_master_refresh',
        replace_existing=True
    )

    # Initial fetch if missing
    status = get_scrip_master_status()
    if not status.get('exists'):
        _refresh_scrip_master(app)

    scheduler.start()
    app.extensions['scrip_master_scheduler'] = scheduler


def _refresh_scrip_master(app):
    with app.app_context():
        success, message, _ = download_scrip_master(force=True)
        if success:
            app.logger.info(message)
        else:
            app.logger.warning(message)
