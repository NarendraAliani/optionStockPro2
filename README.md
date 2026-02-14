# Options Signal Scanner Pro

A Python-based web application for scanning NIFTY stock options in real-time and backtesting strategies using historical data from Angel One SmartAPI.

## Features

- **Live Mode**: Real-time option scanning with configurable refresh intervals
- **Backtest Mode**: Historical data analysis with signal history
- **User Authentication**: Secure login with built-in captcha protection
- **API Credentials Management**: Encrypted storage of Angel One API credentials
- **Configurable Scanner**: Customize stock selection, strike range, price multiplier, and timeframe
- **Modern UI**: Responsive dashboard with light/dark theme support
- **Real-time Updates**: WebSocket-based live signal notifications
- **Data Tables**: Paginated, sortable, and filterable signal display

## Technology Stack

- **Backend**: Flask 3.0, SQLAlchemy, Flask-SocketIO
- **Database**: MySQL 8.x
- **API**: Angel One SmartAPI Python SDK
- **Frontend**: HTML5, CSS3, JavaScript, Bootstrap 5, DataTables.js, Chart.js
- **Security**: bcrypt, Fernet encryption, built-in captcha

## Installation

### Prerequisites

- Python 3.10 or higher
- MySQL 8.x (via XAMPP or standalone)
- Angel One trading account with API access

### Setup Steps

1. **Clone or navigate to project directory**:
   ```bash
   cd C:\xampp\htdocs\optionSignalPro
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   ```

3. **Activate virtual environment**:
   ```bash
   # Windows
   venv\Scripts\activate
   
   # Linux/Mac
   source venv/bin/activate
   ```

4. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

5. **Configure environment variables**:
   - Copy `.env.example` to `.env`
   - Update the following values:
     ```
     SECRET_KEY=your-secret-key-here
     ENCRYPTION_KEY=your-encryption-key-here
     DB_NAME=option_signal_pro
     DB_USER=root
     DB_PASSWORD=your-mysql-password
     ```
   - Optional (recommended for live/backtest):
     ```
     ANGEL_SCRIP_MASTER_PATH=data/OpenAPIScripMaster.json
     ```

6. **Create database**:
   ```bash
   # Login to MySQL
   mysql -u root -p
   
   # Create database
   CREATE DATABASE option_signal_pro;
   exit;
   ```

7. **Initialize database migrations**:
   ```bash
   flask db init
   flask db migrate -m "Initial migration"
   flask db upgrade
   ```

8. **Run the application**:
   ```bash
   python run.py
   ```

9. **Access the application**:
   - Open browser and navigate to: `http://localhost:5000`

## Usage

### First-time Setup

1. **Register**: Create a new user account
2. **Login**: Sign in with your credentials
3. **Configure API**: Go to Settings → API Credentials and enter your Angel One API details
4. **Start Scanning**: Navigate to Dashboard and configure your scanner parameters

### Scanner Configuration

- **Stock Selection**: Choose individual stocks or scan all NIFTY stocks
- **Strike Range**: Number of strikes above/below current price (default: 5)
- **Price Multiplier**: Signal threshold (default: 2x price movement)
- **Timeframe**: Candle interval for comparison (1min, 5min, 15min, etc.)
- **Refresh Interval**: Auto-refresh timing for live mode (default: 5 minutes)

### Live Mode

1. Configure scanner parameters
2. Click "Start Live Scan"
3. View real-time signals as they're detected
4. Export signals to CSV/Excel for analysis

### Backtest Mode

1. Select date range
2. Configure scanner parameters
3. Click "Run Backtest"
4. View historical signals with timestamps
5. Analyze performance metrics

## Project Structure

```
optionSignalPro/
├── app/                    # Application package
│   ├── models/            # Database models
│   ├── routes/            # Route blueprints
│   ├── services/          # Business logic
│   ├── utils/             # Utility functions
│   └── templates/         # HTML templates
├── static/                # Static files (CSS, JS, images)
├── migrations/            # Database migrations
├── logs/                  # Application logs
├── requirements.txt       # Python dependencies
├── run.py                # Application entry point
└── README.md             # This file
```

## Security

- Passwords hashed with bcrypt (cost factor: 12)
- API credentials encrypted with AES-256 (Fernet)
- CSRF protection on all forms
- Built-in math captcha on login/registration
- Secure session cookies (HttpOnly, Secure, SameSite)

## Future Enhancements

- Multi-user role-based access
- Advanced analytics and strategy builder
- Telegram/Email alerts
- Portfolio tracking
- Paper trading integration
- Machine learning for signal prediction

## License

Private project - All rights reserved

## Support

For issues or questions, please contact the development team.
