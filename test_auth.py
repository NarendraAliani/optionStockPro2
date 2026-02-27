"""
Test script to register a user and test authentication
"""
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import os
import sys
import time

BASE_URL = os.getenv("BASE_URL", "http://localhost:5000")

# Create a session to maintain cookies
session = requests.Session()


def wait_for_server(base_url, timeout_seconds=20):
    """Wait for the Flask server to be reachable before running tests."""
    deadline = time.time() + timeout_seconds
    url = f"{base_url}/auth/login"
    while time.time() < deadline:
        try:
            response = session.get(url, timeout=3)
            if response.status_code == 200:
                return True
        except requests.RequestException:
            time.sleep(1)
    return False

print("=" * 60)
print("TESTING OPTIONS SIGNAL SCANNER PRO")
print("=" * 60)

if not wait_for_server(BASE_URL):
    print("\nERROR: App server is not reachable.")
    print(f"Start the app first, then retry: python run.py  (URL: {BASE_URL})")
    sys.exit(1)

# Test 1: Get registration page
print("\n[TEST 1] Getting registration page...")
r = session.get(f"{BASE_URL}/auth/register")
print(f"Status: {r.status_code}")
print(f"Page contains registration form: {'register' in r.text.lower() and 'email' in r.text.lower()}")

# Extract CSRF token and captcha
soup = BeautifulSoup(r.text, 'html.parser')
csrf_token = soup.find('input', {'name': 'csrf_token'})['value']
print(f"CSRF Token extracted: {csrf_token[:20]}...")
captcha_text = soup.find(id='captchaQuestion').get_text(strip=True)
match = re.search(r'(\d+)\s*([+-])\s*(\d+)', captcha_text)
if not match:
    raise RuntimeError(f'Captcha not found or invalid: {captcha_text}')
left, op, right = match.groups()
captcha_answer = str(int(left) + int(right)) if op == '+' else str(int(left) - int(right))

# Test 2: Register a new user
print("\n[TEST 2] Registering new user...")
suffix = datetime.utcnow().strftime('%Y%m%d%H%M%S')
username = f'testuser_{suffix}'
email = f'test_{suffix}@example.com'
register_data = {
    'csrf_token': csrf_token,
    'username': username,
    'email': email,
    'password': 'Test@1234',
    'confirm_password': 'Test@1234',
    'captcha': captcha_answer,
    'submit': 'Register'
}

r = session.post(f"{BASE_URL}/auth/register", data=register_data)
print(f"Status: {r.status_code}")
print(f"Redirected to: {r.url}")
if 'Registration successful' in r.text or r.url.endswith('/auth/login'):
    print("OK: Registration successful")
else:
    print("Registration response:", r.text[:500])

# Test 3: Login with created user
print("\n[TEST 3] Logging in...")
r = session.get(f"{BASE_URL}/auth/login")
soup = BeautifulSoup(r.text, 'html.parser')
csrf_token = soup.find('input', {'name': 'csrf_token'})['value']
captcha_text = soup.find(id='captchaQuestion').get_text(strip=True)
match = re.search(r'(\d+)\s*([+-])\s*(\d+)', captcha_text)
if not match:
    raise RuntimeError(f'Captcha not found or invalid: {captcha_text}')
left, op, right = match.groups()
captcha_answer = str(int(left) + int(right)) if op == '+' else str(int(left) - int(right))

login_data = {
    'csrf_token': csrf_token,
    'username': username,
    'password': 'Test@1234',
    'captcha': captcha_answer,
    'submit': 'Login'
}

r = session.post(f"{BASE_URL}/auth/login", data=login_data)
print(f"Status: {r.status_code}")
print(f"Redirected to: {r.url}")
if 'dashboard' in r.url:
    print("OK: Login successful. Redirected to dashboard")
else:
    print("Login response:", r.text[:500])

# Test 4: Access dashboard
print("\n[TEST 4] Accessing dashboard...")
r = session.get(f"{BASE_URL}/dashboard/")
print(f"Status: {r.status_code}")
print(f"Dashboard loaded: {'Dashboard' in r.text and 'Scanner Configuration' in r.text}")

# Test 5: Access API credentials page
print("\n[TEST 5] Accessing API credentials page...")
r = session.get(f"{BASE_URL}/settings/api-credentials")
print(f"Status: {r.status_code}")
print(f"API credentials page loaded: {'API Credentials' in r.text and 'Angel One' in r.text}")

# Test 6: Logout
print("\n[TEST 6] Logging out...")
r = session.get(f"{BASE_URL}/auth/logout")
print(f"Status: {r.status_code}")
print(f"Redirected to: {r.url}")
print(f"Logged out: {r.url.endswith('/auth/login')}")

print("\n" + "=" * 60)
print("TESTING COMPLETE!")
print("=" * 60)
