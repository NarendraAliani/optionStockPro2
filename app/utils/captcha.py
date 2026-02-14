"""
Simple captcha (math question) validation
"""
import random
from flask import session

CAPTCHA_SESSION_KEY = 'simple_captcha_answer'


def generate_captcha():
    """
    Generate a simple math captcha and store the answer in session.

    Returns:
        str: captcha question to render in the form
    """
    a = random.randint(1, 9)
    b = random.randint(1, 9)
    op = random.choice(['+', '-'])
    if op == '-':
        # avoid negative answers
        if b > a:
            a, b = b, a
    answer = a + b if op == '+' else a - b
    session[CAPTCHA_SESSION_KEY] = str(answer)
    return f'{a} {op} {b} = ?'


def verify_captcha(user_answer):
    """
    Verify the user-provided captcha answer against session.

    Args:
        user_answer: User input from the form

    Returns:
        tuple: (success: bool, error: str)
    """
    expected = session.get(CAPTCHA_SESSION_KEY)
    # Always clear to prevent replay
    session.pop(CAPTCHA_SESSION_KEY, None)

    if not expected:
        return False, 'Captcha expired. Please try again.'

    if str(user_answer).strip() == expected:
        return True, None

    return False, 'Incorrect captcha answer.'
