"""
Authentication Routes
"""
from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, current_user
from app import db
from app.models.user import User
from app.utils.captcha import generate_captcha, verify_captcha
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length
from flask_wtf import FlaskForm
from app.utils.validators import PasswordComplexity, UsernameValidator

auth_bp = Blueprint('auth', __name__)


class RegistrationForm(FlaskForm):
    """User registration form"""
    username = StringField('Username', validators=[
        DataRequired(),
        UsernameValidator()
    ])
    email = StringField('Email', validators=[
        DataRequired(),
        Email()
    ])
    password = PasswordField('Password', validators=[
        DataRequired(),
        PasswordComplexity()
    ])
    confirm_password = PasswordField('Confirm Password', validators=[
        DataRequired(),
        EqualTo('password', message='Passwords must match')
    ])
    captcha = StringField('Captcha', validators=[DataRequired()])
    submit = SubmitField('Register')


class LoginForm(FlaskForm):
    """User login form"""
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    captcha = StringField('Captcha', validators=[DataRequired()])
    submit = SubmitField('Login')


@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    """User registration"""
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.index'))
    
    form = RegistrationForm()
    
    if form.validate_on_submit():
        # Verify captcha
        success, error = verify_captcha(form.captcha.data)
        if not success:
            flash(error, 'danger')
            captcha_question = generate_captcha()
            return render_template('auth/register.html', form=form, captcha_question=captcha_question)
        
        # Check if username or email already exists
        if User.query.filter_by(username=form.username.data).first():
            flash('Username already exists.', 'danger')
            return render_template('auth/register.html', form=form)
        
        if User.query.filter_by(email=form.email.data).first():
            flash('Email already registered.', 'danger')
            return render_template('auth/register.html', form=form)
        
        # Create new user
        user = User(
            username=form.username.data,
            email=form.email.data
        )
        user.set_password(form.password.data)
        
        db.session.add(user)
        db.session.commit()
        
        flash('Registration successful! Please log in.', 'success')
        return redirect(url_for('auth.login'))
    
    captcha_question = generate_captcha()
    return render_template('auth/register.html', form=form, captcha_question=captcha_question)


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """User login"""
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.index'))
    
    form = LoginForm()
    
    if form.validate_on_submit():
        # Verify captcha
        success, error = verify_captcha(form.captcha.data)
        if not success:
            flash(error, 'danger')
            captcha_question = generate_captcha()
            return render_template('auth/login.html', form=form, captcha_question=captcha_question)
        
        # Find user
        user = User.query.filter_by(username=form.username.data).first()
        
        if user and user.check_password(form.password.data):
            if not user.is_active:
                flash('Account is disabled.', 'danger')
                return render_template('auth/login.html', form=form)
            
            login_user(user, remember=True)
            flash(f'Welcome back, {user.username}!', 'success')
            
            # Redirect to next page or dashboard
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('dashboard.index'))
        else:
            flash('Invalid username or password.', 'danger')
    
    captcha_question = generate_captcha()
    return render_template('auth/login.html', form=form, captcha_question=captcha_question)


@auth_bp.route('/logout')
def logout():
    """User logout"""
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('auth.login'))
