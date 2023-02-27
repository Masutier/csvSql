from datetime import datetime
from flask import Flask, render_template as render, request, url_for, flash, redirect

from flask_sqlalchemy import SQLAlchemy

from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired

app = Flask(__name__)


# Home
@app.route('/')
def home():

    objBuy = Buy.query.filter_by(status='1')
    objBuycount = 0
    for obj in objBuy:
        objBuycount += 1

    objHist = Buy.query.filter_by(status='0')
    objHistcount = 0
    for obj in objHist:
        objHistcount += 1

    objDoit = Todo.query.all()
    objDoitount = 0
    for obj in objDoit:
        objDoitount += 1

    return render("home.html", objBuycount=objBuycount, objDoitount=objDoitount, objHistcount=objHistcount)
