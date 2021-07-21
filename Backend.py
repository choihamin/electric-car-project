import os
import urllib.parse as up
import psycopg2, datetime
from flask import Flask, render_template, request, redirect, session, jsonify
from flask_restx import Api, Resource


app=Flask(__name__)
api = Api(app)

up.uses_netloc.append("postgres")
os.environ["DATABASE_URL"] = "postgres://yadctsip:mvZ_FWEhIcFp4PCZMlzUtdZivUkj1IBG@arjuna.db.elephantsql.com/yadctsip"
url = up.urlparse(os.environ["DATABASE_URL"])
connect = psycopg2.connect(database=url.path[1:],
                        user=url.username,
                        password=url.password,
                        host=url.hostname,
                        port=url.port)
cur = connect.cursor()
cur.execute("select * from customer where customer_id='jason4284' and password='123456789a!' and Did='2017170843'")
data = cur.fetchall()

@app.route('/CheckLogin')
def CheckLogin():
    id = request.args.get('Id')
    pw = request.args.get('Password')
    Did = request.args.get('DeviceId')
    print(id, pw, Did)
    try:
        cur.execute("select * from customer where customer_id='{}' and password='{}' and Did='{}'".format(id, pw, Did))
        return jsonify({'result_code': 1})
    except:
        return jsonify({'result_code': 0})

if __name__ == "__main__":
    app.run(Debug=True, host='0.0.0.0')


