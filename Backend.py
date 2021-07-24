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
print(data)

@app.route('/CheckLogin', methods=['GET'])
def CheckLogin():
    id = request.args.get('Id')
    pw = request.args.get('Password')
    print(id, pw)
    cur.execute("select * from customer where customer_id='{}' and password='{}'".format(id, pw))
    data = cur.fechall()
    if len(data) == 1:
        return jsonify({'result_code': 1})
    else:
        return jsonify({'result_code': 0})

@app.route('/GetMemberInfo', methods=['GET'])
def GetMemberInfo():
    id = request.args.get('Id')
    cur.execute("select * from customer car_model_name where customer_id='{}'".format(id))


@app.route('/SetSignUpInfo', methods=['POST'])
def SetSignUpInfo():
    id = request.args.get('Id')
    pw = request.args.get('Password')
    phone = request.args.get('Phone')
    name = request.args.get('Name')
    car_company = request.args.get('Car_company')

    cur.execute("insert into customer values('{}','{}','{}')".format(id, pw, name))
    cur.execute("insert into phone_cus values('{}','{}')".format(id, phone))






if __name__ == "__main__":
    app.run(Debug=True, host='0.0.0.0')


