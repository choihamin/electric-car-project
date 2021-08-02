import os
import urllib.parse as up
import psycopg2, datetime
from flask import Flask, render_template, request, redirect, session, jsonify
from flask_restx import Api, Resource
from mapboxgl.utils import df_to_geojson
import json
import pandas as pd

app=Flask(__name__)
app.config['JSON_AS_ASCII'] = False
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
cur.execute("select * from Station")
data = cur.fetchall()

@app.route('/CheckLogin', methods=['GET', 'POST'])
def CheckLogin():
    id = request.args.get('Id')
    pw = request.args.get('Password')
    print(id, pw)
    cur.execute("select * from customer where customer_id='{}' and password='{}'".format(id, pw))
    data = cur.fetchall()

    if len(data) == 1:
        return jsonify({'result_code': 1})
    else:
        return jsonify({'result_code': 0})

@app.route('/GetMemberInfo', methods=['GET', 'POST'])
def GetMemberInfo():
    id = request.args.get('Id')
    cur.execute("select customer_name, car_model_name, efficiency from Customer natural join CarModel where customer_id='{}'".format(id))
    data = cur.fetchall()
    name = data[0][0]
    car_model = data[0][1]
    efficiency = data[0][2]

    return jsonify({'name': name,
                    'car_model_name': car_model,
                    'efficiency': efficiency})

@app.route('/GetCarInfo', methods=['GET', 'POST'])
def GetChargeResult():
    id = request.args.get('Id')
    cur.execute("select * from ServiceReservation where customer_id='{}".format(id))
    data = cur.fetchall()[-1]
    current_capacity = '몰라'
    seq_reserve = data[0]
    reserve_type = data[6]
    reserve_time = data[3]
    finish_time = data[4]
    min_capacity = data[5]

    return jsonify({
        'current_capacity': current_capacity,
        'seq_reserve': seq_reserve,
        'reserve_type': reserve_type,
        'reserve_time': reserve_time,
        'finish_time': finish_time,
        'min_capacity': min_capacity
    })


@app.route('/SetSignUpInfo', methods=['GET', 'POST'])
def SetSignUpInfo():
    id = request.args.get('Id')
    pw = request.args.get('Password')
    name = request.args.get('Name')
    car_model = request.args.get('Car_model')
    try:
        cur.execute("insert into customer values('{}','{}','{}',{})".format(id, pw, name, int(car_model)))
        connect.commit()
        return jsonify({'result_code': 1})
    except:
        return jsonify({'result_code': 0})

@app.route('/GetCarCompanyInfo', methods=['GET', 'POST'])
def GetCarCompanyInfo():
    cur.execute("select distinct manufacturer from carmodel")
    data = cur.fetchall()
    dict_ = jsonify(manufacturers=[dict(manufacturer=data[i][0]) for i in range(len(data))])
    return dict_



@app.route('/GetCarModelInfo', methods=['GET', 'POST'])
def GetCarModelInfo():
    company = request.args.get('Car_company')
    cur.execute("select car_model_id, car_model_name from CarModel where manufacturer='{}'".format(company))
    data = cur.fetchall()
    dict_ = jsonify(models=[dict(model_id=data[i][0], model_name=data[i][1]) for i in range(len(data))])
    return dict_

@app.route('/GetStationInfo', methods=['GET', 'POST'])
def GetStationInfo():
    cur.execute("select station_id, station_name, slow_charger, fast_charger, dx, dy from Station")
    data = cur.fetchall()
    data = pd.DataFrame(data, columns=['station_id', 'station_name', 'slow_charger', 'fast_charger', 'dx', 'dy'])
    geo_data = df_to_geojson(
        df=data,
        properties=['station_id', 'station_name', 'slow_charger', 'fast_charger'],
        lat='dx',
        lon='dy',
        precision=5,
        filename='station.geojson'
    )
    path = 'station.geojson'
    with open(path) as f:
        data = json.loads(f.read())
        print(1)
    return data




if __name__ == "__main__":
    app.run(Debug=True, host='0.0.0.0')


