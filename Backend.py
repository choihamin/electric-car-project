import os
import urllib.parse as up
import psycopg2, datetime
from flask import Flask, render_template, request, redirect, session, jsonify
from flask_restx import Api, Resource
from mapboxgl.utils import df_to_geojson
import json
import uuid
import logging, time
from apscheduler.schedulers.background import BackgroundScheduler
import requests, bs4
import urllib
import pandas as pd
from fbprophet import Prophet

app=Flask(__name__)
logging = logging.getLogger(__name__)
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


def prophet_1hour():
    cur.execute("select * from HourData")
    trade_train = cur.fetchall()
    trade_train = pd.DataFrame(data=trade_train, columns=['lp_time datetime', 'supp_reserve_pwr'])
    trade_train['lp_time datetime'] = pd.to_datetime(trade_train['lp_time datetime'], format='%Y-%m-%d-%H-%M-%S')
    prophet_data = trade_train.rename(columns={'lp_time datetime': 'ds', 'supp_reserve_pwr': 'y'})

    m = Prophet(yearly_seasonality=False, weekly_seasonality=True, daily_seasonality=True, growth='logistic',
                changepoint_prior_scale=0.1)
    prophet_data['cap'] = 1000000
    prophet_data['floor'] = 0

    m.fit(prophet_data)

    # 144 period = 144시간 = 7일 뒤 데이터까지 분석
    future = m.make_future_dataframe(periods=144, freq='H')
    future['cap'] = 1000000
    future['floor'] = 0
    forecast = m.predict(future)

    time = seven_day_after_forecast = forecast['ds'][len(forecast['ds']) - 1]
    time = time.strftime("%Y-%m-%d-%H-%M-%S")
    seven_day_after_yhat = forecast['yhat'][len(forecast['yhat']) - 1]
    seven_day_after_yhat_upper = forecast['yhat_upper'][len(forecast['yhat_upper']) - 1]
    seven_day_after_yhat_lower = forecast['yhat_lower'][len(forecast['yhat_lower']) - 1]

    cur.execute("select * from Prophet")
    data = cur.fetchall()
    print("ready? 1hour")

    try:
        first_idx = data[0][0]
        if len(data) > 500:
            cur.execute("delete from Prophet where lp_time_datetime='{}'".format(first_idx))
            connect.commit()
        sql = "insert into Prophet values('{}',{},{},{})"
        cur.execute(sql.format(time, seven_day_after_yhat, seven_day_after_yhat_upper, seven_day_after_yhat_lower))
        connect.commit()
        print("prophet_1hour : success")
        return 1
    except:
        print("prophet_1hour : Fail")
        return 0

def return_supp(table):
    url = 'https://openapi.kpx.or.kr/openapi/chejusukub5mToday/getChejuSukub5mToday'
    key = 'cgPcAXpDDuaSdniUhHGNmo3Crgs6NJL3VmR7sOFJ/4yj3KRs/ywyhijGQFORMeyBVvscFlg4Np/GHieko5d1NQ=='
    req = urllib.request.urlopen('{}?ServiceKey={}'.format(url, key,))
    xmlobj = bs4.BeautifulSoup(req, 'lxml-xml')

    # item 다 가져옴
    items = xmlobj.findAll('item')

    # item중 마지막 데이터 = 호출한 시점의 데이터
    last_item = items[-1]

    # datetime = 데이터 시간
    datetime = last_item.baseDatetime.text
    datetime = datetime[0:4] + "-" + datetime[4:6] + "-" + datetime[6:8] + "-" + datetime[8:10] + "-" + datetime[10:12] + "-" + datetime[12:]
    # suppReservePwr = 공급예비력 = 공급능력 - 현재수요
    suppReservePwr = float(last_item.suppAbility.text) - float(last_item.currPwrTot.text)

    cur.execute("select * from {}".format(table))
    data = cur.fetchall()
    print(data)

    if table == 'HourData':
        length = 96
        print('ready? Hour_Data_15min?')
    elif table == 'LpData':
        length = 720
        print('ready? Lp_Data_15min?')
    try:
        if len(data) > length:
            while len(data) > length:
                first_idx = data[0][0]
                cur.execute("delete from {} where lp_time_datetime='{}'".format(table, first_idx))
                connect.commit()
                cur.execute("select * from {}".format(table))
                data = cur.fetchall()
        print(data)
        print("=====================================================================================")
        sql = "insert into {} values('{}',{})"
        cur.execute(sql.format(table, datetime, suppReservePwr))
        connect.commit()
        print('return_supp : success')
        return 1
    except:
        print('return_supp : Fail')
        return 0





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

@app.route('/GetHomeInfo', methods=['GET', 'POST'])
def GetHomeInfo():
    id = request.args.get('Id')
    cur.execute("select customer_name, car_model_name, battery_capacity, efficiency from Customer natural join CarModel where customer_id='{}'".format(id))
    data = cur.fetchall()
    name = data[0][0]
    car_model = data[0][1]
    battery_capacity = data[0][2] # 차량 배터리용량
    efficiency = data[0][3]       # 연비
    current_capacity = "몰라"

    try:
        cur.execute("select reserve_id, reserve_time, finish_time, station_name, is_paid from ServiceReservation natural join Station where customer_id='{}'".format(id))
        target = cur.fetchall()[-1]
        service_reservation_id = target[0]
        start_time = target[1]
        end_time = target[2]
        station_name = target[3]
        is_paid = target[4]

        return jsonify({'name': name,
                        'car_model_name': car_model,
                        'efficiency': efficiency,
                        'battery_capacity': battery_capacity,
                        'current_capacity': current_capacity,
                        'is_paid': is_paid,
                        'service_reservation_id': service_reservation_id,
                        'start_time': start_time,
                        'finish_time': end_time,
                        'station_name':station_name})
    except:
        return jsonify({'name': name,
                        'car_model_name': car_model,
                        'efficiency': efficiency,
                        'battery_capacity': battery_capacity,
                        'current_capacity': current_capacity,
                        'is_paid': -1,
                        'service_reservation_id': -1,
                        'start_time': -1,
                        'finish_time': -1,
                        'station_name': -1
                        })



@app.route('/GetChargeInfo', methods=['GET', 'POST'])
def GetChargeInfo():
    id = request.args.get('Id')
    try:
        cur.execute("select reserve_type, finish_time, expected_fee, dx, dy from ServiceReservation natural join Station where reserve_id='{}".format(id))
        data = cur.fetchall()[-1]
        reserve_type = data[0]
        finish_time = data[1]
        expected_fee = data[2]
        dx = data[3]
        dy = data[4]

        return jsonify({
            'reserve_type': reserve_type,
            'finish_time': finish_time,
            'expected_fee': expected_fee,
            'dx': dx,
            'dy': dy
        })
    except:
        return jsonify({'result_code': 0})

@app.route('/StopCharge', methods=['GET', 'POST'])
def StopCharge():
    id = request.args.get('Service_reservation_id')
    try:
        cur.execute("update ServiceReservation set finish_time=now() where reserve_id = '{}'".format(id))
        connect.commit()
        return jsonify({'result_code': 1})

    except:
        return jsonify({'result_code': 0})

@app.route('/GetChargeHistory', methods=['GET', 'POST'])
def GetChargeHistory():
    id = request.args.get('Id')
    try:
        cur.execute("select reserve_time, reserve_type, expected_fee from ServiceReservation from customer_id='{}'".format(id))
        data = cur.fetchall()
        dict_ = jsonify(list_history=[dict(reserve_time=data[i][0], reserve_type=data[i][1], expected_fee=data[i][2]) for i in range(len(data))])
        return dict_
    except:
        return jsonify({'result_code': 0})

@app.route('/GetChargeResult', methods=['GET','POST'])
def GetChargeResult():
    id = request.args.get('Service_reservation_id')
    try:
        cur.execute("select expected_fee from ServiceReservation where reserve_id='{}'".format(id))
        target = cur.fetchall()
        return jsonify({'expected_fee': target[0]})
    except:
        return jsonify({'result_code': 0})


@app.route('/SetServicePaid', methods=['GET','POST'])
def SetServicePaid():
    id = request.args.get('Service_reservation_id')
    try:
        cur.execute("update ServiceReservation set is_paid = 1 where id='{}'".format(id))
        connect.commit()
        return jsonify({'result_code': 1})
    except:
        return jsonify({'result_code': 0})


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

@app.route('/GetCarCompanyInfo', methods=['GET','POST'])
def GetCarCompanyInfo():
    try:
        cur.execute("select distinct manufacturer from CarModel")
        data = cur.fetchall()
        dict_ = jsonify(manufacturers=[dict(manufacturer = data[i][0]) for i in range(len(data))])
        return dict_
    except:
        return jsonify({'result_code': 0})


@app.route('/GetCarModelInfo', methods=['GET', 'POST'])
def GetCarModelInfo():
    company = request.args.get('Car_company')
    cur.execute("select car_model_id, car_model_name from CarModel where manufacturer='{}'".format(company))
    data = cur.fetchall()
    dict_ = jsonify(models=[dict(model_id=data[i][0], model_name=data[i][1]) for i in range(len(data))])
    return dict_

@app.route('/GetStationInfo', methods=['GET', 'POST'])
def GetStationInfo():
    cur.execute("select station_id, station_name, slow_charger, fast_charger, dx, dy, v2g from Station")
    data = cur.fetchall()
    data = pd.DataFrame(data, columns=['station_id', 'station_name', 'slow_charger', 'fast_charger', 'dx', 'dy', 'v2g'])
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

@app.route('/SetReserveInfo', methods=['GET', 'POST'])
def SetReserveInfo():
    # param get
    id = request.args.get('Id')
    station_id = request.args.get('StationId')
    reserve_type = request.args.get('ReserveType')
    reserve_time = request.args.get('StartTime')
    finish_time = request.args.get('FinishTime')
    minimum_cap = request.args.get('MinimumCap')
    current_cap = request.args.get('CurrentCap') # 나중에 없앨 부분


    # calculate expected_fee
    try:
        cur.execute("select * from HourData")
        HD_target = cur.fetchall()[-1]
        date = HD_target[0]
        supp_reserve_pwer = HD_target[1]

        cur.execute("select * from Prophet where lp_time_datetime='{}'".format(date))
        PP_target = cur.fetchall()[-1]
        yhat = PP_target[1]
        yhat_upper = PP_target[2]
        yhat_lower = PP_target[3]

        now = datetime.datetime.now()
        seasonTime = str(int(now.strftime('%m%H')))
        cur.execute("select fee from SeasonTime natural join LoadFee where season_time_id=''".format(seasonTime))
        fee = cur.fetchall()[0]

        if supp_reserve_pwer > yhat_upper:
            expected_fee = fee * 0.8
        elif supp_reserve_pwer < yhat_lower:
            expected_fee = fee * 1.2
        else:
            expected_fee = fee * (((100 + yhat - supp_reserve_pwer)*20/(yhat_upper - yhat))/100)

    except:
        expected_fee = -1
    # insert tuple
    try:
        reserve_id = str(uuid.uuid4())
        sql = "insert into ServiceReservation values('{}','{}','{}','{}','{}',{},{},{},{})"
        cur.execute(sql.format(reserve_id, id, station_id, reserve_time, finish_time, minimum_cap, reserve_type, expected_fee, 0))
        connect.commit()
        return jsonify({'service_reservation_id': reserve_id,
                        'expected_fee': expected_fee})
    except:
        return jsonify({'result_code': 0})



sched = BackgroundScheduler()
sched.start()
sched.add_job(return_supp, 'cron', args=['HourData'], minute='12', second='30', id="test_1")
sched.add_job(return_supp, 'cron', args=['LpData'], minute='5', second='0', id="test_2")
sched.add_job(return_supp, 'cron', args=['LpData'], minute='20', second='0', id="test_2")
sched.add_job(return_supp, 'cron', args=['LpData'], minute='35', second='0', id="test_2")
sched.add_job(return_supp, 'cron', args=['LpData'], minute='50', second='0', id="test_2")
sched.add_job(prophet_1hour, 'cron', minute='10', second='0', id="test_3")

if __name__ == "__main__":
    app.run(host='192.168.66.72')


