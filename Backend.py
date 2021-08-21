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
from socket import timeout
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote



app = Flask(__name__)
logging = logging.getLogger(__name__)
app.config['JSON_AS_ASCII'] = False
api = Api(app)

up.uses_netloc.append("postgres")
os.environ["DATABASE_URL"] = "postgres://yadctsip:mvZ_FWEhIcFp4PCZMlzUtdZivUkj1IBG@arjuna.db.elephantsql.com/yadctsip"
url = up.urlparse(os.environ["DATABASE_URL"])

connect = None
def conn():
    connect = psycopg2.connect(database=url.path[1:],
                            user=url.username,
                            password=url.password,
                            host=url.hostname,
                            port=url.port)
    return connect


def fee_set():
    connect = conn()
    cur = connect.cursor()
    try:
        try:
            now = datetime.datetime.now()
            if 0 <= int(now.strftime('%M')) < 15:
                date = now.strftime("%Y-%m-%d-%H-00-00")
            elif 15 <= int(now.strftime('%M')) < 30:
                date = now.strftime("%Y-%m-%d-%H-15-00")
            elif 30 <= int(now.strftime('%M')) < 45:
                date = now.strftime("%Y-%m-%d-%H-30-00")
            elif 45 <= int(now.strftime('%M')):
                date = now.strftime("%Y-%m-%d-%H-45-00")
            cur.execute("select * from LpData where lp_time_datetime='{}'".format(date))
            HD_target = cur.fetchall()[-1]
            supp_reserve_pwer = HD_target[1]
        except:
            raise Exception('{}에 해당하는 HourData가 존재하지 않습니다'.format(date))
        try:
            date = now.strftime("%Y-%m-%d-%H-00-00")
            cur.execute("select * from Prophet where lp_time_datetime='{}'".format(date))
            PP_target = cur.fetchall()[-1]
            yhat = PP_target[1]
            yhat_upper = PP_target[2]
            yhat_lower = PP_target[3]
        except:
            raise Exception('{}에 해당하는 Prophet Data가 존재하지 않습니다'.format(date))
        print(PP_target)
        seasonTime = str(int(now.strftime('%m%H')))
        if 0 <= int(now.strftime('%M')) < 15:
            seasonTime += '00'
        elif 15 <= int(now.strftime('%M')) < 30:
            seasonTime += '15'
        elif 30 <= int(now.strftime('%M')) < 45:
            seasonTime += '30'
        elif 45 <= int(now.strftime('%M')):
            seasonTime += '45'
        cur.execute("select fee from SeasonTime natural join LoadFee where season_time_id='{}'".format(seasonTime))
        fee = cur.fetchall()[0]

        if supp_reserve_pwer > yhat_upper:
            expected_fee = fee[0] * 0.8
        elif supp_reserve_pwer < yhat_lower:
            expected_fee = fee[0] * 1.2
        else:
            expected_fee = fee[0] * (100 + ((yhat - supp_reserve_pwer) * 20 / (yhat_upper - yhat))) / 100

        temp = now.strftime("%Y-%m-%d-%H-%M-%S").split('-')

        if temp[0:3] != date.split('-')[0:3]:
            raise Exception('날짜가 서로 매칭되지 않습니다.')

        if 0 <= int(now.strftime('%M')) < 15:
            temp[4] = '00'
            temp[5] = '00'
            time = '-'.join(temp)
        elif 15 <= int(now.strftime('%M')) < 30:
            temp[4] = '15'
            temp[5] = '00'
            time = '-'.join(temp)
        elif 30 <= int(now.strftime('%M')) < 45:
            temp[4] = '30'
            temp[5] = '00'
            time = '-'.join(temp)
        elif 45 <= int(now.strftime('%M')):
            temp[4] = '45'
            temp[5] = '00'
            time = '-'.join(temp)

        cur.execute("select * from FeeInfo")
        data = cur.fetchall()

        if len(data) > 96:
            while len(data) > 96:
                first_idx = data[0][0]
                cur.execute("delete from FeeInfo where lp_time_datetime='{}'".format(first_idx))
                connect.commit()
                cur.execute("select * from FeeInfo")
                data = cur.fetchall()

        sql = "insert into FeeInfo values('{}',{})"
        cur.execute(sql.format(time, expected_fee))
        connect.commit()
        print("Process fee_set is successful")
    except Exception as e:
        print('insert was failed')
        print('Error reason :', e)
    finally:
        if connect is not None:
            connect.close()

def prophet_1hour():
    connect = conn()
    cur = connect.cursor()
    cur.execute("select * from HourData")
    trade_train = cur.fetchall()
    trade_train = pd.DataFrame(data=trade_train, columns=['lp_time datetime', 'supp_reserve_pwr'])
    trade_train['lp_time datetime'] = pd.to_datetime(trade_train['lp_time datetime'], format='%Y-%m-%d-%H-%M-%S')
    prophet_data = trade_train.rename(columns={'lp_time datetime': 'ds', 'supp_reserve_pwr': 'y'})

    m = Prophet(yearly_seasonality=False, weekly_seasonality=True, daily_seasonality=True, growth='logistic',
                changepoint_prior_scale=0.1)
    prophet_data['cap'] = 1000000
    prophet_data['floor'] = 0

    m.fit(prophet_data, iter=250)

    # 144 period = 144시간 = 7일 뒤 데이터까지 분석
    future = m.make_future_dataframe(periods=24, freq='H')
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

    try:
        if len(data) > 500:
            while len(data) > 500:
                first_idx = data[0][0]
                cur.execute("delete from Prophet where lp_time_datetime='{}'".format(first_idx))
                connect.commit()
                cur.execute("select * from Prophet")
                data = cur.fetchall()
        sql = "insert into Prophet values('{}',{},{},{})"
        cur.execute(sql.format(time, seven_day_after_yhat, seven_day_after_yhat_upper, seven_day_after_yhat_lower))
        connect.commit()
        print(time, seven_day_after_yhat, seven_day_after_yhat_upper, seven_day_after_yhat_lower)
        print("prophet_1hour : success")
        return 1
    except:
        print("prophet_1hour : Fail")
        return 0
    finally:
        if connect is not None:
            connect.close()

def return_supp(table):
    connect = conn()
    cur = connect.cursor()
    url = 'https://openapi.kpx.or.kr/openapi/chejusukub5mToday/getChejuSukub5mToday'
    queryParams = '?' + urlencode({quote_plus('ServiceKey'): 'cgPcAXpDDuaSdniUhHGNmo3Crgs6NJL3VmR7sOFJ/4yj3KRs/ywyhijGQFORMeyBVvscFlg4Np/GHieko5d1NQ=='})

    try:
        response = requests.get(url + queryParams).text.encode('utf-8')
    except (HTTPError, URLError) as error:
        logging.error('Data not retrieved because %s\nURL: %s', error, url)
    except timeout:
        logging.error('socket timed out - URL %s', url)
    else:
        logging.info('Access successful.')

    xmlobj = bs4.BeautifulSoup(response, 'lxml-xml')

    # item 다 가져옴
    items = xmlobj.findAll('item')

    # item중 마지막 데이터 = 호출한 시점의 데이터
    last_item = items[-1]

    # datetime = 데이터 시간

    now = datetime.datetime.now()

    temp = now.strftime("%Y-%m-%d-%H-%M-%S").split('-')

    if table == 'LpData':
        if 0 <= int(now.strftime('%M')) < 15:
            temp[4] = '00'
            temp[5] = '00'
        elif 15 <= int(now.strftime('%M')) < 30:
            temp[4] = '15'
            temp[5] = '00'
        elif 30 <= int(now.strftime('%M')) < 45:
            temp[4] = '30'
            temp[5] = '00'
        elif 45 <= int(now.strftime('%M')):
            temp[4] = '45'
            temp[5] = '00'
    elif table == 'HourData':
        temp[4] = '00'
        temp[5] = '00'
    time = '-'.join(temp)
    # suppReservePwr = 공급예비력 = 공급능력 - 현재수요
    suppReservePwr = float(last_item.suppAbility.text) - float(last_item.currPwrTot.text)

    cur.execute("select * from {}".format(table))
    data = cur.fetchall()

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
        print("=====================================================================================")
        sql = "insert into {} values('{}',{})"
        cur.execute(sql.format(table, time, suppReservePwr))
        connect.commit()
        print('return_supp : success')
        return 1
    except:
        print('return_supp : Fail')
        return 0
    finally:
        if connect is not None:
            connect.close()






@app.route('/CheckLogin', methods=['GET', 'POST'])
def CheckLogin():
    connect = conn()
    cur = connect.cursor()
    id = request.args.get('Id')
    pw = request.args.get('Password')
    cur.execute("select * from customer where customer_id='{}' and password='{}'".format(id, pw))
    data = cur.fetchall()
    connect.close()
    if len(data) == 1:
        return jsonify({'result_code': 1})
    else:
        return jsonify({'result_code': 0})


@app.route('/GetHomeInfo', methods=['GET', 'POST'])
def GetHomeInfo():
    connect = conn()
    cur = connect.cursor()

    id = request.args.get('Id')
    cur.execute("select customer_name, car_model_name, battery_capacity, efficiency from Customer natural join CarModel where customer_id='{}'".format(id))
    data = cur.fetchall()
    name = data[0][0]
    car_model = data[0][1]
    battery_capacity = data[0][2] # 차량 배터리용량
    efficiency = data[0][3]       # 연비
    current_capacity = -1

    try:
        cur.execute("select reserve_id, reserve_time, finish_time, station_name, is_paid, reserve_type from ServiceReservation natural join Station where customer_id='{}'".format(id))
        target = cur.fetchall()[-1]
        service_reservation_id = target[0]
        start_time = target[1]
        end_time = target[2]
        station_name = target[3]
        is_paid = target[4]
        reserve_type = target[5]

        return jsonify({'name': name,
                        'car_model_name': car_model,
                        'efficiency': efficiency,
                        'battery_capacity': battery_capacity,
                        'current_capacity': current_capacity,
                        'is_paid': is_paid,
                        'service_reservation_id': service_reservation_id,
                        'start_time': start_time,
                        'finish_time': end_time,
                        'reserve_type': reserve_type,
                        'station_name':station_name})
    except:
        return jsonify({'name': name,
                        'car_model_name': car_model,
                        'efficiency': efficiency,
                        'battery_capacity': battery_capacity,
                        'current_capacity': current_capacity,
                        'is_paid': -1,
                        'service_reservation_id': -1,
                        'start_time': "몰라",
                        'finish_time': "몰라",
                        'reserve_type': -1,
                        'station_name': "몰라",
                        })
    finally:
        if connect is not None:
            connect.close()



@app.route('/GetChargeInfo', methods=['GET', 'POST'])
def GetChargeInfo():
    connect = conn()
    cur = connect.cursor()
    id = request.args.get('Service_reservation_Id')
    try:
        cur.execute("select reserve_type, finish_time, expected_fee, dx, dy from ServiceReservation natural join Station where reserve_id='{}'".format(id))
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
    finally:
        if connect is not None:
            connect.close()

@app.route('/StopCharge', methods=['GET', 'POST'])
def StopCharge():
    connect = conn()
    cur = connect.cursor()
    id = request.args.get('Service_reservation_id')
    now = datetime.datetime.now()
    now = now.strftime('%Y-%m-%d-%H-%M-%S')
    print(now)

    try:
        cur.execute("update ServiceReservation set finish_time='{}' where reserve_id ='{}'".format(now, id))
        connect.commit()
        return jsonify({'result_code': 1})
    except:
        return jsonify({'result_code': 0})
    finally:
        if connect is not None:
            connect.close()

@app.route('/GetChargeHistory', methods=['GET', 'POST'])
def GetChargeHistory():
    connect = conn()
    cur = connect.cursor()
    id = request.args.get('Id')
    try:
        cur.execute("select reserve_time, reserve_type, expected_fee from ServiceReservation where customer_id='{}'".format(id))
        data = cur.fetchall()
        dict_ = jsonify(list_history=[dict(reserve_time=data[i][0], reserve_type=data[i][1], expected_fee=data[i][2]) for i in range(len(data))])
        return dict_
    except:
        return jsonify({'result_code': 0})
    finally:
        if connect is not None:
            connect.close()

@app.route('/GetChargeResult', methods=['GET','POST'])
def GetChargeResult():
    connect = conn()
    cur = connect.cursor()
    id = request.args.get('Service_reservation_id')
    try:
        cur.execute("select expected_fee from ServiceReservation where reserve_id='{}'".format(id))
        target = cur.fetchall()
        return jsonify({'expected_fee': target[0]})
    except:
        return jsonify({'result_code': 0})
    finally:
        if connect is not None:
            connect.close()


@app.route('/SetServicePaid', methods=['GET','POST'])
def SetServicePaid():
    connect = conn()
    cur = connect.cursor()

    id = request.args.get('Service_reservation_id')
    try:
        cur.execute("update ServiceReservation set is_paid = 1 where reserve_id='{}'".format(id))
        connect.commit()
        return jsonify({'result_code': 1})
    except:
        return jsonify({'result_code': 0})
    finally:
        if connect is not None:
            connect.close()

@app.route('/GetFeeInfo', methods=['GET', 'POST'])
def GetFeeInfo():
    connect = conn()
    cur = connect.cursor()

    try:
        cur.execute("select * from FeeInfo")
        data = cur.fetchall()
        now = datetime.datetime.now()
        string = now.strftime('%Y-%m-%d')

        lst = []
        for e in data:
            if string in e[0]:
                lst.append(e)
        dict_ = jsonify(fee_history=[dict(hhmm=':'.join(e[0].split('-')[3:5]), fee=e[1]) for e in lst])
        return dict_
    except:
        return jsonify({'result_code': -1})
    finally:
        if connect is not None:
            connect.close()


@app.route('/SetSignUpInfo', methods=['GET', 'POST'])
def SetSignUpInfo():
    connect = conn()
    cur = connect.cursor()

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
    finally:
        if connect is not None:
            connect.close()

@app.route('/GetCarCompanyInfo', methods=['GET','POST'])
def GetCarCompanyInfo():
    connect = conn()
    cur = connect.cursor()
    try:
        cur.execute("select distinct manufacturer from CarModel")
        data = cur.fetchall()
        dict_ = jsonify(manufacturers=[dict(manufacturer = data[i][0]) for i in range(len(data))])
        return dict_
    except:
        return jsonify({'result_code': 0})
    finally:
        if connect is not None:
            connect.close()


@app.route('/GetCarModelInfo', methods=['GET', 'POST'])
def GetCarModelInfo():
    connect = conn()
    cur = connect.cursor()
    company = request.args.get('Car_company')
    cur.execute("select car_model_id, car_model_name from CarModel where manufacturer='{}'".format(company))
    data = cur.fetchall()
    dict_ = jsonify(models=[dict(model_id=data[i][0], model_name=data[i][1]) for i in range(len(data))])
    connect.close()
    return dict_

@app.route('/GetStationInfo', methods=['GET', 'POST'])
def GetStationInfo():
    connect = conn()
    cur = connect.cursor()
    cur.execute("select station_id, station_name, slow_charger, fast_charger, dx, dy, v2g from Station")
    data = cur.fetchall()
    data = pd.DataFrame(data, columns=['station_id', 'station_name', 'slow_charger', 'fast_charger', 'dx', 'dy', 'v2g'])
    geo_data = df_to_geojson(
        df=data,
        properties=['station_id', 'station_name', 'slow_charger', 'fast_charger', 'v2g'],
        lat='dx',
        lon='dy',
        precision=5,
        filename='station.geojson'
    )
    path = 'station.geojson'
    with open(path) as f:
        data = json.loads(f.read())
    connect.close()

    return data

@app.route('/SetReserveInfo', methods=['GET', 'POST'])
def SetReserveInfo():
    connect = conn()
    cur = connect.cursor()

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
        now = datetime.datetime.now()
        if 0 <= int(now.strftime('%M')) < 15:
            date = now.strftime("%Y-%m-%d-%H-00-00")
        elif 15 <= int(now.strftime('%M')) < 30:
            date = now.strftime("%Y-%m-%d-%H-15-00")
        elif 30 <= int(now.strftime('%M')) < 45:
            date = now.strftime("%Y-%m-%d-%H-30-00")
        elif 45 <= int(now.strftime('%M')):
            date = now.strftime("%Y-%m-%d-%H-45-00")


        cur.execute("select fee from FeeInfo where lp_time_datetime='{}'".format(date))
        data = cur.fetchall()
        if data == []:
            raise Exception('There is no FeeInfo in {}'.format(date))
        else:
            expected_fee = data[0][0]

    except Exception as e:
        expected_fee = -1
        print(e)
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
    finally:
        if connect is not None:
            connect.close()




sched = BackgroundScheduler()
sched.add_job(return_supp, trigger='cron', args=['LpData'], hour='*', minute='*/15', second='59', id="test_1")
sched.add_job(return_supp, trigger='cron', args=['HourData'], hour='*', minute='5', second='0', id="test_2", misfire_grace_time=300)
sched.add_job(prophet_1hour, trigger='cron', hour='*', minute='12', second='30', id="test_3", misfire_grace_time=240)
sched.add_job(fee_set, trigger='cron', hour='*', minute='4', second='0', id='test4')
sched.add_job(fee_set, trigger='cron', hour='*', minute='19', second='0', id='test5')
sched.add_job(fee_set, trigger='cron', hour='*', minute='34', second='0', id='test6')
sched.add_job(fee_set, trigger='cron', hour='*', minute='49', second='0', id='test7')

sched.start()

if __name__ == "__main__":
    app.run(host='0.0.0.0')



