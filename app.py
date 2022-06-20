from flask import Flask, render_template, request
import pyodbc
import textwrap
from azure.storage.blob import BlobServiceClient, ContentSettings
import redis
import timeit
import hashlib
import pickle
server = 'kiran98.database.windows.net'
database = 'Assignment12'
username = 'kiran1998'
password = 'Omsrn@062466'
driver = '{ODBC Driver 18 for SQL Server}'

app = Flask(__name__)

sqlconnection = pyodbc.connect('DRIVER='+driver+';SERVER='+server +
                               ';PORT=1433;DATABASE='+database+';UID='+username+';PWD=' + password)
cursor = sqlconnection.cursor()


connection_string = "DefaultEndpointsProtocol=https;AccountName=assigns1;AccountKey=FjfZ2UGw8oZx9cDaz2PJYqCtqMlyAXVuGt5Dq8TTcN1InDs8yUrgc8PIu48Xq8A7zku1SP0G+1hN+AStHhKtsQ==;EndpointSuffix=core.windows.net"
img_container = "uniqcontain"

r = redis.StrictRedis(host='kvk.redis.cache.windows.net', port=6380, db=0,
                      password='TfTx4gtlLBMTLNoN7DyJ2Pfcba90wkH9rAzCaBmSNlQ=', ssl=True)
result = r.ping()
print("Ping returned : " + str(result))


@app.route('/', methods=["POST", "GET"])
def home():
    return render_template('index.html')


@app.route('/createtable', methods=['GET', 'POST'])
def createtable():
    if request.method == 'POST':
        start_time = timeit.default_timer()
        cursor.execute('CREATE TABLE TEST(Time nvarchar(50), Latitude float, Longitude float, Depth float, Mag float NULL DEFAULT 0.0, Magtype nvarchar(50) NULL DEFAULT 0.0, Nst int NULL DEFAULT 0.0, Gap float NULL DEFAULT 0.0, Dmin  float NULL DEFAULT 0.0, Rms float, Net nvarchar(50), ID nvarchar(50), Updated nvarchar(50), Place nvarchar(MAX), Type nvarchar(50), HorizontalError float NULL DEFAULT 0.0, DepthError float, MagError float NULL DEFAULT 0.0, MagNst int NULL DEFAULT 0.0, Status nvarchar(50), LocationSource nvarchar(50), MagSource nvarchar(50))')
        cursor.execute(
            'CREATE INDEX indexes on TEST(Time, Latitude, Longitude, Mag, Magtype)')
        sqlconnection.commit()
        elapsedtime = timeit.default_timer() - start_time
        print("Time taken to create a table and add indexes is :", elapsedtime)
    return render_template('index.html', createtimeelapsed=elapsedtime)


@app.route('/randomqueries', methods=['GET', 'POST'])
def randomqueries():
    if request.method == 'POST':
        numberexec = request.form.get('randomqueries')
        starttime = timeit.default_timer()
        for data in range(int(numberexec)):
            cursor.execute(
                "select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from [dbo].[eq]")
            cursor.execute("INSERT INTO [dbo].[eq] VALUES('2022-03-08T00:14:13.990Z', 65.56016739, -196.0518964, 42.64500046, 3.54, 'ml', 55, 894, 0.03837, 0.899999995, 'hv', 'hv72898902', '2021-06-20T00:27:46.240Z', '27 km SSE of Fern Forest, Hawaii', 'earthquake', 0.89, 0.479999989,3.22,26, 'automatic', 'hv', 'hv')")
            sqlconnection.commit()
        time_elapsed = timeit.default_timer() - starttime
    return render_template("index.html", randomqueriestimeelapsed=time_elapsed)


@app.route('/randomqueriesredis', methods=['GET', 'POST'])
def randomqueriesredis():
    if request.method == 'POST':
        numberexec = request.form.get('randomqueriesredis')
        sqlquery = "select Time, Latitude, Longitude, Depth, Mag, Magtype, Place, LocationSource from [dbo].[eq] where Mag > 3 and Mag < 7"
        hashvalue = hashlib.sha224(sqlquery.encode('utf-8')).hexdigest()
        redis_key = "redis_cache:" + hashvalue
        starttime = timeit.default_timer()
        for data in range(int(numberexec)):
            if not (r.get(redis_key)):
                cursor.execute(sqlquery)
                fetchlist = list(cursor.fetchall())
                r.set(redis_key, pickle.dumps(list(fetchlist)))
                r.expire(redis_key, 35)
            else:
                print("caching redis")
        time_elapsed = timeit.default_timer() - starttime
    return render_template("index.html", redisrandomelapsed=time_elapsed)


if __name__ == '__main__':  # only run if you run this file, not if you import other main.py file
    #os.environ['PYTHONPATH'] = os.getcwd()
    app.run(debug=True)
