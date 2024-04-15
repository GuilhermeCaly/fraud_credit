## FRADU CREDIT

## NOTEBOOK ESTÁ DIPONÍVEL NO GIHUB:
### https://github.com/GuilhermeCaly/fraud_credit/blob/main/fraud_credit.ipynb
##

DEVE SER EXECULTADO NO GOOGLECOLAB NOTEBOOKS OU CASADO REALIZAR A INTALAÇÃO DAS SEGUINTES BIBLIOTECAS CASO SEA UTILIZADO EM OUTRO AMBIENTE :

google.colab <p>
pandas <p>
numpy <p>
datetime <p>
pymysql <p>
google.colab <p>

##

O JOB TEM INTEGRAÇÃO DIRETA COM O GOOGLE DRIVE, PARA REALIZAR A LEITURA E EDIÇÃO, ESTÁ COM UM SCHEDULE NO GCP  1 VEZ POR DIA AS 01:00 AM.


##

#### PROCESSO DE ETL
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

### CRIAÇÃO DO AMBRIENTE E ARMAZENAMENTO DAS VARIAVEIS DE AMBIENTE:
``` python
host = userdata.get('host')
user = userdata.get('user')
password = userdata.get('password')
database = userdata.get('database')
drive.mount('/content/drive')
file_path = userdata.get('file_path')
dt_source_lc = pd.read_csv(file_path)
df_fraud =  dt_source_lc
```

##
### LIMPEZA E TRATAMENTO:
``` python
df_fraud.dropna(inplace=True)
df_fraud = df_fraud[(df_fraud['location_region'] != '0') & (df_fraud['amount'] != "none") & (df_fraud['risk_score'] != "none")]
df_fraud = df_fraud[(df_fraud['amount'] != "none") & (df_fraud['risk_score'] != "none")]
df_fraud['timestamp'] = pd.to_datetime(df_fraud['timestamp'], unit='s')
```
##
### TIPAGEM DOS DADOS:
``` python

df_fraud['sending_address'] =  df_fraud['sending_address'].astype(str)
df_fraud['risk_score'] =  df_fraud['risk_score'].astype(float)
df_fraud['location_region'] =  df_fraud['location_region'].astype(str)
df_fraud['receiving_address'] =  df_fraud['receiving_address'].astype(str)
df_fraud['amount'] =  df_fraud['amount'].astype(float)
df_fraud['transaction_type'] =  df_fraud['transaction_type'].astype(str)
df_fraud['ip_prefix'] =  df_fraud['ip_prefix'].astype(str)
df_fraud['login_frequency'] =  df_fraud['login_frequency'].astype(int)
df_fraud['session_duration'] =  df_fraud['session_duration'].astype(int)
df_fraud['purchase_pattern'] =  df_fraud['purchase_pattern'].astype(str)
df_fraud['age_group'] =  df_fraud['age_group'].astype(str)
df_fraud['anomaly'] =  df_fraud['anomaly'].astype(str)
```


##

# PRIMEIRO CASE:
``` python
mean_risk_score = df_fraud.groupby('location_region')['risk_score'].mean()
result_table_1 = mean_risk_score.sort_values(ascending=False).reset_index()
display(result_table_1)
```

## RESULTADO:

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Risk Scores by Location Region</title>
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 30px auto;
        }
        th, td {
            border: 0.5px solid black;
            padding: 6px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
</head>
<body>
    <h2>Risk Scores by Location Region</h2>
    <table>
        <thead>
            <tr>
                <th>Location Region</th>
                <th>Risk Score</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>North America</td>
                <td>45.154834</td>
            </tr>
            <tr>
                <td>South America</td>
                <td>45.139408</td>
            </tr>
            <tr>
                <td>Asia</td>
                <td>44.994572</td>
            </tr>
            <tr>
                <td>Africa</td>
                <td>44.902219</td>
            </tr>
            <tr>
                <td>Europe</td>
                <td>44.598708</td>
            </tr>
        </tbody>
    </table>
</body>
</html>


# SEGUNDO CASE:

``` python
sales_data = df_fraud[df_fraud['transaction_type'] == 'sale']
latest_transactions = sales_data.loc[sales_data.groupby('receiving_address')['timestamp'].idxmax()]
result_table_2 = latest_transactions.nlargest(3, 'amount')[['receiving_address', 'amount', 'timestamp']]
result_table_2
```
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Transaction Details</title>
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px auto;
        }
        th, td {
            border: 0.5px solid black;
            padding: 8px;
            text-align: center;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
</head>
<body>
    <h2>Transaction Details</h2>
    <table>
        <thead>
            <tr>
                <th>Receiving Address</th>
                <th>Amount</th>
                <th>Timestamp</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>0x841342e50c508ec4ffdef9b5208719c1dbed7968</td>
                <td>76568.0</td>
                <td>2024-01-02 03:53:01</td>
            </tr>
            <tr>
                <td>0xe8aacdea4f2d7658e711de611bad8e3b5d6b2c7b</td>
                <td>76563.0</td>
                <td>2024-01-01 02:49:56</td>
            </tr>
            <tr>
                <td>0x231dd8e2959e878a59a26ebdbf6f7d122403f350</td>
                <td>76559.0</td>
                <td>2024-01-02 06:31:09</td>
            </tr>
        </tbody>
    </table>
</body>
</html>

# LOG DETALHADO:

### AQUI É POSSÍVEL VERIFICAR OS REGISTROS QUE CONTEM ERRO:

#### O CAMPO "error_message" INFORMA QUAL O  ERRO OCORREU NO REGISTRO
```python
log_error = dt_source_lc.copy()
log_error = dt_source_lc[(dt_source_lc['location_region'] == '0') |
                         (dt_source_lc['amount'] == "none") |
                         (dt_source_lc['risk_score'] == "none")]
                         log_error['error_message'] = ''
log_error.loc[log_error['location_region'] == '0', 'error_message'] = 'REGION DOES NOT EXISTS'
log_error.loc[log_error['amount'] == "none", 'error_message'] = 'AMOUNT IS NONE'
log_error.loc[log_error['risk_score'] == "none", 'error_message'] = 'RISK SCORE IS NONE'
```
##

### SAIDA TABALA log_error :
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Transaction Details</title>
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px auto;
        }
        th, td {
            border: 1px solid black;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
</head>
<body>
    <h2>Transaction Details</h2>
    <table>
        <thead>
            <tr>
                <th>Timestamp</th>
                <th>Sending Address</th>
                <th>Receiving Address</th>
                <th>Amount</th>
                <th>Transaction Type</th>
                <th>Location Region</th>
                <th>IP Prefix</th>
                <th>Login Frequency</th>
                <th>Session Duration</th>
                <th>Purchase Pattern</th>
                <th>Age Group</th>
                <th>Risk Score</th>
                <th>Anomaly</th>
                <th>Error Message</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>1590044893</td>
                <td>0xfcf9a3467d3d93688fb17b4ebb6b681b1fa29f94</td>
                <td>0x6d4f7db757a1bd6bb43029ee13c037651ca20c74</td>
                <td>65319.0</td>
                <td>purchase</td>
                <td>North America</td>
                <td>172.160</td>
                <td>4</td>
                <td>2</td>
                <td>6</td>
                <td>focused</td>
                <td>none</td>
                <td>low_risk</td>
                <td>RISK SCORE IS NONE</td>
                <td></td>
            </tr>
            <tr>
                <td>1661550384</td>
                <td>0xd2cff98e8e707049db92500414fec6f0bb5c895c</td>
                <td>0x2e0925b922fed01f6a85d213ae2718f54b8ca305</td>
                <td>67174.0</td>
                <td>purchase</td>
                <td>South America</td>
                <td>192.000</td>
                <td>2</td>
                <td>3</td>
                <td>8</td>
                <td>random</td>
                <td>none</td>
                <td>low_risk</td>
                <td>RISK SCORE IS NONE</td>
                <td></td>
            </tr>
            <tr>
                <td>1670045880</td>
                <td>0xddbe1291b454f9b8699220f1e8724aa641ef4b42</td>
                <td>0x51c0d24ccc5d9b0dd793052cb2d41efde2568056</td>
                <td>15462.0</td>
                <td>sale</td>
                <td>Africa</td>
                <td>172.000</td>
                <td>6</td>
                <td>4</td>
                <td>14</td>
                <td>high_value</td>
                <td>none</td>
                <td>low_risk</td>
                <td>RISK SCORE IS NONE</td>
                <td></td>
            </tr>
        </tbody>
    </table>
</body>
</html>



##

### CRIAÇÃO DA TABELA DATAQUALITY
```python
quantidade_erros_log = len(log_error)
erros_region = log_error.loc[log_error['error_message'] == 'REGION DOES NOT EXISTS']
erros_region_all = len(erros_region)
erros_amount = log_error.loc[log_error['error_message'] == 'AMOUNT IS NONE']
erros_amount = len(erros_amount)
erros_risk_score = log_error.loc[log_error['error_message'] == 'RISK SCORE IS NONE']
erros_risk_score = len(erros_risk_score)
quantidade_total_registros = len(dt_source_lc)
percentual_conformidade_log = 100 * (quantidade_total_registros - quantidade_erros_log) / quantidade_total_registros
last_insert_date = datetime.now()

data_quality= {
    'qtd_erros_Log': [quantidade_erros_log],
    'qtd_erros_region__not_exits': [erros_region_all],
    'qtd__erros_amount_none': [erros_amount],
    'qtd_erros_risk_none': [erros_risk_score],
    'qtd_total_registros': [quantidade_total_registros],
    'percent_conf_log': [percentual_conformidade_log],
    'Dt_ultimo_inset': [last_insert_date]
}
data_quality = pd.DataFrame(data_quality)

```
### EXEMPLO

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Error Details</title>
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px auto;
        }
        th, td {
            border: 0.5px solid black;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
</head>
<body>
    <h2>Error Details</h2>
    <table>
        <thead>
            <tr>
                <th>Qtd Erros Log</th>
                <th>Qtd Erros Region Not Exists</th>
                <th>Qtd Erros Amount None</th>
                <th>Qtd Erros Risk None</th>
                <th>Qtd Total Registros</th>
                <th>Percent Conf Log</th>
                <th>Data Último Inset</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>149406</td>
                <td>49456</td>
                <td>49724</td>
                <td>50226</td>
                <td>9291894</td>
                <td>98.392082</td>
                <td>2024-04-15 12:03:13.151155</td>
            </tr>
        </tbody>
    </table>
</body>
</html>

##




# CRIA CONEXÃO COM BANCO MYSQL VIA BIBLIOTECA pymysql

``` python
conn = pymysql.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
```
##
### INICIA A CONEXÃO:
 ``` python
conn.open
cursor = conn.cursor()
 ```

 ##

 ### POPULA AS TABELAS 

 ```python

for index, row in data_quality.iterrows():
    insert_tbl_dt_qlt = "INSERT INTO DB_FRAUD.DT_QTLT (qtd_erros_log, qtd_erros_region__not_exits, qtd__erros_amount_none, qtd_erros_risk_none, qtd_total_registros, percent_conf_log, Dt_ultimo_inset) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_quality_values = (row['qtd_erros_Log'], row['qtd_erros_region__not_exits'], row['qtd__erros_amount_none'], row['qtd_erros_risk_none'], row['qtd_total_registros'], row['percent_conf_log'], row['Dt_ultimo_inset'])
    cursor.execute(insert_tbl_dt_qlt, data_quality_values)
    conn.commit()

    for index , row in log_error.iterrows():
    insert_logerror = "INSERT INTO DB_FRAUD.DT_LOGERROR(`timestamp` ,`sending_address`  ,`receiving_address`    ,`amount`   ,`transaction_type` ,`location_region`  ,`ip_prefix`    ,`login_frequency`  ,`session_duration` ,`purchase_pattern` ,`age_group`    ,`risk_score`   ,`anomaly`  ,`error_message`)  VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s) "
    log_error_insert = (row['timestamp'],row['sending_address'],row['receiving_address'],row['amount'],row['transaction_type'],row['location_region'],row['ip_prefix'],row['login_frequency'],row['session_duration'],row['purchase_pattern'],row['age_group'],row['risk_score'],row['anomaly'],row['error_message'],)
    cursor.execute(insert_logerror, log_error_insert)
    conn.commit() 
 ```

 ```python
for index , row in result_table_1.iterrows():
    insert_result_table_1 = "INSERT INTO DB_FRAUD.RSLT_I(location_region,risk_score)  VALUES (%s, %s) "
    result_table_1_insert = (row['location_region'],row['risk_score'])
    cursor.execute(insert_result_table_1, result_table_1_insert)
    conn.commit()
```

```python

for index , row in result_table_2.iterrows():
    insert_result_table_2 = "INSERT INTO DB_FRAUD.RSLT_2(receiving_address,amount,timestamp)  VALUES (%s, %s,%s) "
    result_table_2_insert = (row['receiving_address'],row['amount'], row['timestamp'])
    cursor.execute(insert_result_table_2, result_table_2_insert)
    conn.commit() 
```


 ##

# CRIAÇÃO  TABELAS MYSQL

![MySQL](https://img.shields.io/badge/mysql-4479A1.svg?style=for-the-badge&logo=mysql&logoColor=white)

### CRIAÇÃOTABELA LOG DE ERRO
``` sql
CREATE TABLE IF NOT EXISTS DT_LOGERROR (
timestamp datetime ,
sending_address varchar(250) ,
receiving_address varchar(250) ,
amount int ,
transaction_type varchar(250) ,
location_region varchar(250) ,
ip_prefix varchar(250) ,
login_frequency varchar(250) ,
session_duration varchar(250) ,
purchase_pattern varchar(250) ,
age_group varchar(250) ,
risk_score NUMERIC(10,2) ,
anomaly varchar(250) ,
error_message varchar(250)
)
```
##
### CRIAÇÃO DA TABELA DATAQUALITY

``` SQL
CREATE TABLE IF NOT EXISTS DT_QTLT (
qtd_erros_Log int, 
qtd_erros_region__not_exits int ,
qtd__erros_amount_none int ,
qtd_erros_risk_none int ,
qtd_total_registros int ,
percent_conf_log decimal(10,2) ,
Dt_ultimo_inset datetime
)

```
##

### CRIAÇÃO TABELA RESULTADO 1 :

``` sql
CREATE TABLE IF NOT EXISTS RSLT_1(
location_region varchar(250), 
risk_score decimal(10,2))
```



##
### CRIAÇÃO TABELA RESULTADO 2:

``` SQL
CREATE TABLE IF NOT EXISTS RSLT_2 (
receiving_address varchar(250), 
amount int ,
timestamp datetime
)
```
