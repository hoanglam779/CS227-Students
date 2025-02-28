from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
import gc
from airflow.providers.common.sql.hooks.sql import DbApiHook, ConnectorProtocol
import os 

import logging
from datetime import datetime, timedelta
from datetime import datetime
import pandas as pd
import io

DAG_ID = "theDag"
START_DATE = datetime(2025, 2, 15)
SCHEDULE_INTERVAL = "0 0 1 3,8 *"
CATCHUP = False


def saveAndClearDf(df, path, name, ti):
    ti.xcom_push(key="rateOfDup"+name, value=rateOfDuplicate(df))
    df.to_csv(path,index=False)
    del df
    gc.collect()

def rateOfDuplicate(df):
    return df.duplicated().sum() / len(df)

def fetchDataTask(**kwargs):
    hook = PostgresHook(postgres_conn_id="localpostgres")
    dir_path = str(os.path.dirname(os.path.realpath(__file__)))

    sql = "SELECT * FROM xeploaiav"
    dv = hook.get_pandas_df(sql)
    saveAndClearDf(df=dv, path= dir_path + "//xeploaiav.csv",name= 'dv',ti= kwargs['ti'])
    drl = hook.get_pandas_df("SELECT * FROM diemrl")
    saveAndClearDf(drl, dir_path + "//diemrl.csv", 'drl', kwargs['ti'])

    diem = hook.get_pandas_df("SELECT * FROM diem")
    saveAndClearDf(diem, dir_path + "//diem.csv", 'diem', kwargs['ti'])

    cc = hook.get_pandas_df("SELECT * FROM sinhvien_chungchi")
    saveAndClearDf(cc, dir_path + "//sinhvien_chungchi.csv", 'cc', kwargs['ti'])

    dtb = hook.get_pandas_df("SELECT * FROM sinhvien_dtb_hocky")
    saveAndClearDf(dtb, dir_path + "//sinhvien_dtb_hocky.csv", 'dtb', kwargs['ti'])

    tn = hook.get_pandas_df("SELECT * FROM totnghiep")
    saveAndClearDf(tn, dir_path + "//totnghiep.csv", 'tn', kwargs['ti'])

def naDupRate(name, ti, task_id):
    dup = ti.xcom_pull(key="rateOfDup"+name)
    return dup

def checkErrorTask(**kwargs):
    ti = kwargs['ti']
    task_id = "extractFromFunctionalDatabase"

    tableNames = ['dv', 'drl', 'diem', 'cc', 'dtb', 'tn']
    dup = []

    for n in tableNames:
        dupi = naDupRate(name= n, ti=ti, task_id = task_id)
        dup.append(dupi)

    ti.xcom_push(key="dup", value=dup)

    logging.info(dup)

    if any([i > 0.25 for i in dup]):
        return "highError"
    else:
        return "lowError"
    
     


#https://ctsv.uit.edu.vn/bai-viet/khoa-hoc-lop-sinh-vien-thoi-gian-tiet-hoc
khkttt = ['CNCL', 'CNTT', 'KHDL']
khmt = ['KHMT', 'KHTN', 'KHNT', 'ATTN', 'KHCL']
httt = ['CTTT', 'HTCL', 'HTTT', 'TMCL', 'TMĐT']
ktmt = ['KTMT', 'MTCL', 'MTIO', 'MTLK']
mmtt = ['ATTN', 'ATBC', 'ATCL', 'ATTT', 'MMCL', 'MMTT', 'ANTN', 'ANTT']
cnpm = ['KTPM', 'PMCL']
khoas = ['MMTT', 'KHKTTT', 'HTTT', 'KHMT', 'KTMT', 'CNPM']
def khoa(x):
  x = str(x).upper()
  try:
    for i in khkttt:
      if i in x:
        return 'KHKTTT'
    for i in khmt:
      if i in x:
        return 'KHMT'
    for i in httt:
      if i in x:
        return 'HTTT'
    for i in ktmt:
      if i in x:
        return 'KTMT'
    for i in mmtt:
      if i in x:
        return 'MMTT'
    for i in cnpm:
      if i in x:
        return 'CNPM'
  except:
    return 'NULL'

def transformingData(**kwargs):
    ti = kwargs['ti']
    dir_path = str(os.path.dirname(os.path.realpath(__file__)))

    dv = pd.read_csv(dir_path + "//xeploaiav.csv")
    dv = dv.dropna(how='all')
    thi = dv[list('null' in x.lower() for x in dv.ghichu.to_string().split('\n'))]
    toe = dv[list('null' not in x.lower() for x in dv.ghichu.to_string().split('\n'))]
    saveAndClearDf(df=thi, path= dir_path + "//dv_thi.csv",name= 'thi',ti= ti)
    saveAndClearDf(df=toe, path= dir_path + "//dv_toe.csv",name= 'toe',ti= ti)
    saveAndClearDf(df=dv, path= dir_path + "//xeploaiav.csv",name= 'dv',ti= ti)

    drl = pd.read_csv(dir_path + "//diemrl.csv")
    drl = drl.dropna(how='all')
    drl['khoa'] = drl.lopsh.map(khoa)
    drl['nganh'] = drl.lopsh.map(lambda x: ''.join((i for i in str(x) if i.isalpha())))
    saveAndClearDf(df= drl, path= dir_path+ "//diemrl.csv",name= 'drl',ti= ti)

    diem = pd.read_csv(dir_path + "//diem.csv")
    diem = diem.dropna(how= 'all')
    saveAndClearDf(df= diem, path= dir_path + "//diem.csv",name= 'diem',ti= ti)

    cc = pd.read_csv(dir_path + "//sinhvien_chungchi.csv")
    cc.ngaythi = pd.to_datetime(cc.ngaythi, format='%Y-%m-%d')
    cc['nam'] = cc.ngaythi.dt.year
    iel = cc[cc.loaixn == 'IELTS']
    iel.tongdiem = [float(x) for x in iel.tongdiem]
    toe_sw = cc[cc.loaixn == 'TOEIC_SW']
    toe_lr = cc[cc.loaixn == 'TOEIC_LR']
    saveAndClearDf(df= cc, path= dir_path + "//sinhvien_chungchi.csv",name= 'cc',ti= ti)
    saveAndClearDf(df= iel, path= dir_path + "//cc_ielts.csv",name= 'iel',ti= ti)
    saveAndClearDf(df= toe_sw, path= dir_path + "//cc_toe_sw.csv",name= 'toe_sw',ti= ti)
    saveAndClearDf(df= toe_lr, path= dir_path + "//cc_toe_lr.csv",name= 'toe_lr',ti= ti)

    tn = pd.read_csv(dir_path + "//totnghiep.csv")
    tn = tn.dropna()
    tn = tn.reset_index(drop=True)
    tn[' xeploai'] = tn[' xeploai'].map(lambda x: x.upper())
    tn['nam'] =  tn[' ngaycapvb'].map(lambda x: datetime.strptime(x[1:11], '%d/%m/%Y').year)
    tn = tn.sort_values('nam', ascending=True)


with DAG(
    dag_id = DAG_ID,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=CATCHUP) as dag:
        fetch = PythonOperator(
        task_id = "extractFromFunctionalDatabase",
        python_callable = fetchDataTask,
        )

        checkTask = BranchPythonOperator(
            task_id = "checkDataError",
            python_callable = checkErrorTask
        )
        fetch >> checkTask
        
        t1 = EmptyOperator(
            task_id="lowError",
        )
        t2 = EmptyOperator(
            task_id="highError",
        )
        transformTask = PythonOperator(
            task_id = "transformTask",
            python_callable = transformingData
        )
        mailTask = PythonOperator(
            task_id="sendWarningEmail",
            python_callable = lambda: print("Send warning email")
        )

        checkTask >> Label("lowError") >> t1 >> transformTask
        checkTask >> Label("highError") >> t2 >> mailTask
