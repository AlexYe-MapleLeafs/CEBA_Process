from google.cloud import bigquery
from pandas.tseries.offsets import BDay
import pandas as pd
from datetime import datetime, timedelta, date
import numpy as np


# Get Last business day
def last_bday(date_val):
    holiday_list = [ "2023-01-02","2024-01-01", # new year's day
                    "2023-02-20","2024-02-19", # family day
                    "2023-04-07","2024-03-29", # Good Friday
                    "2023-05-22","2024-05-20", # Victoria's day
                    "2023-07-03","2024-07-01", # Canada Day
                    "2023-08-07","2024-08-05", # August Civic Holiday
                    "2023-09-04","2024-09-02", # labour Day
                    "2023-10-02","2024-09-30", # National Day for Truth and Reconciliation 
                    "2023-10-09","2024-10-14", # Thanksgiving
                    "2023-11-13","2024-11-11", # Remembrance Day
                    "2023-12-25","2024-12-25", # Chrismass Day
                    "2023-12-26","2024-12-26"] # Boxing Day
    prevBday_date = date_val - BDay(1)
    if prevBday_date.strftime('%Y-%m-%d') in holiday_list:
        prevBday_date = prevBday_date - BDay(1)
    return prevBday_date


# Get current reporting month's last business date ###
# get today's date
today = datetime.today()

# Get the first day of the month
first = today.replace(day=1)


# get the last business day before the input date
# because the input date is first day of the month, so here got the last business date of previous month
LastBdateReportMonth_Date = last_bday(first)
LastBdateReportMonth = LastBdateReportMonth_Date.strftime('%Y-%m-%d')
print('Last Business Date in Reporting Month: ' + LastBdateReportMonth)

# Get previous reporting month's last business date ###
# because CEBA report for Don will need previous month's last date interest collected info
todayMinus1Mon = datetime.today() - pd.offsets.MonthEnd(1)
firstMinus1Mon = todayMinus1Mon.replace(day=1)
LastBdatePreReportMonth_Date = last_bday(firstMinus1Mon)
LastBdatePreReportMonth = LastBdatePreReportMonth_Date.strftime('%Y-%m-%d')
print('Last Business Date in Previous Reporting Month: ' + LastBdatePreReportMonth)

# %%bigquery --params {"LastBdatePreReportMonth_1": '2024-05-01', "LastBdateReportMonth_1": '2024-05-31'}
# CREATE OR REPLACE TABLE ceba_report_interactive.ceba_reporting_month_data as
# SELECT 
#     businessEffectiveDate as businesseffectivedate,
#     concat(lpad(cast(combined_loan_info_record.ic_acct_name_rec.ic_an_transit as string),5,'0'),
#     lpad(cast(combined_loan_info_record.ic_acct_name_rec.ic_an_acct_no as string),7,'0')) as oll_number,
#     combined_loan_info_record.ic_guarantor_name_rec.ic_gn_name_address.ic_gn_addr1,
#     combined_loan_info_record.loan_info_rec.ic_loan_level_part1.ic_ln_os_loan_amt, 
#     combined_loan_info_record.loan_info_rec.ic_loan_level_part1.ic_ln_int_collect,
#     combined_loan_info_record.loan_info_rec.ic_loan_level_part1.ic_ln_uncollect_int,
#     combined_loan_info_record.loan_info_rec.ic_loan_level_part1.ic_ln_last_int_cal_date,
#     combined_loan_info_record.loan_info_rec.ic_loan_level_part2.ic_ln_accrued_int,
#     combined_loan_info_record.loan_info_rec.ic_loan_level_part2.ic_ln_lst_princ_chg,
#     combined_loan_info_record.loan_info_rec.ic_loan_level_part2.ic_ln_lst_princ_date,
#     combined_loan_info_record.ic_acct_name_rec.ic_an_name_address.ic_an_name
#     FROM `cidat-50000-prd-7a53.hive_tsz_bl_oll.tsz-bl_bl376t2_v0` 
# WHERE businessEffectiveDate between @LastBdatePreReportMonth_1 and @LastBdateReportMonth_1
# and combined_loan_info_record.ic_acct_name_rec.ic_an_transit = 34033
# and combined_loan_info_record.ic_acct_name_rec.ic_an_acct_no >= 1290
# and combined_loan_info_record.ic_acct_name_rec.ic_an_acct_no <= 1869
# order by businesseffectivedate, oll_number
# limit 100

# Construct a BigQuery client object.
client = bigquery.Client()

query_1 = """
CREATE OR REPLACE TABLE ceba_report_interactive.ceba_"""  + LastBdateReportMonth.replace('-','') + """_data as
SELECT 
    businessEffectiveDate as businesseffectivedate,
    concat(lpad(cast(combined_loan_info_record.ic_acct_name_rec.ic_an_transit as string),5,'0'),
    lpad(cast(combined_loan_info_record.ic_acct_name_rec.ic_an_acct_no as string),7,'0')) as oll_number,
    combined_loan_info_record.ic_guarantor_name_rec.ic_gn_name_address.ic_gn_addr1,
    combined_loan_info_record.loan_info_rec.ic_loan_level_part1.ic_ln_os_loan_amt, 
    combined_loan_info_record.loan_info_rec.ic_loan_level_part1.ic_ln_int_collect,
    combined_loan_info_record.loan_info_rec.ic_loan_level_part1.ic_ln_uncollect_int,
    combined_loan_info_record.loan_info_rec.ic_loan_level_part1.ic_ln_last_int_cal_date,
    combined_loan_info_record.loan_info_rec.ic_loan_level_part2.ic_ln_accrued_int,
    combined_loan_info_record.loan_info_rec.ic_loan_level_part2.ic_ln_lst_princ_chg,
    combined_loan_info_record.loan_info_rec.ic_loan_level_part2.ic_ln_lst_princ_date,
    combined_loan_info_record.ic_acct_name_rec.ic_an_name_address.ic_an_name
    FROM `cidat-50000.hive_oll.tsz-376` 
WHERE businessEffectiveDate between '""" + LastBdatePreReportMonth + """' and '"""  + LastBdateReportMonth + """'
and combined_loan_info_record.ic_acct_name_rec.ic_an_transit = 34033
and combined_loan_info_record.ic_acct_name_rec.ic_an_acct_no >= 1290
and combined_loan_info_record.ic_acct_name_rec.ic_an_acct_no <= 1869
order by businesseffectivedate, oll_number
"""

print(query_1)

# Run query in BigQuery to get data and create table to store it
query_1_job = client.query(query_1)  # Make an API request.

# CEBA_01 = query_job.result().to_dataframe(create_bqstorage_client=True)
# CEBA_01.head(10)
query_1_job.result()

# # drop table no longer needed
# query_drop_table = """
# DROP TABLE `cidat-10040-int-445d.ceba_report_interactive.ceba_reporting_month_data_20240531`;
# """
# query_drop_table_job = client.query(query_drop_table)  # Make an API request.

query_2 = """
SELECT *
FROM `cidat-10040-int-445d.ceba_report_interactive.ceba_"""  + LastBdateReportMonth.replace('-','') + """_data`
 order by businesseffectivedate, oll_number
"""

query_2_job = client.query(query_2)  # Make an API request.

CEBA_01 = query_2_job.result().to_dataframe(create_bqstorage_client=True)
CEBA_01.head(10)


# prepare the data for BD2 and Loan report
CEBA_02 = CEBA_01.copy()
CEBA_02['oll_number'] = CEBA_02['oll_number'].astype(str)
CEBA_02['businesseffectivedate'] = CEBA_02['businesseffectivedate'].astype(str)
CEBA_02['ic_gn_addr1'] = CEBA_02['ic_gn_addr1'].str.replace(" ", "")


CEBA_02['ic_ln_last_int_cal_date'] = CEBA_02['ic_ln_last_int_cal_date'].astype(str)
CEBA_02['ic_ln_last_int_cal_date_01'] = np.where(CEBA_02['ic_ln_last_int_cal_date'].str.len() < 6,
                                                '0' + CEBA_02['ic_ln_last_int_cal_date'],
                                                CEBA_02['ic_ln_last_int_cal_date'])
CEBA_02['ic_ln_last_int_cal_date_02'] = '20' + CEBA_02['ic_ln_last_int_cal_date_01'].str[-2:] + '-' +\
                                        CEBA_02['ic_ln_last_int_cal_date_01'].str[:2] +  '-' +\
                                        CEBA_02['ic_ln_last_int_cal_date_01'].str[2:4]
                                            

CEBA_02['ic_ln_lst_princ_date'] = CEBA_02['ic_ln_lst_princ_date'].astype(str)
CEBA_02['ic_ln_lst_princ_date_01'] = np.where(CEBA_02['ic_ln_lst_princ_date'] == '0', '',
                                             np.where(CEBA_02['ic_ln_lst_princ_date'].str.len() < 6,
                                                '0' + CEBA_02['ic_ln_lst_princ_date'],
                                                CEBA_02['ic_ln_lst_princ_date']))
CEBA_02['ic_ln_lst_princ_date_02'] = np.where(CEBA_02['ic_ln_lst_princ_date_01'] == '', '',
                                        '20' + CEBA_02['ic_ln_lst_princ_date_01'].str[-2:] + '-' +\
                                        CEBA_02['ic_ln_lst_princ_date_01'].str[:2] +  '-' +\
                                        CEBA_02['ic_ln_lst_princ_date_01'].str[2:4])

CEBA_03 = CEBA_02.drop(columns=['ic_ln_last_int_cal_date', 'ic_ln_last_int_cal_date_01',
                              'ic_ln_lst_princ_date', 'ic_ln_lst_princ_date_01'])


CEBA_03.rename(columns = {'ic_ln_last_int_cal_date_02':'ic_ln_last_int_cal_date', 
                         'ic_ln_lst_princ_date_02':'ic_ln_lst_princ_date'}, inplace=True)

CEBA_03.tail(10)

#### CEBA BD2 report starts
####
DB2_04_PreMonLastD = CEBA_03[CEBA_03['businesseffectivedate'] == LastBdatePreReportMonth].reset_index(drop=True)
DB2_04_CurMonFullMon = CEBA_03[CEBA_03['businesseffectivedate'] != LastBdatePreReportMonth].reset_index(drop=True)
DB2_04_CurMonLastD = CEBA_03[CEBA_03['businesseffectivedate'] == LastBdateReportMonth].reset_index(drop=True)

# Total Average Balance
DB2_05_CurMonAvg = DB2_04_CurMonFullMon.groupby(['oll_number']).agg(ic_ln_os_loan_amt_avg=('ic_ln_os_loan_amt', 'mean')).reset_index()
DB2_05_CurMonTotalAvg = DB2_05_CurMonAvg['ic_ln_os_loan_amt_avg'].sum()

# Total Spot Balance
DB2_05_CurMonLastD_TotalSportBal = DB2_04_CurMonLastD['ic_ln_os_loan_amt'].sum()

# Total Interest Accural
# first sum() get the sum of each column, second sum() sum the sum of each column
DB2_05_CurMonLastD_totalIntAccr = DB2_04_CurMonLastD[['ic_ln_int_collect','ic_ln_uncollect_int','ic_ln_accrued_int']].sum().sum()

DB2_06_PreMonLastD_TotalIntRemit = DB2_04_PreMonLastD['ic_ln_int_collect'].sum()

DB2_06_CurMonLastD_TotalUnpaidIntAccrued = DB2_05_CurMonLastD_totalIntAccr - DB2_06_PreMonLastD_TotalIntRemit

# Total Spot Balance: The balance at spot which is the last business date of reporting month
# Total Interest Accrued: Total interest Accrual so far as at the last business date of reporting month
# Total interest remit: Total amount of previous reporting month's "ic_ln_int_collect" in worksheet "Total Spot Bal & Int Accrued"
# Total interest remit: Total amount of interest have sent to BDC (we have collected from clients)
# Total Average Balance: From Worksheet "PT_Total_Average_Balance"

DB2_07_Report = {'businesseffectivedate': [LastBdateReportMonth],
                 'Total Spot Balance': [DB2_05_CurMonLastD_TotalSportBal],
                 'Total Interest Accrued': [DB2_05_CurMonLastD_totalIntAccr],
                 'Total interest remit': [DB2_06_PreMonLastD_TotalIntRemit],
                 'Unpaid Interest Accrued': [DB2_06_CurMonLastD_TotalUnpaidIntAccrued],
                 'Total Average Balance': [DB2_05_CurMonTotalAvg]
                 }

DB2_07_ReportTable = pd.DataFrame(data=DB2_07_Report)

DB2_07_ReportTable['Total Spot Balance'] = DB2_07_ReportTable['Total Spot Balance'].astype(float)
DB2_07_ReportTable['Total Interest Accrued'] = DB2_07_ReportTable['Total Interest Accrued'].astype(float)
DB2_07_ReportTable['Total interest remit'] = DB2_07_ReportTable['Total interest remit'].astype(float)
DB2_07_ReportTable['Unpaid Interest Accrued'] = DB2_07_ReportTable['Unpaid Interest Accrued'].astype(float)
DB2_07_ReportTable['Total Average Balance'] = DB2_07_ReportTable['Total Average Balance'].astype(float).round(2)

DB2_07_ReportTable.head(10)

####
#### CEBA BD2 report ends

# This code snippet loads data from a dataframe to a BigQuery table.
# If the BigQuery table does not exist, the table will be created.
# Make sure to update the schema to match your data.

def load_data_df_to_bq(df,
                        bigquery_destination_project_id,
                        bigquery_destination_dataset,
                        bigquery_destination_table,
                        schema):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = '{}.{}.{}'.format(bigquery_destination_project_id,bigquery_destination_dataset,bigquery_destination_table)
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE"
    )
    
    load_job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.

    result = load_job.result()
    print(result)


schema = [
    bigquery.SchemaField("businesseffectivedate", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Total Spot Balance", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Total Interest Accrued", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Total interest remit", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Unpaid Interest Accrued", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Total Average Balance", "FLOAT64", mode="REQUIRED"),
]

## For example 'gs://hodazbucket-interactive-985747/s4161262/iris.csv'. Change the path to match your first name to create your own directory and csv file.
## Add the destination project id. For example 'cidat-00202-int-9359'.
## Add the destination BQ dataset, For exmaple 'notebook_workshop_interactive'.
## Add the destination BQ table name - this is a new table that will be created. For example 'sID_iris'


load_data_df_to_bq(df=DB2_07_ReportTable,
                    bigquery_destination_project_id='cidat-10040-int-445d',
                    bigquery_destination_dataset='ceba_report_interactive',
                    bigquery_destination_table='ceba_' + LastBdateReportMonth.replace('-','') + '_db2_report',
                    schema=schema)



#### CEBA Loan Report starts
####
CEBA_04_PreMonLastD = CEBA_03[CEBA_03['businesseffectivedate'] == LastBdatePreReportMonth].reset_index(drop=True)
CEBA_04_PreMonLastD['Total_Interest_Accrual'] = CEBA_04_PreMonLastD['ic_ln_int_collect'] + \
                                        CEBA_04_PreMonLastD['ic_ln_uncollect_int']  + \
                                        CEBA_04_PreMonLastD['ic_ln_accrued_int'] 
print(list(CEBA_04_PreMonLastD))

CEBA_04_CurMonLastD = CEBA_03[CEBA_03['businesseffectivedate'] == LastBdateReportMonth].reset_index(drop=True)
CEBA_04_CurMonLastD['Total_Interest_Accrual'] = CEBA_04_CurMonLastD['ic_ln_int_collect'] + \
                                        CEBA_04_CurMonLastD['ic_ln_uncollect_int']  + \
                                        CEBA_04_CurMonLastD['ic_ln_accrued_int'] 

CEBA_05_CurMonLastD = \
    CEBA_04_CurMonLastD.merge(CEBA_04_PreMonLastD[['oll_number','ic_ln_int_collect','Total_Interest_Accrual']],
                    how='left',
                    left_on=['oll_number'],
                    right_on=['oll_number'])
    
print(list(CEBA_05_CurMonLastD))

# x is current reporting month
# y is previous reporting month
CEBA_05_CurMonLastD['Interest_Repayment'] = CEBA_05_CurMonLastD['ic_ln_int_collect_x'] - CEBA_05_CurMonLastD['ic_ln_int_collect_y'] 
CEBA_05_CurMonLastD['Cumulative_Int_Owing'] = CEBA_05_CurMonLastD['ic_ln_uncollect_int'] + CEBA_05_CurMonLastD['ic_ln_accrued_int'] 
CEBA_05_CurMonLastD['Monthly_Int_Applicable'] = CEBA_05_CurMonLastD['Total_Interest_Accrual_x'] - CEBA_05_CurMonLastD['Total_Interest_Accrual_y']



CEBA_06_CurMonLastD_report = CEBA_05_CurMonLastD[
    ['businesseffectivedate', 'oll_number', 'ic_gn_addr1', 'ic_ln_os_loan_amt', 
     'ic_ln_last_int_cal_date', 
     'ic_ln_int_collect_x', 'ic_ln_int_collect_y', 'Interest_Repayment',
     'ic_ln_uncollect_int', 'ic_ln_accrued_int', 'Cumulative_Int_Owing', 
     'ic_ln_lst_princ_chg',  'ic_ln_lst_princ_date', 
      'Total_Interest_Accrual_x', 'Total_Interest_Accrual_y', 'Monthly_Int_Applicable']]

CEBA_06_CurMonLastD_report.rename(columns = {'ic_ln_int_collect_x': 'ic_ln_int_collect_' + LastBdateReportMonth, 
                                             'ic_ln_int_collect_y':'ic_ln_int_collect_' + LastBdatePreReportMonth,
                                             'Total_Interest_Accrual_x': 'Total_Interest_Accrual_' + LastBdateReportMonth,
                                             'Total_Interest_Accrual_y': 'Total_Interest_Accrual_' + LastBdatePreReportMonth,
                                             'ic_gn_addr1': 'Former_oll_number',
                                             'ic_ln_os_loan_amt': 'Outstanding_principal',
                                             'ic_ln_last_int_cal_date': 'Interest Date_repayment',
                                             'ic_ln_lst_princ_chg': 'Principal_RePayment',
                                             'ic_ln_lst_princ_date': 'Principal Date_repayment'
                                             }, inplace=True)

CEBA_06_CurMonLastD_report['Principal Date_repayment'] = np.where( CEBA_06_CurMonLastD_report['Principal Date_repayment']=='2000-00-00',
                                                                  '',
                                                                  CEBA_06_CurMonLastD_report['Principal Date_repayment'])
                                                                  
# print(CEBA_06_CurMonLastD_report.dtypes)

CEBA_06_CurMonLastD_report.head(5)

####
#### CEBA Loan Report ends

CEBA_06_CurMonLastD_report['Outstanding_principal'] = CEBA_06_CurMonLastD_report['Outstanding_principal'].astype('float')
CEBA_06_CurMonLastD_report['ic_ln_int_collect_' + LastBdateReportMonth] = CEBA_06_CurMonLastD_report['ic_ln_int_collect_' + LastBdateReportMonth].astype('float')
CEBA_06_CurMonLastD_report['ic_ln_int_collect_' + LastBdatePreReportMonth] = CEBA_06_CurMonLastD_report['ic_ln_int_collect_' + LastBdatePreReportMonth].astype('float')
CEBA_06_CurMonLastD_report['Interest_Repayment'] = CEBA_06_CurMonLastD_report['Interest_Repayment'].astype('float')
CEBA_06_CurMonLastD_report['ic_ln_uncollect_int'] = CEBA_06_CurMonLastD_report['ic_ln_uncollect_int'].astype('float')
CEBA_06_CurMonLastD_report['ic_ln_accrued_int'] = CEBA_06_CurMonLastD_report['ic_ln_accrued_int'].astype('float')
CEBA_06_CurMonLastD_report['Cumulative_Int_Owing'] = CEBA_06_CurMonLastD_report['Cumulative_Int_Owing'].astype('float')
CEBA_06_CurMonLastD_report['Principal_RePayment'] = CEBA_06_CurMonLastD_report['Principal_RePayment'].astype('float')
CEBA_06_CurMonLastD_report['Total_Interest_Accrual_' + LastBdateReportMonth] = CEBA_06_CurMonLastD_report['Total_Interest_Accrual_' + LastBdateReportMonth].astype('float')
CEBA_06_CurMonLastD_report['Total_Interest_Accrual_' + LastBdatePreReportMonth] = CEBA_06_CurMonLastD_report['Total_Interest_Accrual_' + LastBdatePreReportMonth].astype('float')
CEBA_06_CurMonLastD_report['Monthly_Int_Applicable'] = CEBA_06_CurMonLastD_report['Monthly_Int_Applicable'].astype('float')

CEBA_06_CurMonLastD_report.head(3)
# print(CEBA_06_CurMonLastD_report.dtypes)


# This code snippet loads data from a dataframe to a BigQuery table.
# If the BigQuery table does not exist, the table will be created.
# Make sure to update the schema to match your data.

def load_data_df_to_bq(df,
                        bigquery_destination_project_id,
                        bigquery_destination_dataset,
                        bigquery_destination_table,
                        schema):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = '{}.{}.{}'.format(bigquery_destination_project_id,bigquery_destination_dataset,bigquery_destination_table)
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE"
    )
    
    load_job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.

    result = load_job.result()
    print(result)


schema = [
    bigquery.SchemaField("businesseffectivedate", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("oll_number", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Former_oll_number", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Outstanding_principal", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Interest Date_repayment", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ic_ln_int_collect_" + LastBdateReportMonth, "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ic_ln_int_collect_" + LastBdatePreReportMonth, "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Interest_Repayment", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ic_ln_uncollect_int", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("ic_ln_accrued_int", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Cumulative_Int_Owing", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Principal_RePayment", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Principal Date_repayment", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Total_Interest_Accrual_" + LastBdateReportMonth, "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Total_Interest_Accrual_" + LastBdatePreReportMonth, "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("Monthly_Int_Applicable", "FLOAT64", mode="REQUIRED")
]

## For example 'gs://hodazbucket-interactive-985747/s4161262/iris.csv'. Change the path to match your first name to create your own directory and csv file.
## Add the destination project id. For example 'cidat-00202-int-9359'.
## Add the destination BQ dataset, For exmaple 'notebook_workshop_interactive'.
## Add the destination BQ table name - this is a new table that will be created. For example 'sID_iris'


load_data_df_to_bq(df=CEBA_06_CurMonLastD_report,
                    bigquery_destination_project_id='cidat-10040-int-445d',
                    bigquery_destination_dataset='ceba_report_interactive',
                    bigquery_destination_table='ceba_' + LastBdateReportMonth.replace('-','') + '_loan_report',
                    schema=schema)
