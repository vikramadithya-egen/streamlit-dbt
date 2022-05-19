import streamlit as st
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import plotly.graph_objects as go
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from datetime import datetime
import altair as alt
import plotly.express as px
from PIL import Image
import base64
import pytz




st.set_page_config(layout="wide")


@st.cache(allow_output_mutation=True)# snowflake DB connection
def load_data_SF():  
    conn = snowflake.connector.connect(
    user='vikramadithya.baddam@springml.com',
    password='Sunny@365',
    account='bp02230.us-central1.gcp',
    warehouse='LOAD_WH',
    database='DBT_TEST',
    schema='DBT_TEST_DEMO_DBT_TEST__AUDIT',
    role = 'SYSADMIN'
    )
    cur = conn.cursor()
    sql = "select * from AGGREGATE_RESULTS;"
    cur.execute(sql)
    data = cur.fetch_pandas_all()
    cur.close()
    print(data)
    print("data retrieved")
    lowercase = lambda x : str(x).lower()
    data.rename(lowercase,axis='columns',inplace=True)
    return data 



def modify_df(df_main,status):
    df_new = df_main.loc[df_main['status']== status].copy()
    df_new['Test_count'] = df_new.groupby('test_name')['status'].transform('size')
    df_new = df_new.drop_duplicates(subset = ["test_name"])
    df_new.reset_index(inplace = True, drop = True)
    del df_new['status']
    df_new.rename(columns = {'Test_count':status}, inplace = True)
    return df_new

@st.cache(allow_output_mutation=True) 
def make_date_compatible(df_main):
    utc=pytz.UTC
    for i in range(len(df_main['execution_time'])):
        temp = df_main['execution_time'][i]
        temp = temp.to_pydatetime()
        df_main['execution_time'][i] = temp.replace(tzinfo=utc)
    


def merge_df(df_passed,df_failed):
    # print(df_passed ,df_failed ,"both")
    if df_passed.empty and df_failed.empty :
        final_df = df_passed.reindex(df_passed.columns.union(df_failed.columns), axis=1)
        final_df = final_df[final_df.columns[::-1]]
        final_df.loc[len(final_df.index)] = ['No Data', 0, 0]
    else:
        final_df = pd.merge(df_passed, df_failed, how='outer', on = 'test_name')
        # print(final_df,"one")
        final_df['PASSED'] = final_df['PASSED'].fillna(0)
        final_df['FAILED'] = final_df['FAILED'].fillna(0)
        final_df['PASSED'] = final_df['PASSED'].astype(int)
        final_df['FAILED'] = final_df['FAILED'].astype(int)
        final_df.loc[len(final_df.index)] = ['Grand Total', final_df['PASSED'].sum(), final_df['FAILED'].sum()]
    
    return final_df

def get_table_data(df_main):
    temp_df = df_main.iloc[:-1].copy()
    passed = temp_df.loc[temp_df['PASSED'] != 0]['PASSED'].count()
    failed = temp_df.loc[temp_df['FAILED'] != 0]['FAILED'].count()
    data_one = pd.DataFrame({'Result': ['passed', 'failed'], 'Quantity': [passed.item(), failed.item()],'Types':["passed","failed"]})
    data_two = pd.DataFrame({'passed':[None,passed.item()],'failed':[failed.item(),None]},index =["failed","passed"])
    
    return data_one,data_two

def get_failed_rows(df_main):
    df_temp = df_main[['test_name','rows_failed']].copy()
    if not df_temp.empty:
        df_temp = df_temp.groupby('test_name',as_index=False).agg({'rows_failed': 'sum'}) #not using groupby column as index 
        df_temp.loc[len(df_temp.index)] = ['Grand Total',df_temp['rows_failed'].sum()]
    else:
        df_temp.loc[len(df_temp.index)] = ['No Data',0]

    return df_temp

st.markdown(
            """
            <style>
            .date-text{
                font-size:20px;
                font-family:sans-serif;
                padding-right:20px;
                
            }
            .test-date{
                font-size:20px;
                font-family:sans-serif;
                padding-top:10px
            }
            .total-tests{
                font-size:25px;
                font-family:sans-serif;
                padding-top:10px
            }
            .table-padding{
                padding-top:20px
            }
            .mySelector {
                color: var(--text-color);
                }
            
            .title{
                font-size:40px;
                font-family:sans-serif;
                color:#42c5f5;
                padding:0px;
                text-align:left;
                float:left
                
            }
            .logo-img{
                float:right;
                padding-bottom:10px
            }
            .header-style {
                font-size:25px;
                font-family:sans-serif;
                padding:0px;
            }
            .block-container {
                padding-top: 5rem;
            }
            footer {
                text-align:center;
                padding:150px;
                }
            

            </style>
            """,
            unsafe_allow_html=True
)






col1, buffer , col3 = st.columns([5,0.2,4.5])
with col1:
    st.write('<div class="title">DBT Testing Summary</div>',unsafe_allow_html=True)

    initial = datetime(2020, 1, 1)
    final = datetime(2023, 1, 1)
    range_ = st.slider('Select Date\n\n',min_value=initial, value=(initial, final), max_value=final, format="YY/MM/DD")
    start_d , end_d= range_[0],range_[1]


utc=pytz.UTC
start_d = datetime.combine(start_d, datetime.min.time())
end_d = datetime.combine(end_d, datetime.max.time())
start_d = utc.localize(start_d)
end_d = utc.localize(end_d)

df_main = load_data_SF() #load data from snowflake
print("function executed")
if ( 'datetime.datetime' in str(type(df_main['execution_time'][0])) ):
    print()
else:
    make_date_compatible(df_main)
print("dates compatible")
# print(df_main,"one")
# converting dates that are compatible with DB dates 



if (end_d >= start_d):
    # print(start_d,end_d)
    df_main = df_main[(df_main['execution_time'] >= start_d) & (df_main['execution_time'] <= end_d)]
    df_main.reset_index(inplace = True, drop = True)
    # print(df_main,"two")


    with col1:
        st.write('<div><span class="date-text"><b style="color:#a83268">Start Date:\n</b>{start_date}</span><span class="date-text"><b style="color:#a83268">End Date:\n</b>{end_date}\n</span></div>'.format(start_date = start_d.strftime("%d %b, %Y"),end_date = end_d.strftime("%d %b, %Y")),unsafe_allow_html=True)
        
        try:
            oldest_date = str(df_main['execution_time'].min().strftime("%d %b,%Y")) +" " + str(df_main['execution_time'].min().strftime("%H:%M:%S") )
            latest_date = str(df_main['execution_time'].max().strftime("%d %b,%Y")) +" " + str(df_main['execution_time'].max().strftime("%H:%M:%S") )
        except :
            oldest_date = None
            latest_date = None
        
        st.write('<div class="test-date"><b style="color:#a83268">Oldest Test Time:\n</b>{oldest_date}</div>'.format(oldest_date = oldest_date),unsafe_allow_html=True)
        st.write('<div class="test-date" ><b style="color:#a83268">Latest Test Time:</b>{latest_date}</div>'.format(latest_date=latest_date),unsafe_allow_html=True)
        
    
    with col3:
        image = Image.open('./sprinml_logo.png')
        LOGO_IMAGE = "./sprinml_logo.png"
        st.markdown(
            f"""
                <img class="logo-img" src="data:image/png;base64,{base64.b64encode(open(LOGO_IMAGE, "rb").read()).decode()}">
            """,
            unsafe_allow_html=True
        )

        
        with st.form("my_form",clear_on_submit=True):
            temp_df = df_main.drop_duplicates(subset=['test_name'])["test_name"]
            # print(temp_df,"temp_df............")
            options = st.multiselect('Select test names ',temp_df)
            submitted = st.form_submit_button("Submit")
            if submitted:
                print()

        # print(submitted ,options,"submitted and options.....................")
                
    
    df_exec = df_main.copy()
    del df_exec['execution_time'] #dropping execution_time column
    del df_exec['rows_failed'] #dropping rows_failed column to modify and show only test_name,passed,failed columns
    # print(df_exec)
    df_passed = modify_df(df_exec,'PASSED')
    df_failed = modify_df(df_exec,'FAILED')
    final_df = merge_df(df_passed,df_failed)
    # print(final_df)

    with col1:
        if submitted:
            filtered_df = final_df[final_df['test_name'].isin(options)].copy()
            filtered_df.reset_index(inplace = True, drop = True)
            # print(filtered_df ,"before")
            filtered_df.loc[len(filtered_df.index)] = ['Grand Total', filtered_df['PASSED'].sum(), filtered_df['FAILED'].sum()]  
            # print(filtered_df,"after")
            data_one,data_two = get_table_data(filtered_df)
            
            df_fail_rows = get_failed_rows(df_main)
            df_filter_fail_rows = df_fail_rows[df_fail_rows['test_name'].isin(options)].copy()
            df_filter_fail_rows.reset_index(inplace = True, drop = True)
            df_filter_fail_rows.loc[len(df_filter_fail_rows.index)] = ['Grand Total',df_filter_fail_rows['rows_failed'].sum()]
            
        else:
            data_one,data_two = get_table_data(final_df)
            df_fail_rows = get_failed_rows(df_main)
        # print(data_one)
        # print(data_two)

        #graph one 
        fig = px.bar(data_one, x='Result', y='Quantity', color='Types')
        fig.update_layout(
            autosize=False,
            width=600,
            height=600)
        fig.update_traces(width=0.5)
        st.plotly_chart(fig, use_container_width=True) 

        #graph two
        st.bar_chart(data_two,width=500,height=400,use_container_width=False) 

        
        
    with col3:
        print("\n\n")
        if submitted:
            final_df = filtered_df
            df_fail_rows = df_filter_fail_rows
        else:
            final_df = final_df
            df_fail_rows = df_fail_rows
            
        # print(final_df['test_name'].values,"final  df..............")
        if (final_df['test_name'].values[0] == 'No Data'):
            values = final_df[final_df['test_name'] == 'No Data'].values[0]
        else:
            values = final_df[final_df['test_name'] == 'Grand Total'].values[0]

        total = values[1] + values[2]    
        st.write("<div class='total-tests'><b style='color:#a83268'>Total Tests:\n\n</b><span class='total-tests'>{grand_total}</span>\n</div>".format(grand_total = total),unsafe_allow_html=True)

        styler_one = final_df.style.hide(axis = 'index') 
        st.write(styler_one.to_html(), unsafe_allow_html=True)
        st.markdown("""<style>.row_heading.level0 {display:none}.blank {display:none}</style> <br><br><br>""", unsafe_allow_html=True) #table one 
        
        st.dataframe(final_df,width=None,height=700) #table two
        st.markdown('##')
        
        styler_three = df_fail_rows.style.hide(axis = 'index') 
        st.write(styler_three.to_html(), unsafe_allow_html=True)
        st.markdown("""<style>.row_heading.level0 {display:none}.blank {display:none} .table-border{border: 1px solid black;}</style> <br><br>""", unsafe_allow_html=True) #table three 


else:
    st.markdown(f'<p style="text-size:20px">Start date should not be greater than end Date</p>',unsafe_allow_html=True)




