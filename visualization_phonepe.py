import pymysql
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import pandas as pd


def plot_map_transaction(result_df, year, quarter):

    map_transaction_df = result_df.groupby(["State", "Year", "Quarter"]).agg({'Transaction Amount': 'sum', 'Transaction Count': 'sum'}).reset_index()
    map_transaction_selected_df = map_transaction_df[(map_transaction_df["Year"] == year) & (map_transaction_df["Quarter"] == quarter)]
    fig = px.choropleth(
    map_transaction_selected_df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='State',
    color='Transaction Amount',
    hover_data=['Transaction Count','Transaction Amount'],
    color_continuous_scale='Blues'
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},geo=dict(bgcolor='#002b2b'),height=600, width=1300)
    return fig

def plot_map_user(result_df, year, quarter):

    map_user_df = result_df.groupby(["State", "Year", "Quarter"]).agg({'Registred Users': 'sum', 'App Opens': 'sum'}).reset_index()
    map_user_df["App Opens"] = map_user_df["App Opens"].replace({0 : "Unavailable"})
    map_user_selected_df = map_user_df[(map_user_df["Year"] == year) & (map_user_df["Quarter"] == quarter)]
    fig = px.choropleth(
    map_user_selected_df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='State',
    color='Registred Users',
    hover_data=['Registred Users','App Opens'],
    color_continuous_scale='Blues'
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},geo=dict(bgcolor='#002b2b'),height=600, width=1300)

    agg_user_df = result_df.groupby(["Year", "Quarter"]).agg({'Registred Users': 'sum', 'App Opens': 'sum'}).reset_index()
    agg_user_df["App Opens"] = agg_user_df["App Opens"].replace({0 : "Unavailable"})
    agg_user_selected_df = agg_user_df[(agg_user_df["Year"] == year) & (agg_user_df["Quarter"] == quarter)]
    return fig, agg_user_selected_df

def plot_agg_transaction(result_df, year, quarter):

    agg_transaction_df = result_df.groupby(["Year", "Quarter"]).agg({'Count': 'sum', 'Amount': 'sum'}).reset_index()
    agg_categories_transaction_df = result_df.groupby(["Year", "Quarter","Categories"]).agg({'Count': 'sum', 'Amount': 'sum'}).reset_index()
    agg_transaction_selected_df = agg_transaction_df[(agg_transaction_df["Year"] == year) & (agg_transaction_df["Quarter"] == quarter)]
    agg_categories_transaction_selected_df = agg_categories_transaction_df[(agg_categories_transaction_df["Year"] == year) & (agg_categories_transaction_df["Quarter"] == quarter)]
    agg_categories_transaction_selected_df = agg_categories_transaction_selected_df.loc[:, ['Categories', 'Count', 'Amount']]
    return agg_transaction_selected_df, agg_categories_transaction_selected_df 

def plot_agg_user(result_df,year, quarter):

    agg_brand_user_df = result_df.groupby(["Year", "Quarter", "Brand"]).agg({"Count" : "sum"}).reset_index()
    agg_brand_user_selected_df = agg_brand_user_df[(agg_brand_user_df["Year"] == year) & (agg_brand_user_df["Quarter"] == quarter)]
    agg_brand_user_selected_df = agg_brand_user_selected_df.loc[:, ['Brand', 'Count']]
    return agg_brand_user_selected_df


if __name__ == "__main__":

    host = 'localhost' 
    user = 'root' 
    password = '12121995' 
    dbname = 'Phonepe_Pulse'

    connection = pymysql.connect(host=host, user=user, password=password, db=dbname)
    cursor = connection.cursor()

    # set app page layout type
    st.set_page_config(layout="wide")

    with st.sidebar:        
        page = option_menu(
                            menu_title='Phonepe Pulse',
                            options=['Explore', 'Q & A'],
                            icons=['map','bar-chart-line'],
                            menu_icon="pin-map-fill",
                            default_index=0 ,
                            styles={"container": {"padding": "5!important"},
                                    "icon": {"color": "brown", "font-size": "23px"}, 
                                    "nav-link": {"color":"white","font-size": "20px", "text-align": "left", "margin":"0px", "--hover-color": "lightblue"},
                                    "nav-link-selected": {"background-color": "grey"},}  
                        )
    if page == "Explore":

        col1, col2, col3, col4 = st.columns([1,1,1,5])
        col5, col6 = st.columns([3,1])
        selection_type = col1.selectbox("Transactions/Users",["Transactions", "Users"])
        container_map = col5.container(border=True, height=600)
        container = col6.container(border=True, height=600)
        if selection_type == "Transactions":

            cursor.execute("SELECT * FROM transaction_map")
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns = ["State", "Year", "Quarter", "District", "Transaction Count", "Transaction Amount"])
            year = col2.selectbox("Year", result_df["Year"].unique())
            quarter = col3.selectbox("Quarter", result_df["Quarter"].unique())
            fig = plot_map_transaction(result_df, year, quarter)
            container_map.plotly_chart(fig, use_container_width=True)

            cursor.execute("SELECT * FROM aggregated_transaction")
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns = ["State", "Year", "Quarter", "Categories", "Count", "Amount"])
            agg_transaction_selected_df, agg_categories_transaction_selected_df = plot_agg_transaction(result_df, year, quarter)
            all_count = agg_transaction_selected_df["Count"].values[0]
            total_payment = agg_transaction_selected_df["Amount"].values[0]
            container.subheader(f":green[Total Transaction Count] {all_count}")
            container.subheader(f":green[Total Payment Value] {total_payment}")
            container.write("")
            container.write("")
            container.markdown(":red[__________________________________________________]")
            container.write("")
            container.write("")
            container.dataframe(agg_categories_transaction_selected_df, hide_index=True)
        if selection_type == "Users":
            cursor.execute("SELECT * FROM user_map")
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns = ["State", "Year", "Quarter", "District", "Registred Users", "App Opens"])
            year = col2.selectbox("Year", result_df["Year"].unique())
            quarter = col3.selectbox("Quarter", result_df["Quarter"].unique())
            fig, agg_user_selected_df = plot_map_user(result_df, year, quarter)
            container_map.plotly_chart(fig, use_container_width=True)

            all_registered_users = agg_user_selected_df["Registred Users"].values[0]
            total_app_opens = agg_user_selected_df["App Opens"].values[0]
            container.subheader(f":green[Total Phonepe Registered Users] {all_registered_users}")
            container.subheader(f":green[Total Phonepe App Opens] {total_app_opens}")
            container.write("")
            container.write("")
            container.markdown(":red[_________________________________________________]")
            container.write("")
            container.write("")

            cursor.execute("SELECT * FROM aggregated_user")
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns = ["State", "Year", "Quarter", "Brand", "Count", "Percentage"])
            agg_brand_user_selected_df = plot_agg_user(result_df, year, quarter)
            container.dataframe(agg_brand_user_selected_df, use_container_width=True, hide_index=True)
