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
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},geo=dict(bgcolor='#002b2b'),height=700, width=1300)
    return fig, map_transaction_selected_df

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
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},geo=dict(bgcolor='#002b2b'),height=700, width=1300)

    agg_user_df = result_df.groupby(["Year", "Quarter"]).agg({'Registred Users': 'sum', 'App Opens': 'sum'}).reset_index()
    agg_user_df["App Opens"] = agg_user_df["App Opens"].replace({0 : "Unavailable"})
    agg_user_selected_df = agg_user_df[(agg_user_df["Year"] == year) & (agg_user_df["Quarter"] == quarter)]
    return fig, agg_user_selected_df, map_user_selected_df

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
                            options=['Home','Explore', 'Q & A'],
                            icons=['house', 'map','bar-chart-line'],
                            menu_icon="pin-map-fill",
                            default_index=0 ,
                            styles={"container": {"padding": "5!important"},
                                    "icon": {"color": "brown", "font-size": "23px"}, 
                                    "nav-link": {"color":"white","font-size": "20px", "text-align": "left", "margin":"0px", "--hover-color": "lightblue"},
                                    "nav-link-selected": {"background-color": "grey"},}  
                        )
    if page == "Home":
        
        st.header(':green[Phonepe Pulse Data Visualization and Exploration] :world_map:')
        st.write("")
        st.subheader(":orange[Application Features :]")
        st.subheader(":one: :grey[_Extract data from the Phonepe pulse Github repository through scripting_.]")
        st.subheader(":two: :grey[_Transform the data into a dataframe and perform cleaning and pre-processing steps_.]")
        st.subheader(":three: :grey[_Insert the transformed data into a MySQL database for efficient storage and retrieval_.]")
        st.subheader(":four: :grey[_Create a live geo visualization dashboard using Streamlit and Plotly_.]")
        st.subheader(":four: :grey[_Querying the data from the SQL database and display meaningful insights about the data to the user_.]")

    if page == "Explore":

        col1, col2, col3, col4 = st.columns([1,1,1,5])
        col5, col6 = st.columns([3,1])
        selection_type = col1.selectbox("Transactions/Users",["Transactions", "Users"])
        container_map = col5.container(border=False, height=700)
        container = col6.container(border=True, height=700)
        tab1, tab2 = container.tabs(["Aggregate", "Top 10"])
        tab3, tab4, tab5 = tab2.tabs([":blue[State]", ":orange[District]", ":violet[Pincode]"])
        tab3.write("")
        tab4.write("")
        tab5.write("")
        if selection_type == "Transactions":

            cursor.execute("SELECT * FROM transaction_map")
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns = ["State", "Year", "Quarter", "District", "Transaction Count", "Transaction Amount"])
            year = col2.selectbox("Year", result_df["Year"].unique())
            quarter = col3.selectbox("Quarter", result_df["Quarter"].unique())
            fig, map_transaction_selected_df = plot_map_transaction(result_df, year, quarter)
            container_map.plotly_chart(fig, use_container_width=True)

            cursor.execute("SELECT * FROM aggregated_transaction")
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns = ["State", "Year", "Quarter", "Categories", "Count", "Amount"])
            agg_transaction_selected_df, agg_categories_transaction_selected_df = plot_agg_transaction(result_df, year, quarter)
            all_count = agg_transaction_selected_df["Count"].values[0]
            total_payment = agg_transaction_selected_df["Amount"].values[0]
            tab1.subheader(f":green[Total Transaction Count] {all_count}")
            tab1.subheader(f":green[Total Payment Value] {total_payment}")
            tab1.write("")
            tab1.write("")
            tab1.markdown(":red[_________________________________________________]")
            tab1.write("")
            tab1.write("")
            tab1.dataframe(agg_categories_transaction_selected_df, hide_index=True)


            map_transaction_selected_df = map_transaction_selected_df.sort_values(by='Transaction Count', ascending=False).head(10)
            tab3.dataframe(map_transaction_selected_df,column_order=("State", "Transaction Count"), use_container_width=True, hide_index=True)

            sql = "SELECT * FROM top_transaction_district WHERE Year = %s and Quarter = %s ORDER BY Transaction_Count DESC Limit 10"
            cursor.execute(sql,(year,quarter))
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns= ["State", "Year", "Quarter", "District", "Transaction Count", "Transaction Amount"])
            top_transaction_district = result_df.loc[:, ['District', 'Transaction Count']]
            tab4.dataframe(top_transaction_district, use_container_width=True, hide_index=True)

            sql = "SELECT * FROM top_transaction_pincode WHERE Year = %s and Quarter = %s ORDER BY Transaction_Count DESC Limit 10"
            cursor.execute(sql,(year,quarter))
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns= ["State", "Year", "Quarter", "Pincode", "Transaction Count", "Transaction Amount"])
            top_transaction_pincode = result_df.loc[:, ['Pincode', 'Transaction Count']]
            tab5.dataframe(top_transaction_pincode, use_container_width=True, hide_index=True)

        if selection_type == "Users":
            cursor.execute("SELECT * FROM user_map")
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns = ["State", "Year", "Quarter", "District", "Registred Users", "App Opens"])
            year = col2.selectbox("Year", result_df["Year"].unique())
            quarter = col3.selectbox("Quarter", result_df["Quarter"].unique())
            fig, agg_user_selected_df, map_user_selected_df = plot_map_user(result_df, year, quarter)
            container_map.plotly_chart(fig, use_container_width=True)

            all_registered_users = agg_user_selected_df["Registred Users"].values[0]
            total_app_opens = agg_user_selected_df["App Opens"].values[0]
            tab1.subheader(f":green[Total Phonepe Registered Users] {all_registered_users}")
            tab1.subheader(f":green[Total Phonepe App Opens] {total_app_opens}")
            tab1.write("")
            tab1.markdown(":red[_________________________________________________]")
            tab1.write("")
            tab1.write("")
            cursor.execute("SELECT * FROM aggregated_user")
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns = ["State", "Year", "Quarter", "Brand", "Count", "Percentage"])
            agg_brand_user_selected_df = plot_agg_user(result_df, year, quarter)
            tab1.dataframe(agg_brand_user_selected_df, use_container_width=True, hide_index=True)

            map_user_selected_df = map_user_selected_df.sort_values(by='Registred Users', ascending=False).head(10)
            tab3.dataframe(map_user_selected_df,column_order=("State", "Registred Users"), use_container_width=True, hide_index=True)

            sql = "SELECT * FROM top_user_district WHERE Year = %s and Quarter = %s ORDER BY Registered_Users DESC Limit 10"
            cursor.execute(sql,(year,quarter))
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns= ["State", "Year", "Quarter", "District", "Registered Users"])
            top_user_district = result_df.loc[:, ['District', 'Registered Users']]
            tab4.dataframe(top_user_district, use_container_width=True, hide_index=True)

            sql = "SELECT * FROM top_user_pincode WHERE Year = %s and Quarter = %s ORDER BY Registered_Users DESC Limit 10"
            cursor.execute(sql,(year,quarter))
            connection.commit()
            result = cursor.fetchall()
            result_df = pd.DataFrame(result, columns= ["State", "Year", "Quarter", "Pincode", "Registered Users"])
            top_user_pincode = result_df.loc[:, ['Pincode', 'Registered Users']]
            tab5.dataframe(top_user_pincode, use_container_width=True, hide_index=True)

    if page == "Q & A":
        
        st.header(":green[_Insights From the Phonepe Data_] :bulb:")

        col1, col2= st.columns([2,2])
        expander_1 = col1.expander("1). Which year has the most number of transactions?")
        cursor.execute("SELECT Year, SUM(Transaction_Count) AS Transaction_Count, SUM(Transaction_Amount) AS Transaction_Amount FROM transaction_map GROUP BY Year")
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["Year", "Transaction Count", "Transaction Amount"])
        fig = px.bar(result_df, x="Year", y="Transaction Count", hover_data=["Transaction Amount"], color="Transaction Amount", color_continuous_scale="temps", height=400, width=650)
        expander_1.plotly_chart(fig)

        expander_2 = col2.expander("2). Total number of registered users for each year?")
        cursor.execute("SELECT Year, SUM(Registered_Users) AS Registered_Users FROM user_map GROUP BY Year")
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["Year", "Users Registered"])
        fig = px.bar(result_df, x="Year", y="Users Registered",color="Users Registered", color_continuous_scale="haline", height=400, width=650)
        expander_2.plotly_chart(fig)

        expander_3 = col1.expander("3). Which district has the most users registered for each year?")
        sql = """SELECT t.Year, t.District, t.Registered_Users
                FROM (
                    SELECT Year, District, SUM(Registered_Users) AS Registered_Users
                    FROM top_user_district
                    GROUP BY Year, District
                ) AS t
                JOIN (
                    SELECT Year, MAX(Registered_Users) AS Max_Registered_Users
                    FROM (
                        SELECT Year, District, SUM(Registered_Users) AS Registered_Users
                        FROM top_user_district
                        GROUP BY Year, District
                    ) AS subquery
                    GROUP BY Year
                ) AS max_users
                ON t.Year = max_users.Year AND t.Registered_Users = max_users.Max_Registered_Users"""
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["Year", "District", "Users Registered"])
        fig = px.line(result_df, x="Year", y="Users Registered", hover_data=["District"], color_discrete_sequence=["lightseagreen"], height=400, width=650)
        expander_3.plotly_chart(fig)

        expander_4 = col2.expander("4). Which 5 states tops in the amount transacted through Phonepe?")
        sql = "SELECT State, SUM(Transaction_Amount) AS Transaction_Amount FROM transaction_map GROUP BY State ORDER BY Transaction_Amount DESC LIMIT 5"
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["State", "Transaction Amount"])
        fig = px.pie(result_df, values="Transaction Amount", names="State", hover_data=["Transaction Amount"], hole=0.5, height=400, width=650)
        expander_4.write(fig)

        expander_5 = col2.expander("5). Which transaction type have been done through Phonepe the most?")
        sql = "SELECT Transaction_Type, SUM(Transaction_Count) AS Transaction_Count FROM aggregated_transaction GROUP BY Transaction_Type ORDER BY Transaction_Count ASC"
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["Transaction Type", "Transaction Count"])
        fig = px.pie(result_df, values="Transaction Count", names="Transaction Type", hover_data=["Transaction Count"], height=400, width=650)
        colors = ['olivedrab', 'violet', 'slategrey', 'navajowhite', 'steelblue']
        fig.update_traces(marker=dict(colors=colors))
        expander_5.write(fig)

        expander_6 = col1.expander("6). Which quarter has the most number of transactions for each year?")
        sql = "SELECT Year, Quarter, SUM(Transaction_Count) AS Transaction_Count FROM transaction_map GROUP BY Year, Quarter"
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["Year", "Quarter", "Transaction Count"])
        result_df = result_df.groupby(["Year"]).apply(lambda x : x.loc[x["Transaction Count"].idxmax()])
        fig = px.bar(result_df, x="Year", y="Transaction Count", hover_data=["Quarter"], color_discrete_sequence=["saddlebrown"], height=400, width=650)
        expander_6.plotly_chart(fig)

        expander_7 = col1.expander("7). Which 5 states has the least amount transacted through Phonepe?")
        sql = "SELECT State, SUM(Transaction_Amount) AS Transaction_Amount FROM transaction_map GROUP BY State ORDER BY Transaction_Amount ASC LIMIT 5"
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["State", "Transaction Amount"])
        fig = px.pie(result_df, values="Transaction Amount", names="State", hover_data=["Transaction Amount"], color_discrete_sequence=px.colors.sequential.RdBu, height=400, width=650)
        expander_7.write(fig)

        expander_8 = col2.expander("8). Which district has the least users registered for each year?")
        sql = """SELECT t.Year, t.District, t.Registered_Users
                FROM (
                    SELECT Year, District, SUM(Registered_Users) AS Registered_Users
                    FROM top_user_district
                    GROUP BY Year, District
                ) AS t
                JOIN (
                    SELECT Year, MIN(Registered_Users) AS Min_Registered_Users
                    FROM (
                        SELECT Year, District, SUM(Registered_Users) AS Registered_Users
                        FROM top_user_district
                        GROUP BY Year, District
                    ) AS subquery
                    GROUP BY Year
                ) AS min_users
                ON t.Year = min_users.Year AND t.Registered_Users = min_users.Min_Registered_Users"""
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["Year", "District", "Users Registered"])
        fig = px.bar(result_df, x="Year", y="Users Registered", hover_data=["District"], color_discrete_sequence=["springgreen"], height=400, width=650)
        expander_8.plotly_chart(fig)

        expander_9 = col1.expander("9). Which 5 states transacted the least through Phonepe?")
        sql = "SELECT State, SUM(Transaction_Count) AS Transaction_Count FROM transaction_map GROUP BY State ORDER BY Transaction_Count ASC LIMIT 5"
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["State", "Transaction Count"])
        fig = px.pie(result_df, values="Transaction Count", names="State", hover_data=["Transaction Count"], height=400, width=650)
        colors = ['mediumorchid', 'gold', 'mediumturquoise', 'darkorange', 'lightgreen']
        fig.update_traces(marker=dict(colors=colors, pattern=dict(shape=[".", "x", "+", "-", "/"])))
        expander_9.write(fig)

        expander_10 = col2.expander("10). Which 5 states transacted the most through Phonepe?")
        sql = "SELECT State, SUM(Transaction_Count) AS Transaction_Count FROM transaction_map GROUP BY State ORDER BY Transaction_Count DESC LIMIT 5"
        cursor.execute(sql)
        connection.commit()
        result = cursor.fetchall()
        result_df = pd.DataFrame(result, columns=["State", "Transaction Count"])
        fig = px.pie(result_df, values="Transaction Count", names="State", hover_data=["Transaction Count"], hole=0.5, height=400, width=650)
        colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen', 'mediumorchid']
        fig.update_traces(marker=dict(colors=colors, line=dict(color='#000000', width=2)))
        expander_10.write(fig)