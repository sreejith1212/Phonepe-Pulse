
import os
from pulse.data import *
import json
import pandas as pd
import pymysql

# extract aggregated transaction data and save to database
def aggregated_transaction(connection, cursor):
    
    agg_transaction_dict = {"State":[], "Year":[],"Quarter":[],"Transaction Type":[], "Transaction Count":[], "Transaction Amount":[]}
    states_path="pulse/data/aggregated/transaction/country/india/state"
    agg_state_list=os.listdir(states_path)
    for state in agg_state_list:
        years_path = states_path + "/" + state
        agg_year_list = os.listdir(years_path)
        for year in agg_year_list:
            quaters_path = years_path + "/" + year
            agg_quater_list = os.listdir(quaters_path)
            for quarter in agg_quater_list:
                quarter_file = quaters_path + "/" + quarter
                open_agg_transaction_file = open(quarter_file, "r")
                agg_transaction_data = json.load(open_agg_transaction_file)
                if agg_transaction_data["data"]["transactionData"] is not None:
                    for data in agg_transaction_data["data"]["transactionData"]:
                        agg_transaction_dict["State"].append(state)
                        agg_transaction_dict["Year"].append(year)
                        agg_transaction_dict["Quarter"].append(quarter.strip(".json"))
                        try:
                            agg_transaction_dict["Transaction Type"].append(data["name"])
                            agg_transaction_dict["Transaction Count"].append(data["paymentInstruments"][0]["count"])
                            agg_transaction_dict["Transaction Amount"].append(data["paymentInstruments"][0]["amount"])
                        except KeyError:
                            agg_transaction_dict["Transaction Type"].append("")
                            agg_transaction_dict["Transaction Count"].append("")
                            agg_transaction_dict["Transaction Amount"].append("")

                else:
                    agg_transaction_dict["State"].append(state)
                    agg_transaction_dict["Year"].append(year)
                    agg_transaction_dict["Quarter"].append(quarter.strip(".json"))
                    agg_transaction_dict["Transaction Type"].append("")
                    agg_transaction_dict["Transaction Count"].append("")
                    agg_transaction_dict["Transaction Amount"].append("")

    df_agg_transaction = pd.DataFrame(agg_transaction_dict)
    df_agg_transaction["State"] = df_agg_transaction["State"].apply(lambda x : x.replace("-", " "))
    df_agg_transaction["State"] = df_agg_transaction["State"].apply(lambda x : x.title())
    df_agg_transaction["Quarter"] = df_agg_transaction["Quarter"].apply(lambda x : "Q" + x)
    df_agg_transaction['State'] = df_agg_transaction['State'].replace({'Andaman & Nicobar Islands': 'Andaman & Nicobar', 'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu'})

    for i, row in df_agg_transaction.iterrows():

        State = row["State"]
        Year = row["Year"]
        Quarter = row["Quarter"]
        Transaction_Type = row["Transaction Type"]
        Transaction_Count = row["Transaction Count"]
        Transaction_Amount = row["Transaction Amount"]
        sql = "INSERT INTO aggregated_transaction (State, Year, Quarter, Transaction_Type, Transaction_Count, Transaction_Amount) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (State, Year, Quarter, Transaction_Type, Transaction_Count, Transaction_Amount)
        cursor.execute(sql, val)
        connection.commit()

# extract aggregated user data and save to database
def aggregated_user(connection, cursor):

    agg_user_dict = {"State":[], "Year":[],"Quarter":[],"Brand":[], "Count":[], "Percentage":[]}
    states_path="pulse/data/aggregated/user/country/india/state"
    agg_state_list=os.listdir(states_path)
    for state in agg_state_list:
        years_path = states_path + "/" + state
        agg_year_list = os.listdir(years_path)
        for year in agg_year_list:
            quaters_path = years_path + "/" + year
            agg_quater_list = os.listdir(quaters_path)
            for quarter in agg_quater_list:
                quarter_file = quaters_path + "/" + quarter
                open_agg_user_file = open(quarter_file, "r")
                agg_user_data = json.load(open_agg_user_file)
                if agg_user_data["data"]["usersByDevice"] is not None:
                    for data in agg_user_data["data"]["usersByDevice"]:
                        agg_user_dict["State"].append(state)
                        agg_user_dict["Year"].append(year)
                        agg_user_dict["Quarter"].append(quarter.strip(".json"))
                        try:
                            agg_user_dict["Brand"].append(data["brand"])
                            agg_user_dict["Count"].append(data["count"])
                            agg_user_dict["Percentage"].append(data["percentage"])
                        except KeyError:
                            agg_user_dict["Brand"].append("")
                            agg_user_dict["Count"].append("")
                            agg_user_dict["Percentage"].append("")
                else:
                    agg_user_dict["State"].append(state)
                    agg_user_dict["Year"].append(year)
                    agg_user_dict["Quarter"].append(quarter.strip(".json"))    
                    agg_user_dict["Brand"].append("")
                    agg_user_dict["Count"].append("")
                    agg_user_dict["Percentage"].append("")                 

    df_agg_user = pd.DataFrame(agg_user_dict)
    df_agg_user = df_agg_user[df_agg_user["Brand"] != ""]
    df_agg_user["Count"] = df_agg_user["Count"].astype(int)
    df_agg_user["Percentage"] = df_agg_user["Percentage"].astype(float)
    df_agg_user["State"] = df_agg_user["State"].apply(lambda x : x.replace("-", " "))
    df_agg_user["State"] = df_agg_user["State"].apply(lambda x : x.title())
    df_agg_user["Quarter"] = df_agg_user["Quarter"].apply(lambda x : "Q" + x)
    df_agg_user['State'] = df_agg_user['State'].replace({'Andaman & Nicobar Islands': 'Andaman & Nicobar', 'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu'})    

    for i, row in df_agg_user.iterrows():

        State = row["State"]
        Year = row["Year"]
        Quarter = row["Quarter"]
        Brand = row["Brand"]
        Count = row["Count"]
        Percentage = row["Percentage"]
        sql = "INSERT INTO aggregated_user (State, Year, Quarter, Brand, Count, Percentage) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (State, Year, Quarter, Brand, Count, Percentage)
        cursor.execute(sql, val)
        connection.commit()

# extract total transaction data at the State and District levels and save to database
def transaction_map(connection, cursor):
  
    map_transaction_dict = {"State":[], "Year":[], "Quarter":[], "District":[], "Transaction Count":[], "Transaction Amount":[]}
    states_path="pulse/data/map/transaction/hover/country/india/state"
    map_state_list=os.listdir(states_path)
    for state in map_state_list:
        years_path = states_path + "/" + state
        map_year_list = os.listdir(years_path)
        for year in map_year_list:
            quaters_path = years_path + "/" + year
            map_quater_list = os.listdir(quaters_path)
            for quarter in map_quater_list:
                quarter_file = quaters_path + "/" + quarter
                open_map_transaction_file = open(quarter_file, "r")
                map_transaction_data = json.load(open_map_transaction_file)
                if map_transaction_data["data"]["hoverDataList"] is not None:
                    for data in map_transaction_data["data"]["hoverDataList"]:
                        map_transaction_dict["State"].append(state)
                        map_transaction_dict["Year"].append(year)
                        map_transaction_dict["Quarter"].append(quarter.strip(".json"))
                        try:
                            map_transaction_dict["District"].append(data["name"])
                            map_transaction_dict["Transaction Count"].append(data["metric"][0]["count"])
                            map_transaction_dict["Transaction Amount"].append(data["metric"][0]["amount"])
                        except KeyError:
                            map_transaction_dict["District"].append("")
                            map_transaction_dict["Transaction Count"].append("")
                            map_transaction_dict["Transaction Amount"].append("")

                else:
                    map_transaction_dict["State"].append(state)
                    map_transaction_dict["Year"].append(year)
                    map_transaction_dict["Quarter"].append(quarter.strip(".json"))
                    map_transaction_dict["District"].append("")
                    map_transaction_dict["Transaction Count"].append("")
                    map_transaction_dict["Transaction Amount"].append("")

    df_map_transaction = pd.DataFrame(map_transaction_dict)
    df_map_transaction["State"] = df_map_transaction["State"].apply(lambda x : x.replace("-", " "))
    df_map_transaction["State"] = df_map_transaction["State"].apply(lambda x : x.title())
    df_map_transaction["District"] = df_map_transaction["District"].apply(lambda x : x.title())
    df_map_transaction["Quarter"] = df_map_transaction["Quarter"].apply(lambda x : "Q" + x)
    df_map_transaction['State'] = df_map_transaction['State'].replace({'Andaman & Nicobar Islands': 'Andaman & Nicobar', 'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu'})    


    for i, row in df_map_transaction.iterrows():

        State = row["State"]
        Year = row["Year"]
        Quarter = row["Quarter"]
        District = row["District"]
        Transaction_Count = row["Transaction Count"]
        Transaction_Amount = row["Transaction Amount"]
        sql = "INSERT INTO transaction_map (State, Year, Quarter, District, Transaction_Count, Transaction_Amount) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (State, Year, Quarter, District, Transaction_Count, Transaction_Amount)
        cursor.execute(sql, val)
        connection.commit()

# extract total user data at the State and District levels and save to database
def user_map(connection, cursor):
    
    map_user_dict = {"State":[], "Year":[],"Quarter":[],"District":[], "Registered Users":[], "App Opens":[]}
    states_path="pulse/data/map/user/hover/country/india/state"
    map_state_list=os.listdir(states_path)
    for state in map_state_list:
        years_path = states_path + "/" + state
        map_year_list = os.listdir(years_path)
        for year in map_year_list:
            quaters_path = years_path + "/" + year
            map_quater_list = os.listdir(quaters_path)
            for quarter in map_quater_list:
                quarter_file = quaters_path + "/" + quarter
                open_map_user_file = open(quarter_file, "r")
                map_user_data = json.load(open_map_user_file)
                if map_user_data["data"]["hoverData"] is not None:
                    for dis, val in map_user_data["data"]["hoverData"].items():
                        map_user_dict["State"].append(state)
                        map_user_dict["Year"].append(year)
                        map_user_dict["Quarter"].append(quarter.strip(".json"))
                        try:
                            map_user_dict["District"].append(dis)
                            map_user_dict["Registered Users"].append(val["registeredUsers"])
                            map_user_dict["App Opens"].append(val["appOpens"])
                        except KeyError:
                            map_user_dict["District"].append("")
                            map_user_dict["Registered Users"].append("")
                            map_user_dict["App Opens"].append("")
                else:
                    map_user_dict["State"].append(state)
                    map_user_dict["Year"].append(year)
                    map_user_dict["Quarter"].append(quarter.strip(".json"))    
                    map_user_dict["District"].append("")
                    map_user_dict["Registered Users"].append("")
                    map_user_dict["App Opens"].append("")              

    df_map_user = pd.DataFrame(map_user_dict)
    df_map_user["State"] = df_map_user["State"].apply(lambda x : x.replace("-", " "))
    df_map_user["State"] = df_map_user["State"].apply(lambda x : x.title())
    df_map_user["District"] = df_map_user["District"].apply(lambda x : x.title())
    df_map_user["Quarter"] = df_map_user["Quarter"].apply(lambda x : "Q" + x)
    df_map_user['State'] = df_map_user['State'].replace({'Andaman & Nicobar Islands': 'Andaman & Nicobar', 'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu'})    


    for i, row in df_map_user.iterrows():

        State = row["State"]
        Year = row["Year"]
        Quarter = row["Quarter"]
        District = row["District"]
        Registered_Users = row["Registered Users"]
        App_Opens = row["App Opens"]
        sql = "INSERT INTO user_map (State, Year, Quarter, District, Registered_Users, App_Opens) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (State, Year, Quarter, District, Registered_Users, App_Opens)
        cursor.execute(sql, val)
        connection.commit()

# extract top transaction data and save to database
def top_transaction(connection, cursor):

    top_transaction_district_dict = {"State":[], "Year":[],"Quarter":[],"District":[], "Transaction Count":[], "Transaction Amount":[]}
    top_transaction_pincode_dict = {"State":[], "Year":[],"Quarter":[],"Pincode":[], "Transaction Count":[], "Transaction Amount":[]}
    states_path="pulse/data/top/transaction/country/india/state"
    top_state_list=os.listdir(states_path)
    for state in top_state_list:
        years_path = states_path + "/" + state
        top_year_list = os.listdir(years_path)
        for year in top_year_list:
            quaters_path = years_path + "/" + year
            top_quater_list = os.listdir(quaters_path)
            for quarter in top_quater_list:
                quarter_file = quaters_path + "/" + quarter
                open_top_transaction_file = open(quarter_file, "r")
                top_transaction_data = json.load(open_top_transaction_file)
                if top_transaction_data["data"]["districts"] is not None:
                    for data in top_transaction_data["data"]["districts"]:
                        top_transaction_district_dict["State"].append(state)
                        top_transaction_district_dict["Year"].append(year)
                        top_transaction_district_dict["Quarter"].append(quarter.strip(".json"))
                        try:
                            top_transaction_district_dict["District"].append(data["entityName"])
                            top_transaction_district_dict["Transaction Count"].append(data["metric"]["count"])
                            top_transaction_district_dict["Transaction Amount"].append(data["metric"]["amount"])
                        except KeyError:
                            top_transaction_district_dict["District"].append("")
                            top_transaction_district_dict["Transaction Count"].append("")
                            top_transaction_district_dict["Transaction Amount"].append("")

                else:
                    top_transaction_district_dict["State"].append(state)
                    top_transaction_district_dict["Year"].append(year)
                    top_transaction_district_dict["Quarter"].append(quarter.strip(".json"))
                    top_transaction_district_dict["District"].append("")
                    top_transaction_district_dict["Transaction Count"].append("")
                    top_transaction_district_dict["Transaction Amount"].append("")

                if top_transaction_data["data"]["pincodes"] is not None:
                    for data in top_transaction_data["data"]["pincodes"]:
                        top_transaction_pincode_dict["State"].append(state)
                        top_transaction_pincode_dict["Year"].append(year)
                        top_transaction_pincode_dict["Quarter"].append(quarter.strip(".json"))
                        try:
                            top_transaction_pincode_dict["Pincode"].append(data["entityName"])
                            top_transaction_pincode_dict["Transaction Count"].append(data["metric"]["count"])
                            top_transaction_pincode_dict["Transaction Amount"].append(data["metric"]["amount"])
                        except KeyError:
                            top_transaction_pincode_dict["Pincode"].append("")
                            top_transaction_pincode_dict["Transaction Count"].append("")
                            top_transaction_pincode_dict["Transaction Amount"].append("")

                else:
                    top_transaction_pincode_dict["State"].append(state)
                    top_transaction_pincode_dict["Year"].append(year)
                    top_transaction_pincode_dict["Quarter"].append(quarter.strip(".json"))
                    top_transaction_pincode_dict["Pincode"].append("")
                    top_transaction_pincode_dict["Transaction Count"].append("")
                    top_transaction_pincode_dict["Transaction Amount"].append("")

    df_top_district_transaction = pd.DataFrame(top_transaction_district_dict)
    df_top_pincode_transaction = pd.DataFrame(top_transaction_pincode_dict)

    df_top_district_transaction["State"] = df_top_district_transaction["State"].apply(lambda x : x.replace("-", " "))
    df_top_district_transaction["State"] = df_top_district_transaction["State"].apply(lambda x : x.title())
    df_top_district_transaction["District"] = df_top_district_transaction["District"].apply(lambda x : x.title())
    df_top_district_transaction["Quarter"] = df_top_district_transaction["Quarter"].apply(lambda x : "Q" + x)
    df_top_district_transaction['State'] = df_top_district_transaction['State'].replace({'Andaman & Nicobar Islands': 'Andaman & Nicobar', 'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu'})

    df_top_pincode_transaction["State"] = df_top_pincode_transaction["State"].apply(lambda x : x.replace("-", " "))
    df_top_pincode_transaction["State"] = df_top_pincode_transaction["State"].apply(lambda x : x.title())
    df_top_pincode_transaction["Quarter"] = df_top_pincode_transaction["Quarter"].apply(lambda x : "Q" + x)
    df_top_pincode_transaction['State'] = df_top_pincode_transaction['State'].replace({'Andaman & Nicobar Islands': 'Andaman & Nicobar', 'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu'})    


    for i, row in df_top_district_transaction.iterrows():

        State = row["State"]
        Year = row["Year"]
        Quarter = row["Quarter"]
        District = row["District"]
        Transaction_Count = row["Transaction Count"]
        Transaction_Amount = row["Transaction Amount"]
        sql = "INSERT INTO top_transaction_district (State, Year, Quarter, District, Transaction_Count, Transaction_Amount) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (State, Year, Quarter, District, Transaction_Count, Transaction_Amount)
        cursor.execute(sql, val)
        connection.commit()

    for i, row in df_top_pincode_transaction.iterrows():

        State = row["State"]
        Year = row["Year"]
        Quarter = row["Quarter"]
        Pincode = row["Pincode"]
        Transaction_Count = row["Transaction Count"]
        Transaction_Amount = row["Transaction Amount"]
        sql = "INSERT INTO top_transaction_pincode (State, Year, Quarter, Pincode, Transaction_Count, Transaction_Amount) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (State, Year, Quarter, Pincode, Transaction_Count, Transaction_Amount)
        cursor.execute(sql, val)
        connection.commit()

# extract top user data and save to database
def top_user(connection, cursor):

    top_user_district_dict = {"State":[], "Year":[],"Quarter":[],"District":[], "Registered Users":[]}
    top_user_pincode_dict = {"State":[], "Year":[],"Quarter":[],"Pincode":[], "Registered Users":[]}
    states_path="pulse/data/top/user/country/india/state"
    top_state_list=os.listdir(states_path)
    for state in top_state_list:
        years_path = states_path + "/" + state
        top_year_list = os.listdir(years_path)
        for year in top_year_list:
            quaters_path = years_path + "/" + year
            top_quater_list = os.listdir(quaters_path)
            for quarter in top_quater_list:
                quarter_file = quaters_path + "/" + quarter
                open_top_user_file = open(quarter_file, "r")
                top_user_data = json.load(open_top_user_file)
                if top_user_data["data"]["districts"] is not None:
                    for data in top_user_data["data"]["districts"]:
                        top_user_district_dict["State"].append(state)
                        top_user_district_dict["Year"].append(year)
                        top_user_district_dict["Quarter"].append(quarter.strip(".json"))
                        try:
                            top_user_district_dict["District"].append(data["name"])
                            top_user_district_dict["Registered Users"].append(data["registeredUsers"])
                        except KeyError:
                            top_user_district_dict["District"].append("")
                            top_user_district_dict["Registered Users"].append("")
                else:
                    top_user_district_dict["State"].append(state)
                    top_user_district_dict["Year"].append(year)
                    top_user_district_dict["Quarter"].append(quarter.strip(".json"))    
                    top_user_district_dict["District"].append("")
                    top_user_district_dict["Registered Users"].append("")

                if top_user_data["data"]["pincodes"] is not None:
                    for data in top_user_data["data"]["pincodes"]:
                        top_user_pincode_dict["State"].append(state)
                        top_user_pincode_dict["Year"].append(year)
                        top_user_pincode_dict["Quarter"].append(quarter.strip(".json"))
                        try:
                            top_user_pincode_dict["Pincode"].append(data["name"])
                            top_user_pincode_dict["Registered Users"].append(data["registeredUsers"])
                        except KeyError:
                            top_user_pincode_dict["Pincode"].append("")
                            top_user_pincode_dict["Registered Users"].append("")
                else:
                    top_user_pincode_dict["State"].append(state)
                    top_user_pincode_dict["Year"].append(year)
                    top_user_pincode_dict["Quarter"].append(quarter.strip(".json"))    
                    top_user_pincode_dict["Pincode"].append("")
                    top_user_pincode_dict["Registered Users"].append("")

    df_top_district_user = pd.DataFrame(top_user_district_dict)
    df_top_pincode_user = pd.DataFrame(top_user_pincode_dict)

    df_top_district_user["State"] = df_top_district_user["State"].apply(lambda x : x.replace("-", " "))
    df_top_district_user["State"] = df_top_district_user["State"].apply(lambda x : x.title())
    df_top_district_user["District"] = df_top_district_user["District"].apply(lambda x : x.title())
    df_top_district_user["Quarter"] = df_top_district_user["Quarter"].apply(lambda x : "Q" + x)
    df_top_district_user['State'] = df_top_district_user['State'].replace({'Andaman & Nicobar Islands': 'Andaman & Nicobar', 'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu'})    

    df_top_pincode_user["State"] = df_top_pincode_user["State"].apply(lambda x : x.replace("-", " "))
    df_top_pincode_user["State"] = df_top_pincode_user["State"].apply(lambda x : x.title())
    df_top_pincode_user["Quarter"] = df_top_pincode_user["Quarter"].apply(lambda x : "Q" + x)
    df_top_pincode_user['State'] = df_top_pincode_user['State'].replace({'Andaman & Nicobar Islands': 'Andaman & Nicobar', 'Dadra & Nagar Haveli & Daman & Diu': 'Dadra and Nagar Haveli and Daman and Diu'})    

    for i, row in df_top_district_user.iterrows():

        State = row["State"]
        Year = row["Year"]
        Quarter = row["Quarter"]
        District = row["District"]
        Registered_Users = row["Registered Users"]
        sql = "INSERT INTO top_user_district (State, Year, Quarter, District, Registered_Users) VALUES (%s, %s, %s, %s, %s)"
        val = (State, Year, Quarter, District, Registered_Users)
        cursor.execute(sql, val)
        connection.commit()

    for i, row in df_top_pincode_user.iterrows():

        State = row["State"]
        Year = row["Year"]
        Quarter = row["Quarter"]
        Pincode = row["Pincode"]
        Registered_Users = row["Registered Users"]
        sql = "INSERT INTO top_user_pincode (State, Year, Quarter, Pincode, Registered_Users) VALUES (%s, %s, %s, %s, %s)"
        val = (State, Year, Quarter, Pincode, Registered_Users)
        cursor.execute(sql, val)
        connection.commit()

# delete existing data from the database
def delete_sql_data(connection, cursor):
   
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    for table in tables:
        cursor.execute("SELECT * FROM {}".format(table[0]))
        results = cursor.fetchall()
        if len(results) > 0:
            cursor.execute("DELETE FROM {}".format(table[0]))
            connection.commit()

def extract_data(connection, cursor):
    
    delete_sql_data(connection, cursor)
    aggregated_transaction(connection, cursor)
    aggregated_user(connection, cursor)
    transaction_map(connection, cursor)
    user_map(connection, cursor)
    top_transaction(connection, cursor)
    top_user(connection, cursor)

# create database tables
def create_mysql_tables(cursor):

    cursor.execute("SHOW TABLES")
    tables_sql = []
    for table in cursor:
        tables_sql.append(table[0])

    if "aggregated_transaction" not in tables_sql:
        cursor.execute("CREATE TABLE aggregated_transaction ( "
                        "State VARCHAR(100),"
                        "Year VARCHAR(10),"
                        "Quarter VARCHAR(20),"
                        "Transaction_Type VARCHAR(50),"
                        "Transaction_Count INT,"
                        "Transaction_Amount FLOAT )")
    if "aggregated_user" not in tables_sql:
        cursor.execute("CREATE TABLE aggregated_user ( "
                        "State VARCHAR(100),"
                        "Year VARCHAR(10),"
                        "Quarter VARCHAR(20),"
                        "Brand VARCHAR(50),"
                        "Count INT,"
                        "Percentage FLOAT )")
    if "transaction_map" not in tables_sql:
        cursor.execute("CREATE TABLE transaction_map ( "
                        "State VARCHAR(100),"
                        "Year VARCHAR(10),"
                        "Quarter VARCHAR(20),"
                        "District VARCHAR(100),"
                        "Transaction_Count INT,"
                        "Transaction_Amount FLOAT )")
    if "user_map" not in tables_sql:
        cursor.execute("CREATE TABLE user_map ( "
                        "State VARCHAR(100),"
                        "Year VARCHAR(10),"
                        "Quarter VARCHAR(20),"
                        "District VARCHAR(100),"
                        "Registered_Users INT,"
                        "App_Opens INT )")
    if "top_transaction_district" not in tables_sql:
        cursor.execute("CREATE TABLE top_transaction_district ( "
                        "State VARCHAR(100),"
                        "Year VARCHAR(10),"
                        "Quarter VARCHAR(20),"
                        "District VARCHAR(100),"
                        "Transaction_Count INT,"
                        "Transaction_Amount FLOAT )")
    if "top_transaction_pincode" not in tables_sql:
        cursor.execute("CREATE TABLE top_transaction_pincode ( "
                        "State VARCHAR(100),"
                        "Year VARCHAR(10),"
                        "Quarter VARCHAR(20),"
                        "Pincode VARCHAR(50),"
                        "Transaction_Count INT,"
                        "Transaction_Amount FLOAT )")
    if "top_user_district" not in tables_sql:
        cursor.execute("CREATE TABLE top_user_district ( "
                        "State VARCHAR(100),"
                        "Year VARCHAR(10),"
                        "Quarter VARCHAR(20),"
                        "District VARCHAR(100),"
                        "Registered_Users INT )")
    if "top_user_pincode" not in tables_sql:
        cursor.execute("CREATE TABLE top_user_pincode ( "
                        "State VARCHAR(100),"
                        "Year VARCHAR(10),"
                        "Quarter VARCHAR(20),"
                        "Pincode VARCHAR(50),"
                        "Registered_Users INT )")


if __name__ == "__main__":

    host = 'localhost' 
    user = 'root' 
    password = '12121995' 
    dbname = 'Phonepe_Pulse'

    # connect to required sql database, if the database does not exist, create the database and connect to it
    try:
        connection = pymysql.connect(host=host, user=user, password=password, db=dbname)
        cursor = connection.cursor()
    except Exception as e:
        connection = pymysql.connect(host=host, user=user, password=password)
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE {}".format(dbname))
        connection = pymysql.connect(host=host, user=user, password=password, db=dbname)
        cursor = connection.cursor()
    
    create_mysql_tables(cursor)

    # extract relevant data from the phonepe pulse repo
    extract_data(connection,cursor)