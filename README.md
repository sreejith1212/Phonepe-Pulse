# Phonepe Pulse Data Visualization and Exploration
Building a live geo visualization dashboard from the Phonepe pulse Github repository. Also the dashboard provides valuable insights and information about the data in the Phonepe pulse Github repository, making it a valuable tool for data analysis and decision-making.

## Pre-Requisite
1) Python: Install Python
2) MySQL: Install MySQL database server and client on your system.
3) Clone Phonepe pulse Github repository(https://github.com/PhonePe/pulse.git) into the project folder.

## Installation
1) Clone the repo, create and activate the environment for the project.
2) Install all required packages from requirements.txt file using command: "pip install -r requirements.txt"

## Usage
1) To extract the phonepe pulse repo data, run command: "python extract_phonepe_data.py"
2) To start the app, run command: "streamlit run visualization_phonepe.py"
3) To view the geo visualization and basic data info, go to "Explore" page.
4) Get general insights about the extracted data in the "Q & A" section.

## Features
1) Data extraction and processing: Extract data from the Phonepe pulse Github repository and using pandas transform the data into a dataframe and perform necessary pre-processing  steps.
2) Database management: Insert the transformed data into a MySQL database for efficient storage and retrieval.
3) Visualization and dashboard creation: Using Streamlit and Plotly libraries in Python to create an interactive and visually appealing dashboard. Plotly's built-in geo map functions can be used to display the data on a map and Streamlit can be used to create a user-friendly interface with multiple dropdown options for users to select different facts and figures to display.


