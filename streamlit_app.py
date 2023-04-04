import streamlit
import pandas
import requests
import snowflake.connector
from urllib.error import URLError

streamlit.title('Breakfast Favorites')

# Read fruits portions
my_fruit_list = pandas.read_csv("https://uni-lab-files.s3.us-west-2.amazonaws.com/dabw/fruit_macros.txt")

# Let's put a pick list here so they can pick the fruit they want to include 
fruits_selected = streamlit.multiselect("Pick some fruits:", list(my_fruit_list.index))
fruits_to_show = my_fruit_list.loc[fruits_selected]

# Display the table on the page.
streamlit.dataframe(fruits_to_show)

# Fruityvice API
streamlit.header("Fruityvice Fruit Advice!")

try:
  fruit_choice = streamlit.text_input('What fruit would you like information about?')
  if not fruit_choice:
    streamlit.error("please select a fruit")
  else:
    fruityvice_response = requests.get("https://fruityvice.com/api/fruit/"+ fruit_choice )
    fruityvice_normalized = pandas.json_normalize(fruityvice_response.json())
    streamlit.dataframe(fruityvice_normalized)
except URLError as e:
  streamlit.error()

#snowflake connection
streamlit.header("the fruit load list contains")
def get_fruit_list():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select * from pc_rivery_db.public.fruit_load_list")
    return my_cur.fetchall()
  
if streamlit.button('Get fruit list'):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  my_data_rows = get_fruit_list()
  streamlit.dataframe(my_data_rows)
  
streamlit.header("Add a new fruit")
def add_fruit(fruit_input):
  with my_cnx.cursor() as my_cur:
    query = f"select * from pc_rivery_db.public.fruit_load_list where FRUIT_NAME = '{fruit_input}'"
    my_cur.execute("select * from pc_rivery_db.public.fruit_load_list")
    if my_cur.execute(query).fetchone():
      return f"{fruit_input} record already exist"
    else:
      query_add = f"insert into PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST values ('{fruit_input}')"
      my_cur.execute(query_add)
      return f"thanks for adding {fruit_input}"

fruit = streamlit.text_input('What fruit would you like to add?')

if streamlit.button('Add a fruit'):
  my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
  result = add_fruit(fruit)
  streamlit.text(result)

  
