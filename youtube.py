from sqlite3 import Connection
from tkinter import YES
from PIL import Image
import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import mysql.connector
from mysql.connector import Error
import boto3
import pandas as pd
from sqlalchemy import create_engine
import tkinter



#api funtion
def api_func():
    api_name = "youtube"
    api_key = "AIzaSyBaX3xrdwcIxkBXw3rgaGtIjL27NxyD22g"
    api_version = "v3"

    youtube = build(api_name,api_version,developerKey=api_key)
    return youtube 

youtube = api_func()

# get channel details
def channel_func(given1_channel_id):
  request = youtube.channels().list(
         id= given1_channel_id,
         part="snippet,contentDetails,statistics"               
  )
  response = request.execute()

  channel_id = response["items"][0]["id"]
  channel_name = response["items"][0]["snippet"]["title"]
  subscription_count = response["items"][0]["statistics"]["subscriberCount"]
  channel_views = response["items"][0]["statistics"]["viewCount"]
  channel_discription = response["items"][0]["snippet"]["description"]
  video_count = response["items"][0]["statistics"]["videoCount"]
  playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

  return channel_name, channel_id, channel_views, subscription_count, video_count, playlist_id, channel_discription

# upload the all informations in mysql 
        
# Connect to the MySQL database
def create_server_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host = "localhost" , #"youtubedb.c96m2ysgag7t.eu-north-1.rds.amazonaws.com"
            database = "practice" , # "youtubedb"
            user = "root" , #"admin"
            password = "root" , # "nambukeerthi"
            port = "3306"
        
        )
    except Error as err:
        print(f"Error: '{err}'")

    return connection

# excution funtion
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        #print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

# excution 2 funtion
def execute_querys(connection, query1,query2):
    cursor = connection.cursor()
    try:
        cursor.execute(query1,query2)
        connection.commit()
        #print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

#table creation 

def create_tables(): 

            use_database = """(USE youtubedb);"""  

            create_channel_table = """
                    CREATE TABLE channel (
                                    id VARCHAR(50),  
                                    name VARCHAR(40),
                                    views INT,
                                    videoscount INT,
                                    subscriptioncount INT,
                                    discribtion VARCHAR(100)    

                                ); """

            create_playlist_table = """
                    CREATE TABLE playlist (  
                                    channelid VARCHAR(50), 
                                    playlistid VARCHAR(50),                       
                                    playlistname VARCHAR(50)
                                                        
                                    ); """

            create_videos_table = """
                    CREATE TABLE videos (
                                    id VARCHAR(50),  
                                    videoname VARCHAR(100),
                                    publisheddate TIMESTAMP,
                                    views INT,
                                    likes INT,
                                    channelid VARCHAR(50),                        
                                    commentcount INT
                                ); """

            create_comments_table = """
                    CREATE TABLE comments (
                                    commentid VARCHAR(50),  
                                    videosid VARCHAR(50),
                                    text VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, 
                                    commentauthor VARCHAR(50)
                                    
                                ); ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci """
            
            connection = create_server_connection()
            execute_query(connection, use_database)

            connection = create_server_connection()
            execute_query(connection, create_channel_table)

            connection = create_server_connection()
            execute_query(connection, create_playlist_table)

            connection = create_server_connection()
            execute_query(connection, create_videos_table)

            connection = create_server_connection()
            execute_query(connection, create_comments_table)

def drop_tables():
            drop_channel_query = "DROP TABLE IF EXISTS channel)"
            drop_videos_query = "DROP TABLE IF EXISTS playlist "
            drop_comments_query = "DROP TABLE IF EXISTS videos "
            drop_playlist_query = "DROP TABLE IF EXISTS comments " 

            connection = create_server_connection()
            execute_query(connection, drop_comments_query)
            
            connection = create_server_connection()
            execute_query(connection, drop_playlist_query)
            
            connection = create_server_connection()
            execute_query(connection, drop_videos_query)
            
            connection = create_server_connection()
            execute_query(connection, drop_channel_query)


# upload details
def upload_func(given_channel_id):
#get channel details
            request = youtube.channels().list(
            id= given_channel_id,
            part="snippet,contentDetails,statistics",  
            maxResults = 10,            
            )
 
            response = request.execute()
            #for i in range(len(response["items"])):
            channel_id = response["items"][0]["id"]
            channel_name = response["items"][0]["snippet"]["title"]
            subscription_count = response["items"][0]["statistics"]["subscriberCount"]
            channel_views = response["items"][0]["statistics"]["viewCount"]
            channel_discription = response["items"][0]["snippet"]["description"]
            video_count = response["items"][0]["statistics"]["videoCount"]

#insert query
            insert_channel_query = "INSERT INTO channel ( id,name,views,videoscount,subscriptioncount,discribtion ) VALUES (%s,%s,%s,%s,%s,LEFT(%s,100))"
            insert_channel_values = (channel_id,channel_name,channel_views,video_count,subscription_count,channel_discription)

            connection = create_server_connection()
            execute_querys(connection, insert_channel_query,insert_channel_values )  
#get playlist details
#requst1 
#response1
            next_page_token = None
            playlist_ids =[]
            playlist_titles = []
            channel_ids =[]
            request1 = youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId= channel_id,  
                        maxResults=10,  
                        pageToken= next_page_token  
            )
            response1 = request1.execute()

            for i in range(len(response1["items"])):
                  playlist_ids.append(response1["items"][i]["id"] )
                  playlist_titles.append(response1["items"][i]["snippet"]["title"])
                  channel_ids.append(response1["items"][i]["snippet"]["channelId"])

#insert query
            for i in range(len(channel_ids)): 
                  insert_playlist_query = "INSERT INTO playlist( channelid,playlistid,playlistname ) VALUES (%s,%s,LEFT(%s,50))"
                  insert_playlist_values = (channel_ids[i], playlist_ids[i], playlist_titles[i])

                  connection = create_server_connection()
                  execute_querys(connection, insert_playlist_query, insert_playlist_values)
#get video ID details
#request2
#response2
            video_ids=[]
            for i in range (len(playlist_ids)):
              request2 = youtube.playlistItems().list(
                    part= "snippet",
                    playlistId= playlist_ids[i], 
                    maxResults = 5,           
               )
              response2 = request2.execute()

              for i in range(len(response2["items"])):
                  video_ids.append(response2["items"][i]["snippet"]["resourceId"]["videoId"]
               )   
#get video details
#request3
#response3
            video_id =[]
            video_title =[]
            view_count =[]
            like_count =[]
            published_date =[]
            comment_count =[]
            channel_IDS =[] 
            for i in range (len(video_ids)):
                  request3 = youtube.videos().list(
                        part= "snippet,contentDetails,statistics",
                        id = video_ids[i],
                     maxResults = 5,                                           
                  )
                  response3 = request3.execute()
      
                  for i in range(len(response3["items"])):
                       video_id.append(response3["items"][i]["id"])
                       s=[(response3["items"][i]["snippet"]["publishedAt"])[:-1]]
                       published_date.append(s)
                       video_title.append(response3["items"][i]["snippet"]["title"])        
                       view_count.append(response3["items"][i]["statistics"].get("viewCount"))
                       like_count.append(response3["items"][i]["statistics"].get("likeCount"))       
                       comment_count.append(response3["items"][i]["statistics"].get("commentCount"))
                       channel_IDS.append(response3["items"][i]["snippet"]["channelId"])          
#insert query
            for i in range(len(video_id)): 
                insert_video_query = "INSERT INTO videos ( id,videoname,views,likes,channelid,commentcount ) VALUES (%s,%s,%s,%s,%s,%s)"
                insert_video_values = (video_id[i], video_title[i], view_count[i],like_count[i],channel_IDS[i],comment_count[i])
    
                connection = create_server_connection()
                execute_querys(connection, insert_video_query, insert_video_values)
#get comment details 
#request4
#response4
            comment_id =[]
            comment_text =[]
            comment_author =[]
            video_ID =[]
            for i in range (len(video_id)):
                 request4 = youtube.commentThreads().list(
                      part= "snippet",
                      videoId=video_id[i], 
                      maxResults = 5,        
                 )

                 response4 = request4.execute()
        
                 for i in range(len(response4["items"])): 
                       comment_id.append(response4["items"][i]["snippet"]["topLevelComment"]["id"])
                       comment_text.append(response4["items"][i]["snippet"]["topLevelComment"]["snippet"]["textOriginal"])
                       comment_author.append(response4["items"][i]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"])
                       video_ID.append(response4["items"][i]["snippet"]["videoId"]) 
#insert query  
            for i in range(len(comment_id)):  #"INSERT INTO comments (commentid,videosid,text,commentauthor) VALUES (LEFT(%s,%s,%s,%s)"            
                      insert_comment_query =  "INSERT INTO comments (commentid,videosid,text,commentauthor) VALUES (%s,%s,LEFT(%s,100), %s)"                     
                      insert_comment_values = (comment_id[i],video_ID[i],comment_text[i],comment_author[i])

                      connection = create_server_connection()
                      execute_querys(connection, insert_comment_query, insert_comment_values)  


#analist the data
#'mysql+mysqlconnector://admin:nambukeerthi@youtubedb.c96m2ysgag7t.eu-north-1.rds.amazonaws.com/youtubedb'

connection_variable = 'mysql+mysqlconnector://root:root@localhost/practice'
def task_1():          
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query1 = """(SELECT c.name,v.videoname FROM channel c JOIN videos v ON c.id=v.channelid);"""
    df_task1 = pd.read_sql(analys_query1, con=db_connection)
    return df_task1 
    
    #st.write("Task 1 selected.")
    # Implement task 1 logic here
    # Example: Query and display names of videos and their channels

def task_2():
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query2 = """(SELECT name,videoscount FROM channel ORDER BY videoscount DESC LIMIT 10);"""
    df_task2 = pd.read_sql(analys_query2, con=db_connection)
    return df_task2 
    
    # Implement task 2 logic here
    # Example: Find channels with the most videos and their counts

def task_3():
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query3 = """(SELECT c.name,v.videoname,v.views FROM CHANNEL C LEFT JOIN videos v ON v.channelid=c.id ORDER BY v.views DESC LIMIT 10);"""
    df_task3 = pd.read_sql(analys_query3, con=db_connection)
    return df_task3 
    
    # Implement task 3 logic here
    # Example: List top 10 most viewed videos and their channels

def task_4():
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query4 = """(SELECT v.videoname,v.commentcount,c.text FROM videos v LEFT JOIN comments c ON v.id=c.videosid ORDER BY v.commentcount DESC LIMIT 10);"""
    df_task4 = pd.read_sql(analys_query4, con=db_connection)
    return df_task4 
    
    # Implement task 4 logic here
    # Example: Show comments count for each video with their names

def task_5():
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query5 = """(SELECT c.name,v.likes FROM channel c LEFT JOIN videos v on c.id=v.channelid ORDER BY v.likes DESC LIMIT 10);"""
    df_task5 = pd.read_sql(analys_query5, con=db_connection)
    return df_task5 

    # Implement task 5 logic here
    # Example: Display videos with the highest number of likes and their channels

def task_6():
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query6 = """(SELECT c.name,v.videoname,v.likes FROM channel c LEFT JOIN videos v on c.id=v.channelid ORDER BY v.likes DESC LIMIT 10);"""
    df_task6 = pd.read_sql(analys_query6, con=db_connection)
    return df_task6 

    # Implement task 6 logic here
    # Example: Calculate total views for each channel and display names

def task_7():
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query7 = """(SELECT name,views FROM channel ORDER BY views DESC);"""
    df_task7 = pd.read_sql(analys_query7, con=db_connection)
    return df_task7 
    
    # Implement task 7 logic here
    # Example: List channels that published videos in 2022

def task_8():
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query8 = """(SELECT c.name,v.videoname,v.publisheddate FROM channel c right JOIN videos v ON c.id=v.channelid WHERE YEAR(publisheddate)=2022);"""
    df_task8 = pd.read_sql(analys_query8, con=db_connection)
    return df_task8 
    # Implement task 8 logic here
    # Example: Find videos with the highest number of comments and their channels

def task_9():
    db_connection_str = connection_variable
    db_connection = create_engine(db_connection_str)
    analys_query9 = """(SELECT c.name,c.id, COUNT(v.commentcount) as commentcount FROM channel c JOIN videos v ON c.id=v.channelid GROUP BY c.id,c.name);"""
    df_task9 = pd.read_sql(analys_query9, con=db_connection)
    return df_task9 
    
    # Implement task 8 logic here
    # Example: Find videos with the highest number of comments and their channels

def main():
    st.title("Video Analytics Dashboard")

#stremlit 
st.header("Youtube Data Harvesting and Warehousing")
#sidebar
with st.sidebar:
     selected_page = option_menu ("Main Menu",["Home","Upload","Tasks"],
     icons=["house", "cloud_upload" , "list_task"],
     menu_icon="cast",
       default_index=0)

#homepage
def pagehome():
       
          st.subheader("",)
          img = Image.open("youtube_project.jpeg")
          st.image( img, caption= "Method of process",use_column_width=True,channels="RGB"
                   )
#upload page       
def pageupload():
           
       if st.button("Create DB"):                                      
         create_tables()
         st.caption("Database Created") 
       input_channel_id = st.text_input("Give Channel Id")
       col1,col2 = st.columns(2)
       with col1:
             clicked =st.button("Detalis")
             if clicked:    
                st.write("Channel Details:  ") 
                channel_details =channel_func(input_channel_id)
                st.write(channel_details)

       with col2:  
             if st.button("Upload") :
                upload_func(input_channel_id)
                st.write("UPLOAD SUCCESSFULLY")
                
#task page            
def pagetasks():

    if st.button("Drop DB"):                                  
        drop_tables()
        st.caption("Database Droped") 

    # Dropdown select box for tasks
    choice = st.selectbox("Select Task", [
        "What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of likes for each video, and what are their corresponding video names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "Which videos have the highest number of comments, and what are their corresponding channel names?"
    ])
    if st.button("Submit"):
        if choice == "What are the names of all the videos and their corresponding channels?":
            df_task1 = task_1()
            st.dataframe(df_task1)
        elif choice == "Which channels have the most number of videos, and how many videos do they have?":
            df_task2 = task_2()
            st.dataframe(df_task2)
        elif choice == "What are the top 10 most viewed videos and their respective channels?":
            df_task3 = task_3()
            st.dataframe(df_task3)
        elif choice == "How many comments were made on each video, and what are their corresponding video names?":
            df_task4 = task_4()
            st.dataframe(df_task4)
        elif choice == "Which videos have the highest number of likes, and what are their corresponding channel names?":
            df_task5 = task_5()
            st.dataframe(df_task5)
        elif choice == "What is the total number of likes for each video, and what are their corresponding video names?":
            df_task6 = task_6()
            st.dataframe(df_task6)
        elif choice == "What is the total number of views for each channel, and what are their corresponding channel names?":
            df_task7 = task_7()
            st.dataframe(df_task7)
        elif choice == "What are the names of all the channels that have published videos in the year 2022?":
            df_task8 = task_8()
            st.dataframe(df_task8)
        elif choice == "Which videos have the highest number of comments, and what are their corresponding channel names?":         
            df_task9 = task_9()
            st.dataframe(df_task9)
        
page_names_to_funcs ={
      "Home":pagehome,
      "Upload":pageupload,
      "Tasks":pagetasks,

}
selected = selected_page
if selected in page_names_to_funcs:
    try:
       page_names_to_funcs [selected]()
    except KeyError:
            st.caption (f"Error: '{selected}' is not a valid key in page_names_to_funcs.")
    except Exception as e:
            st.caption (f"An error occurred: {e}")
            # Optionally, add more specific handling or logging based on the exception type


       