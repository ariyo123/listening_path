import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import csv
import mysql.connector as msql
import mysql.connector
from mysql.connector import Error
import logging
import csv
import datetime
from datetime import date, timedelta
import logging
import traceback
import sys
import os
import shutil
import to

while True:
#move1('port')
    if __name__ == "__main__":
        patterns = ["*"]
        # C:/python_work/changes/port
        ignore_patterns = None
        ignore_directories = False
        case_sensitive = True
        my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)


    def on_created(event):
        print(f"hey, {event.src_path} has been created!")
        port_path = "./port/port.csv"
        #myport_path = "C:/python_work/changes/port/port.csv"
        
        #myport_path = "C:/python_work/changes/port/port.csv"
        this={event.src_path}
        if this == port_path:
            print(event.src_path)
        
        try:
            with open(port_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The port.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The port.csv does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(port_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql = f"UPDATE `transaction_keys` SET local_port = {item[1]} WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql)
                            print(sql)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated port for {item[0]}:{sql}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("port update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Port update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('port')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminal parameters were registered')

        mcc_path = "./port/mcc.csv"
        this1={event.src_path}

        if this1 == mcc_path:
            print(event.src_path)
        try:
            with open(mcc_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The mcc.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mcc.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(mcc_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql1 = f"UPDATE `terminal_parameters` SET merchant_category_code = {item[1]} WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql1}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql1)
                            print(sql1)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MCC for {item[0]}:{sql1}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MCC update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MCC update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('mcc')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated')

        mid_path = "./port/mid.csv"
        this2={event.src_path}

        if this2 == mid_path:
            print(event.src_path)
        try:
            with open(mid_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The mid.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mid.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(mid_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql2 = f"UPDATE `terminal_parameters` SET merchant_id = '{item[1]}' WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql2}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql2)
                            print(sql2)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MID for {item[0]}:{sql2}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MID update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MID update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('mid')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated')                #to.move1('port')


        name_path = "./port/name.csv"
        this3={event.src_path}

        if this3 == name_path:
            print(event.src_path)
        try:
            with open(name_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The name.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mcc.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(name_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    ng='NG'
                    position1=str(item[1]+"                    ")
                    pos=position1[:23]
                    print(position1)
                    position2=str(item[2]+"           ")
                    pos1=position2[:13]
                    sql3 = f"UPDATE `terminal_parameters` SET merchant_location_name_address = '{pos}{pos1}{item[2]}{ng}' WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql3}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql3)
                            print(sql3)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MID for {item[0]}:{sql3}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MID update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MID update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('name')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated') 

    def on_deleted(event):
        print(f"hymmm Someone deleted {event.src_path}!")

    def on_modified(event):
        print(f"hey buddy, {event.src_path} has been modified")
        port_path = "./port/port.csv"
        #myport_path = "C:/python_work/changes/port/port.csv"
        
        #myport_path = "C:/python_work/changes/port/port.csv"
        this={event.src_path}
        if this == port_path:
            print(event.src_path)
        
        try:
            with open(port_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The port.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The port.csv does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(port_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql = f"UPDATE `transaction_keys` SET local_port = {item[1]} WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql)
                            print(sql)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated port for {item[0]}:{sql}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("port update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Port update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('port')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminal parameters were registered')

        mcc_path = "./port/mcc.csv"
        this1={event.src_path}

        if this1 == mcc_path:
            print(event.src_path)
        try:
            with open(mcc_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The mcc.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mcc.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(mcc_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql1 = f"UPDATE `terminal_parameters` SET merchant_category_code = {item[1]} WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql1}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql1)
                            print(sql1)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MCC for {item[0]}:{sql1}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MCC update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MCC update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('mcc')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated')

        mid_path = "./port/mid.csv"
        this2={event.src_path}

        if this2 == mid_path:
            print(event.src_path)
        try:
            with open(mid_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The mid.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mid.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(mid_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql2 = f"UPDATE `terminal_parameters` SET merchant_id = '{item[1]}' WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql2}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql2)
                            print(sql2)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MID for {item[0]}:{sql2}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MID update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MID update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('mid')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated')                #to.move1('port')


        name_path = "./port/name.csv"
        this3={event.src_path}

        if this3 == name_path:
            print(event.src_path)
        try:
            with open(name_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The name.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mcc.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(name_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    ng='NG'
                    position1=str(item[1]+"                    ")
                    pos=position1[:23]
                    print(position1)
                    position2=str(item[2]+"           ")
                    pos1=position2[:13]
                    sql3 = f"UPDATE `terminal_parameters` SET merchant_location_name_address = '{pos}{pos1}{item[2]}{ng}' WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql3}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql3)
                            print(sql3)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MID for {item[0]}:{sql3}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MID update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MID update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('name')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated') 
    def on_moved(event):
        print(f"ok ok ok, someone moved {event.src_path} to {event.dest_path}")
        port_path = "./port/port.csv"
        #myport_path = "C:/python_work/changes/port/port.csv"
        
        #myport_path = "C:/python_work/changes/port/port.csv"
        this={event.src_path}
        if this == port_path:
            print(event.src_path)
        
        try:
            with open(port_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The port.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The port.csv does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(port_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql = f"UPDATE `transaction_keys` SET local_port = {item[1]} WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql)
                            print(sql)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated port for {item[0]}:{sql}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("port update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Port update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('port')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminal parameters were registered')

        mcc_path = "./port/mcc.csv"
        this1={event.src_path}

        if this1 == mcc_path:
            print(event.src_path)
        try:
            with open(mcc_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The mcc.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mcc.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(mcc_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql1 = f"UPDATE `terminal_parameters` SET merchant_category_code = {item[1]} WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql1}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql1)
                            print(sql1)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MCC for {item[0]}:{sql1}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MCC update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MCC update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('mcc')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated')

        mid_path = "./port/mid.csv"
        this2={event.src_path}

        if this2 == mid_path:
            print(event.src_path)
        try:
            with open(mid_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The mid.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mid.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(mid_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    sql2 = f"UPDATE `terminal_parameters` SET merchant_id = '{item[1]}' WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql2}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql2)
                            print(sql2)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MID for {item[0]}:{sql2}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MID update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MID update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('mid')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated')                #to.move1('port')


        name_path = "./port/name.csv"
        this3={event.src_path}

        if this3 == name_path:
            print(event.src_path)
        try:
            with open(name_path, newline='') as f:
                reader = csv.reader(f)
                my_list = list(reader)
                f.close()
        except:
                print(f"The name.csv file does not exist in the location - C:/python_work/changes/port/")
                logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                logging.warning(f'The mcc.csv file does not exist in the location - C:/python_work/changes/port/ ')

        else:
            list_items = []
            parameter_count = 1
            for item in my_list:
                with open(name_path, newline='') as f:
                    reader = csv.reader(f)
                    my_list = list(reader)
                    contents1 = my_list
                    list_items.append(contents1)
                    f.close()
                    ng='NG'
                    position1=str(item[1]+"                    ")
                    pos=position1[:23]
                    print(position1)
                    position2=str(item[2]+"           ")
                    pos1=position2[:13]
                    sql3 = f"UPDATE `terminal_parameters` SET merchant_location_name_address = '{pos}{pos1}{item[2]}{ng}' WHERE terminal_id = '{item[0]}';"
                    logging.basicConfig(filename='log.log', filemode='a',
                                        format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'Total of {parameter_count} was updated ')
                    #print(sql)
                    logging.basicConfig(filename='log.log', filemode='a',format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'SQl statement are now being written to file named sql {item[0]}:{sql3}. ')
                    try:
                        conn = msql.connect(host='127.0.0.1', database='postxnprocessors', user='root', password='Magfum12@')
                        if conn.is_connected():
                            cursor = conn.cursor()
                            

                            cursor.execute("select database();")
                            record = cursor.fetchone()
                            cursor.execute(sql3)
                            print(sql3)
                            print("You're connected to database: ", record)
                            logging.warning(f'You are connected to database and have updated MID for {item[0]}:{sql3}')
                            
                            print("Record updated")
                            # the connection is not auto committed by default, so we must commit to save our changes
                            conn.commit()
                            print("MID update has ended")
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'MID update has ended')
                            print(f"A total of {parameter_count} terminal parameters were updated")
                            to.move1('name')
                    except Error as e:
                            logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                            logging.warning(f'Duplicated or database connection error ')
                            print(e)
                    parameter_count=parameter_count+ 1
                    
                    logging.basicConfig(filename='log.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
                    logging.warning(f'A total of {parameter_count} terminals  were updated')                #to.move1('port')




    my_event_handler.on_created = on_created
    my_event_handler.on_deleted = on_deleted
    my_event_handler.on_modified = on_modified
    my_event_handler.on_moved = on_moved

    #C:/python_work/changes/port
    path = "C:/python_work/changes/port"
    go_recursively = True
    my_observer = Observer()
    my_observer.schedule(my_event_handler, path, recursive=go_recursively)

    my_observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        my_observer.stop()
        my_observer.join()

    time.sleep(2)










