import psycopg2
from psycopg2 import sql
import datetime
import pytz
import json

class PostgresLogger:
    def __init__(self, job_id, details = None, job_type = None):
        try:
            self.conn = psycopg2.connect(
                dbname="Logger",
                user="postgres",
                password="Subbu@2002",
                host="localhost",
                port="5432"
            )
            self.cur = self.conn.cursor()
        except Exception as e:
            print(e)
        self.job_id = job_id
        self.details = details
        self.job_type = job_type
        self.__transformation_list = []
        self.__starttime = self.get_current_time()

    def get_current_time(self):
        current_datetime = datetime.datetime.now()
        ist_timezone = pytz.timezone('Asia/Kolkata')
        ist_time = current_datetime.astimezone(ist_timezone)
        return ist_time.strftime("%Y-%m-%d %H:%M:%S:%f")

    def stat(self, job_info):
        try:
            insert = sql.SQL("""
                INSERT INTO stats (job_id, details, job_type, start_time, end_time, total_runtime, info, transformations) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """)
            data = (
                job_info["Job ID"],
                job_info["Details"],
                job_info["Job Type"],
                job_info["Start Time"],
                job_info["End Time"],
                job_info["Total Runtime"],
                json.dumps(job_info["Info"]),
                json.dumps(job_info["Transformations"])
            )
            self.cur.execute(insert, data)
            self.conn.commit()
        except Exception as e:
            print(e)

    def start_transformation(self, name, details = None, SQL_query = None):
        transformation = self.Transformation(name, details, SQL_query)
        self.__transformation_list.append(transformation)
        return transformation

    def end_transformation(self, transformation, info = {}):
        transformation.trans_end(info)

    def end(self, info = {}):
        endtime = self.get_current_time()
        time_format = "%Y-%m-%d %H:%M:%S:%f"
        datetime1 = datetime.datetime.strptime(self.__starttime, time_format)
        datetime2 = datetime.datetime.strptime(endtime, time_format)
        time_difference = datetime2 - datetime1
        hours = time_difference.days * 24 + time_difference.seconds // 3600
        minutes = (time_difference.seconds % 3600) // 60
        seconds = time_difference.seconds % 60
        milliseconds = time_difference.microseconds // 1000
        microseconds = time_difference.microseconds % 1000
        totalruntime = f"{hours}:{minutes}:{seconds}:{milliseconds}:{microseconds}"
        self.__info = info
        job_info = {
            "Job ID": self.job_id,
            "Details": self.details,
            "Job Type": self.job_type,
            "Start Time": self.__starttime,
            "End Time": endtime,
            "Total Runtime": totalruntime,
            "Info": self.__info,
            "Transformations": [trans.__dict__ for trans in self.__transformation_list]
        }
        self.stat(job_info)

    class Transformation:
        def __init__(self, name, details = None, SQL_query = None):
            self.transformation_name = name
            self.transformation_details = details
            self.SQL_query = SQL_query
            self.starttime = self.get_current_time()
            self.trans_info = {}

        def get_current_time(self):
            current_datetime = datetime.datetime.now()
            ist_timezone = pytz.timezone('Asia/Kolkata')
            ist_time = current_datetime.astimezone(ist_timezone)
            return ist_time.strftime("%Y-%m-%d %H:%M:%S:%f")

        def trans_end(self, info = {}):
            self.trans_info.update(info)
            self.endtime = self.get_current_time()
            time_format = "%Y-%m-%d %H:%M:%S:%f"
            datetime1 = datetime.datetime.strptime(self.starttime, time_format)
            datetime2 = datetime.datetime.strptime(self.endtime, time_format)
            time_difference = datetime2 - datetime1
            hours = time_difference.days * 24 + time_difference.seconds // 3600
            minutes = (time_difference.seconds % 3600) // 60
            seconds = time_difference.seconds % 60
            milliseconds = time_difference.microseconds // 1000
            microseconds = time_difference.microseconds % 1000
            self.totalruntime = f"{hours}:{minutes}:{seconds}:{milliseconds}:{microseconds}"

    def __del__(self):
        self.conn.close()
        self.cur.close()