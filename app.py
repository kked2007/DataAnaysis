from flask import Flask, render_template, request, redirect, url_for, session
from db import get_connection
import pandas as pd
import matplotlib.pyplot as plt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

app = Flask(__name__)
app.secret_key = "admin12345"
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form['username']
        password = request.form['password']

        
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        query = """SELECT * FROM users WHERE username = %s AND password = %s"""

        cursor.execute(query, (username, password))

        user = cursor.fetchone()

        if user :
            print(user)
            session["userName"] = username
            return redirect(url_for("welcome"))
        
        else:    
            return render_template(
                "login.html",
                error="Invalid Username or Password"
            )
    else:
          return render_template(
                "login.html",
                error="Your logout..."
            )  
    cursor.close()
    conn.close()
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/welcome", methods=["GET", "POST"])
def welcome():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]

    selected_table = None
    data = []
    columns = []
    summary = None

    if request.method == "POST":
        selected_table = request.form["table_name"]

        query = f"SELECT * FROM `{selected_table}` LIMIT 100"
        df = pd.read_sql(query, conn)

        data = df.to_dict(orient="records")
        columns = df.columns.tolist()

        summary = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "numeric_summary": df.describe().to_html(classes="table table-bordered")
        }

    cursor.close()
    conn.close()

    return render_template(
        "welcome.html",
        tables=tables,
        selected_table=selected_table,
        columns=columns,
        data=data,
        summary=summary
    )

@app.route("/analysis/table_name/<table_name>", methods=["GET", "POST"])
def analysis(table_name):

    conn = get_connection()
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()

    total_rows = len(df)
    columns = list(df.columns)

    spark = SparkSession.builder \
        .appName("MySQL Data Analysis") \
        .getOrCreate()

    spark_df = spark.createDataFrame(df)

    result = None

    if table_name == "sales":
        result_df = spark_df.groupBy("category").agg(
            spark_sum(col("quantity") * col("price")).alias("total_sales")
        )

        result = result_df.toPandas().to_dict(orient="records")

    return render_template(
        "analysis.html",
        table_name=table_name,
        total_rows=total_rows,
        columns=columns,
        result=result
    )

if __name__ == "__main__":
    app.run(debug=True)