import pyodbc

def load_data(df):
    conn = pyodbc.connect(
        'Driver={ODBC Driver 18 for SQL Server};'
        'Server=sqlserver,1433;' 
        'Database=BigDataDB;'
        'UID=sa;'
        'PWD=BigData123!;'
        'TrustServerCertificate=yes;'
    )
    cursor = conn.cursor()

    #xóa data cũ tránh duplicate
    cursor.execute("TRUNCATE TABLE customer_spending")
    conn.commit()

    # insert data
    for row in df.collect():
        cursor.execute(
            "INSERT INTO customer_spending (customer, total_spent) VALUES (?, ?)",
            (row['customer'], float(row['total_spent']))
        )

    conn.commit()

    cursor.close()
    conn.close()