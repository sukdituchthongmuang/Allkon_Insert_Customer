import psycopg2
import pandas as pd

# รายละเอียดฐานข้อมูล
db_config = {
    'dbname': 'Customer',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

# ฟังก์ชันสำหรับดึงข้อมูล
def fetch_data(cursor, ca_ctl_id):
    query = """
    SELECT ca_id, ca_cus_id, ca_detail_address
    FROM customer_address 
    WHERE ca_ctl_id = %s
    """
    cursor.execute(query, (ca_ctl_id,))
    return cursor.fetchall()

# สร้างการเชื่อมต่อและดึงข้อมูล
try:
    # เชื่อมต่อกับฐานข้อมูล
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # ดึงข้อมูลจากตาราง customer_address 
    ca_ctl_id1 = ['0a089c93-a446-4596-92d4-f5e12cb0d63d']
    records = None
    
    for ca_ctl_id in ca_ctl_id1:
        records = fetch_data(cursor, ca_ctl_id)
        if records:
            break
    
    if records:
        # สร้าง DataFrame จากผลลัพธ์
        df = pd.DataFrame(records, columns=['ca_id', 'ca_cus_id', 'ca_detail_address'])
        
        # ปิด cursor
        cursor.close()
        
        # เชื่อมต่อกับฐานข้อมูลอีกครั้งเพื่ออัพเดตข้อมูล
        cursor = connection.cursor()
        
        for index, row in df.iterrows():
            # อัพเดตข้อมูลในตาราง customer
            update_query = """
            UPDATE customer 
            SET cus_ca_id = %s, cus_adress = %s
            WHERE cus_id = %s
            """
            cursor.execute(update_query, (row['ca_id'], row['ca_detail_address'], row['ca_cus_id']))
        
        # บันทึกการเปลี่ยนแปลง
        connection.commit()
        
        print("การอัพเดตข้อมูลเสร็จสิ้น")
    else:
        print("ไม่พบข้อมูลในตาราง customer_address ที่ตรงกับเงื่อนไข")

except (Exception, psycopg2.DatabaseError) as error:
    print(f"เกิดข้อผิดพลาดในการเชื่อมต่อข้อมูล: {error}")

# สร้างการเชื่อมต่อและดึงข้อมูล
try:
    # เชื่อมต่อกับฐานข้อมูล
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # ดึงข้อมูลจากตาราง customer_address 
    ca_ctl_id1 = ['17edf27d-c7ca-4146-a8a6-8319df669c73']
    records = None
    
    for ca_ctl_id in ca_ctl_id1:
        records = fetch_data(cursor, ca_ctl_id)
        if records:
            break
    
    if records:
        # สร้าง DataFrame จากผลลัพธ์
        df = pd.DataFrame(records, columns=['ca_id', 'ca_cus_id', 'ca_detail_address'])
        
        # ปิด cursor
        cursor.close()
        
        # เชื่อมต่อกับฐานข้อมูลอีกครั้งเพื่ออัพเดตข้อมูล
        cursor = connection.cursor()
        
        for index, row in df.iterrows():
            # อัพเดตข้อมูลในตาราง customer
            update_query = """
            UPDATE customer 
            SET cus_ca_id = %s, cus_adress = %s
            WHERE cus_id = %s
            """
            cursor.execute(update_query, (row['ca_id'], row['ca_detail_address'], row['ca_cus_id']))
        
        # บันทึกการเปลี่ยนแปลง
        connection.commit()
        
        print("การอัพเดตข้อมูลเสร็จสิ้น")
    else:
        print("ไม่พบข้อมูลในตาราง customer_address ที่ตรงกับเงื่อนไข")

except (Exception, psycopg2.DatabaseError) as error:
    print(f"เกิดข้อผิดพลาดในการเชื่อมต่อข้อมูล: {error}")
        
# สร้างการเชื่อมต่อและดึงข้อมูล
try:
    # เชื่อมต่อกับฐานข้อมูล
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # ดึงข้อมูลจากตาราง customer_address 
    ca_ctl_id1 = ['87558eec-2f3e-4165-93b8-ac5498d25c71']
    records = None
    
    for ca_ctl_id in ca_ctl_id1:
        records = fetch_data(cursor, ca_ctl_id)
        if records:
            break
    
    if records:
        # สร้าง DataFrame จากผลลัพธ์
        df = pd.DataFrame(records, columns=['ca_id', 'ca_cus_id', 'ca_detail_address'])
        
        # ปิด cursor
        cursor.close()
        
        # เชื่อมต่อกับฐานข้อมูลอีกครั้งเพื่ออัพเดตข้อมูล
        cursor = connection.cursor()
        
        for index, row in df.iterrows():
            # อัพเดตข้อมูลในตาราง customer
            update_query = """
            UPDATE customer 
            SET cus_ca_id = %s, cus_adress = %s
            WHERE cus_id = %s
            """
            cursor.execute(update_query, (row['ca_id'], row['ca_detail_address'], row['ca_cus_id']))
        
        # บันทึกการเปลี่ยนแปลง
        connection.commit()
        
        print("การอัพเดตข้อมูลเสร็จสิ้น")
    else:
        print("ไม่พบข้อมูลในตาราง customer_address ที่ตรงกับเงื่อนไข")

except (Exception, psycopg2.DatabaseError) as error:
    print(f"เกิดข้อผิดพลาดในการเชื่อมต่อข้อมูล: {error}")

finally:
    if connection is not None:
        connection.close()
