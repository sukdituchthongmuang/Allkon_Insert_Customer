import psycopg2
import pandas as pd
import numpy as np

file_path = r"C:/Users/sukdi/Downloads/Telegram Desktop/CustomerAddress (1) (2).xlsx"

# รายละเอียดฐานข้อมูล
db_config = {
    'dbname': 'Customer',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

# อ่านข้อมูลจากฐานข้อมูล PostgreSQL
try:
    # เชื่อมต่อกับฐานข้อมูล
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # ดึงข้อมูลจากตาราง customer
    cursor.execute("SELECT cus_no, cus_id, cus_company_name FROM customer")
    customer_data = cursor.fetchall()
    customer_dict = {item[0]: {'cus_id': item[1], 'cus_company_name': item[2]} for item in customer_data}
    
    # ดึงข้อมูลจากตาราง customer_type_location
    cursor.execute("SELECT ctl_name, ctl_id FROM customer_type_location")
    ctl_data = cursor.fetchall()
    ctl_dict = {item[0]: item[1] for item in ctl_data}
    
    # ปิด cursor และ connection 
    cursor.close()
    connection.close()
    
    print("การเชื่อมต่อกับฐานข้อมูลและนำข้อมูลเสร็จสิ้น")
    
except (Exception, psycopg2.DatabaseError) as error:
    print(f"เกิดข้อผิดพลาดในการเชื่อมต่อหรือการดึงข้อมูล: {error}")

# อ่านข้อมูลจากไฟล์ Excel
try:
    # อ่านข้อมูลจากไฟล์ Excel
    df = pd.read_excel(file_path, dtype={'State': str, 'County': str, 'ZIP/postal code': str, 'City': str})
    print("การนำเข้าไฟล์ Excel เสร็จสิ้น")
    print("ก่อนการแก้ไข:")
    print(df)
    
    # แทนค่าว่างใน DataFrame เป็น ''
    df.replace(np.nan, '', inplace=True)
    
    # สร้าง DataFrame ใหม่สำหรับแถวที่ถูกต้อง
    new_rows = []
    
    for index, row in df.iterrows():
        address_roles = row['Address location roles']
        roles_list = address_roles.split(';')
        
        for role in roles_list:
            role_clean = role.strip()
            new_row = row.copy()
            new_row['Address location roles'] = role_clean
            new_rows.append(new_row)
    
    # สร้าง DataFrame ใหม่จากแถวที่ถูกต้อง
    if new_rows:
        df_new = pd.DataFrame(new_rows)
    else:
        df_new = df
    
    # ตรวจสอบการแมพข้อมูลและลบแถวที่มี 'Business' ออกหาก cus_no มีใน customer_dict
    df_new['to_remove'] = df_new.apply(
        lambda row: row['Address location roles'] == 'Business' and row['Customer account'] in customer_dict, axis=1)
    
    df_final = df_new[df_new['to_remove'] == False].drop(columns=['to_remove'])
    
    # แมพข้อมูลระหว่าง Address location roles กับ customer_type_location และเพิ่มคอลัมน์ใหม่
    df_final['ca_ctl_id'] = df_final['Address location roles'].map(ctl_dict)
    
    # แมพข้อมูลระหว่าง Customer account กับ customer และเพิ่มคอลัมน์ใหม่
    df_final['ca_cus_id'] = df_final['Customer account'].map(lambda x: customer_dict.get(x, {}).get('cus_id', None))
    df_final['cus_company_name'] = df_final['Customer account'].map(lambda x: customer_dict.get(x, {}).get('cus_company_name', ''))
    
    # สร้างคอลลัมน์ใหม่และใส่ค่า 't' ทุกแถวที่มีข้อมูล
    df_final['ca_is_default'] = 't'
    
    # แปลงค่า NaN ในคอลัมน์ ca_ctl_id และ ca_cus_id เป็น None
    df_final['ca_ctl_id'] = df_final['ca_ctl_id'].replace({np.nan: None})
    df_final['ca_cus_id'] = df_final['ca_cus_id'].replace({np.nan: None})
    
    print("หลังการแก้ไข:")
    print(df_final)
    
except Exception as e:
    print(f"เกิดข้อผิดพลาดในการอ่านไฟล์ Excel หรือการนำข้อมูลลงในฐานข้อมูล: {e}")

# นำข้อมูลลงในฐานข้อมูล
try:
    # เชื่อมต่อกับฐานข้อมูลอีกครั้ง
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # SQL query สำหรับการนำเข้าข้อมูล
    insert_query = """
    INSERT INTO customer_address (ca_cus_id, ca_name, ca_loc_province_name, ca_loc_district_name, ca_loc_sub_district_name, ca_loc_data, ca_latitude, ca_longitude, ca_ctl_id, ca_detail_address, ca_is_default, ca_loc_province, ca_loc_district, ca_postcode)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
    
    # เตรียมข้อมูลที่จะนำเข้า
    data_to_insert = df_final[['ca_cus_id', 'Address description', 'State description', 'County description', 'City description', 'City', 'Latitude', 'Longitude', 'ca_ctl_id', 'Street', 'ca_is_default', 'State', 'County', 'ZIP/postal code']].values.tolist()
    
    # นำข้อมูลลงในฐานข้อมูล
    cursor.executemany(insert_query, data_to_insert)
    
    # บันทึกการเปลี่ยนแปลง
    connection.commit()
    
    # ปิด cursor และ connection
    cursor.close()
    connection.close()
    
    print("การนำข้อมูลลงในฐานข้อมูลสำเร็จ")

except (Exception, psycopg2.DatabaseError) as error:
    print(f"เกิดข้อผิดพลาดในการนำข้อมูลลงในฐานข้อมูล: {error}")
