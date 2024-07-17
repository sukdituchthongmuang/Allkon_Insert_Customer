import psycopg2
import pandas as pd

# ระบุเส้นทางของไฟล์ Excel
file_path = r"C:/Users/sukdi/Downloads/Telegram Desktop/Customer (1) (2).xlsx"

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
    
    # ดึงข้อมูลจากตาราง customer_group
    cursor.execute("SELECT cg_id, cg_name FROM customer_group")
    customer_groups = cursor.fetchall()
    
    # สร้าง DataFrame จากข้อมูลที่ดึงมา
    df_customer_group = pd.DataFrame(customer_groups, columns=['cg_id', 'cg_name'])
    
    # ดึงข้อมูลจากตาราง corporation_type
    cursor.execute("SELECT ct_id, ct_name FROM corporation_type")
    corporation_types = cursor.fetchall()
    
    # สร้าง DataFrame จากข้อมูลที่ดึงมา
    df_corporation_type = pd.DataFrame(corporation_types, columns=['ct_id', 'ct_name'])
    
    # ปิด cursor และ connection
    cursor.close()
    connection.close()
    
    print("การเชื่อมต่อกับฐานข้อมูลและนำข้อมูลเสร็จสิ้น")
    
except (Exception, psycopg2.DatabaseError) as error:
    print(f"เกิดข้อผิดพลาดในการเชื่อมต่อหรือการดึงข้อมูล: {error}")

# อ่านข้อมูลจากไฟล์ Excel
try:
    # อ่านข้อมูลโดยระบุ dtype ของ Tax branch เป็น str
    df = pd.read_excel(file_path, dtype={'Tax branch': str, 'Tax exempt number': str})
    print("การนำเข้าไฟล์ Excel เสร็จสิ้น")
    
except Exception as e:
    print(f"เกิดข้อผิดพลาดในการอ่านไฟล์ Excel: {e}")

# ทำการแมพข้อมูลระหว่าง DataFrame
try:
    # ทำการแมพข้อมูลจาก cg_name ใน df_customer_group กับ Customer group ใน df
    df = df.merge(df_customer_group, left_on='Customer group', right_on='cg_name', how='left')
    
    # เขียนทับข้อมูลใน df คอลลัมน์ Customer group ด้วย cg_id และเปลี่ยนชื่อเป็น cus_cg_id
    df['cus_cg_id'] = df['cg_id']
    df.drop(['cg_name', 'cg_id'], axis=1, inplace=True)
    
    # ทำการแมพข้อมูลจาก ct_name ใน df_corporation_type กับ Type ใน df
    df = df.merge(df_corporation_type, left_on='Type', right_on='ct_name', how='left')
    
    # เขียนทับข้อมูลใน df คอลลัมน์ Type ด้วย ct_id และเปลี่ยนชื่อเป็น cus_ct_id
    df['cus_ct_id'] = df['ct_id']
    df.drop(['ct_name', 'ct_id'], axis=1, inplace=True)
    
    # เพิ่มคอลัมน์ใหม่ และใส่ค่าเสนอใจ
    df['cus_vendor_id'] = '08f2058e-9bc5-4d52-844d-8f23b3ccc692'
    # เพิ่มคอลัมน์ใหม่ และใส่ค่าในตอนที่ผ่านการรับรอง
    df['cus_cs_id'] = '04a6eb38-fb72-4511-84d7-12cc304f453b'
    # เพิ่มคอลัมน์ใหม่ และใส่ค่า t
    df['cus_is_use'] = 't'
    # เพิ่มคอลัมน์ใหม่ และใส่ค่า f
    df['cus_is_agent'] = 'f'
    
    # แทนค่าว่างใน DataFrame เป็น ''
    df.fillna('', inplace=True)
    
    # กำจัดข้อมูลซ้ำโดยเก็บแถวสุดท้ายที่เจอ
    df.drop_duplicates(subset=['Customer account'], keep='last', inplace=True)
    
    # แสดงตัวอย่างข้อมูลหลังจากแมพและกำจัดข้อมูลซ้ำ
    print("ข้อมูลหลังจากแมพและกำจัดข้อมูลซ้ำ:")
    print(df.iloc[:5])

except Exception as e:
    print(f"เกิดข้อผิดพลาดในการแมพข้อมูล: {e}")

# นำข้อมูลลงในฐานข้อมูล
try:
    # เชื่อมต่อกับฐานข้อมูลอีกครั้ง
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()
    
    # SQL query สำหรับการนำเข้าข้อมูล
    insert_query = """
    INSERT INTO customer (cus_no, cus_cg_id, cus_name, cus_company_name, cus_tax_no, cus_contact_fax, cus_contact_tel, cus_ct_id, cus_fax, cus_tel, cus_vendor_id, cus_cs_id, cus_is_use, cus_branch, cus_credit_limit, cus_is_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # เตรียมข้อมูลที่จะนำเข้า
    data_to_insert = df[['Customer account', 'cus_cg_id', 'Search name', 'Organization name', 'Tax exempt number', 'Primary contact fax', 'Primary contact phone', 'cus_ct_id', 'Primary contact fax', 'Primary contact phone', 'cus_vendor_id', 'cus_cs_id', 'cus_is_use', 'Tax branch', 'Credit limit', 'cus_is_agent']].values.tolist()
    
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
