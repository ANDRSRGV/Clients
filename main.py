import psycopg2

database = "TestDB"
user = 'postgres'
password = 'system'

def reset_data_table(database=database, user=user, password=password):
    with psycopg2.connect(database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute("""DROP table if exists  client_phone;
            DROP table if exists  clients;
            """)
            cur.execute("""CREATE TABLE if not exists clients (client_name varchar(60) NOT NULL,
            client_surname varchar(60) NOT NULL,
            e_mail varchar(30) NULL,
            client_code serial4 NOT NULL,
            CONSTRAINT clients_pk PRIMARY KEY (client_code)
            );
            CREATE TABLE client_phone (
            client_id int NOT NULL,
            phone varchar(12) NOT NULL,
            CONSTRAINT client_phone_pk PRIMARY KEY (client_id,phone),
            CONSTRAINT client_phone_fk FOREIGN KEY (client_id) REFERENCES clients(client_code)
            );""")
            conn.commit()


def add_client(name: str, surname: str, phone=None, e_mail=None):
    if e_mail!=None and not check_email(e_mail):
        return
    if not check_name(name):
        return
    if not check_surname(surname):
        return
    with psycopg2.connect(database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO clients (client_name, client_surname, e_mail) 
            values (%s, %s, %s) returning client_code;""", (name, surname, e_mail));
            client_code = cur.fetchone()[0]
            print(f'Новый клиент добавлен под номером {client_code}')
            if phone!=None and not check_phone(phone):
                return
            cur.execute("""INSERT INTO client_phone (client_id, phone)  values (%s, %s) ;""", (client_code, phone))
            conn.commit()

def check_phone(phone:str):
    if phone[0] !='+' or len(phone) !=12 or not phone[1:].isnumeric():
        print('Номер телефона введён некорректно. Введите номер телефона в формате +ххххххххххх')
        return (False)
    else:
        return (True)

def check_email(e_mail:str):
    at = e_mail.find('@')
    dot = e_mail.rfind('.')
    if len(e_mail)>30 or at==-1 or at==0 or e_mail.find('@',at+1)!=-1 or at>dot or e_mail.rfind('.', dot-1)<at or dot==len(e_mail):
        print('Адрес электронной почты введён некорректно. Введите адрес электронной почты в формате youraddress@yourdomain.globaldomain')
        return (False)
    else:
        return (True)

def check_name(name:str):
    if len(name)>60:
        print('Имя не может быть длиннее 60 символов.')
        return(False)
    else:
        return(True)

def check_surname(surname:str):
    if len(surname)>60:
        print('Фамилия не может быть длиннее 60 символов.')
        return(False)
    else:
        return(True)

def check_phone_in_base(client_code:int, phone:str, mode='Add'):
    with psycopg2.connect(database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT client_id FROM client_phone WHERE client_id=%s AND phone=%s;", (client_code, phone))
            if cur.fetchone() != None:
                if mode == 'Add':
                    print(f'Номер телефона {phone} уже добавлен для лиента {client_code}.')
                return(False)
            else:
                return(True)

def replace_phone(client_code:int, phone):
    replacement_phones = []
    new_phones = []
    for ph in phone:
        if not check_phone(ph):
            continue
        if not check_phone_in_base(client_code, ph, 'Replace'):
            replacement_phones.append(ph)
        else:
            new_phones.append(ph)
    for ph in new_phones:
        with psycopg2.connect(database=database, user=user, password=password) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO client_phone (client_id,phone) VALUES (%s, %s)", (client_code, ph))
        conn.commit()
        print(f'Клиенту {client_code} добавлены новые номера телефонов: {new_phones}')

    for ph in replacement_phones:
        with psycopg2.connect(database=database, user=user, password=password) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE client_phone SET phone=%s where client_id=%s AND phone=%s;", (client_code, ph))
                print(f'У клиента {client_code} номер телефона : {new_phones}')


    with psycopg2.connect(database=database, user=user, password=password) as conn:
         with conn.cursor() as cur:
             cur.execute("INSERT INTO client_phone (client_id,phone) VALUES (%s, %s)", (client_code, ph))
        conn.commit()
    print(f'Клиенту {client_code} добавлен новый номер телефона {ph}')

def add_phone(client_code:int, phone):
    for ph in phone:
        if not check_phone(ph):
            continue
        if not check_phone_in_base(client_code, ph):
            continue
        with psycopg2.connect(database=database, user=user, password=password) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO client_phone (client_id,phone) VALUES (%s, %s)", (client_code, ph))
            conn.commit()
        print(f'Клиенту {client_code} добавлен новый номер телефона {ph}')



add_client('sdf','sdf','+49037488859')
add_phone(1, ['+89037388859','+69997468859'])
# reset_data_table()