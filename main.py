import psycopg2



def create_db(conn, database, user, password):
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


def add_client(conn, name: str, surname: str, phone: str = None, e_mail: str = None):
    if e_mail!=None and not check_email(e_mail):
        return
    if not check_name(name):
        return
    if not check_surname(surname):
        return
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO clients (client_name, client_surname, e_mail) 
        values (%s, %s, %s) returning client_code;""", (name, surname, e_mail));
        client_code = cur.fetchone()[0]
        print(f'Новый клиент добавлен под номером {client_code}')
        if phone!=None and check_phone(phone):
            cur.execute("""INSERT INTO client_phone (client_id, phone)  values (%s, %s) ;""", (client_code, phone))
            conn.commit()

def check_phone(phone:str):
    if phone[0] !='+' or len(phone) !=12 or not phone[1:].isnumeric():
        print(f'Номер телефона {phone} введён некорректно. Введите номер телефона в формате +ххххххххххх')
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

def check_phone_in_base(conn, client_code:int, phone:str):
    with conn.cursor() as cur:
        cur.execute("SELECT client_id FROM client_phone WHERE client_id=%s AND phone=%s;", (client_code, phone))
        if cur.fetchone() != None:
            print(f'Номер телефона {phone} для клиента {client_code} хранится в базе данных.')
        else:
            return(True)

def check_client_in_base(conn, client_code):
    with conn.cursor() as cur:
        cur.execute("SELECT client_code FROM clients WHERE client_code=%s;", (client_code,))
        if cur.fetchone() == None:
            print(f'Клиент с кодом {client_code} отсутствует.')
            return(False)
        else:
            return(True)



def add_phone(conn, client_code: int, phones):
        phones_to_add = []
        for ph in phones:
            if check_phone(ph) == True and check_phone_in_base(conn, client_code, ph) == True:
                phones_to_add.append(ph)
        if len(phones_to_add) == 0:
            return
        with conn.cursor() as cur:
            for ph in phones_to_add:
                cur.execute("INSERT INTO client_phone (client_id,phone) VALUES (%s, %s)", (client_code, ph))
                print(f'Клинету {client_code} присвоен номер телефона {ph}.')
        conn.commit()


def change_client(conn, client_code: int, name=None, surname=None, phones=None, e_mail=None):
           if check_client_in_base(conn, client_code) == False:
               return
           else:
                if name is not None and check_name(name)==False:
                   return
                if surname is not None and check_surname(surname)==False:
                   return
                if e_mail is not None and check_email(e_mail)==False:
                   return
           with conn.cursor() as cur:
                cur.execute("UPDATE clients SET client_name=%s, client_surname=%s, e_mail=%s WHERE client_code=%s;",
                            (name, surname, e_mail, client_code))
                conn.commit()
                print(f'Клиенту с кодом {client_code} присовены имя и фамилия {name} {surname}, электронная почта - {e_mail}.')
                if len(phones) == 0:
                    return
                cur.execute("DELETE FROM client_phone WHERE client_id=%s;", (client_code,))
                conn.commit()
                add_phone(conn, client_code, phones)


def rem_phone(conn, client_code: int, phone: str):
    if check_client_in_base(conn, client_code) == False:
        return
    with conn.cursor() as cur:
        if check_phone_in_base(conn, client_code, phone) == True:
            print(f'Номер телефона {phone} для клинета {client_code} отсутствует в базе данных.')
            return
        else:
            cur.execute("DELETE FROM client_phone WHERE client_id=%s AND phone=%s;", (client_code, phone))
            conn.commit()
            print(f'Номер телефона {phone} для клинета {client_code} удалён из базы данных.')


def rem_client(conn, client_code):
    if check_client_in_base(conn, client_code) == False:
        return
    with conn.cursor() as cur:
        cur.execute("""DELETE FROM client_phone WHERE client_id=%s;
        DELETE FROM clients WHERE client_code=%s;""", (client_code,client_code))
        conn.commit()
        print (f'Клиент {client_code} удалён из базы данных.')


def append_q(par_name: str, par_val: str = None ):
    if par_val != None:
        return(f" AND {par_name}=%s")
    else:
        return(" AND Null IS %s ")

def find_client(conn, name: str = None, surname: str = None, e_mail: str = None, phone: str = None):
    with conn.cursor() as cur:
        if phone is None:
            cur_q = "SELECT client_code, client_name, client_surname FROM clients WHERE True" + append_q('client_name', name) + \
            append_q('client_surname', surname) + append_q('e_mail', e_mail)
            cur.execute(cur_q, (name, surname, e_mail))
        else:
            cur_q = "SELECT client_code, client_name, client_surname FROM clients JOIN client_phone ON client_code=client_id WHERE" \
            " True" + append_q('client_name', name) + append_q('client_surname', surname) + append_q('e_mail', e_mail) + append_q('phone', phone)
            cur.execute(cur_q, (name, surname, e_mail, phone))
        result = cur.fetchall()
        if len(result) == 0:
            print('В базе данных не надены клиенты с указанными параметрами.')
        else:
            print('В базе данных надены следующие клиенты с указанными параметрами:')
            print(*result, sep='\n')


database = "clients"
user = 'postgres'
password = 'system'
with psycopg2.connect(database=database, user=user, password=password) as conn:
    # create_db(conn, database, user, password)
    # add_client(conn, 'Tom', 'Peters', '+49037488859')
    # add_client(conn, 'Tom', 'Peters' )
    # add_phone(conn, 3, ['+89037388859','+69997468859','asd','+79037488899'])
    # change_client(conn, 3,'Peter', 'Sad', ['123', '+79037488859', '2', '+79037488855'])
    # rem_phone(conn, 3,'+79037488855')
    # rem_client(conn, 4)
    # find_client(conn, 'Tom')
    # find_client(conn, 'Tom', 'Sad')
    # find_client(conn, 'Tom', 'Peters')
    # find_client(conn, 'Tom',  phone='+49037488859')
