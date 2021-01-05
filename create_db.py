from sqlalchemy import create_engine, insert, Table, Column, Integer, String, MetaData, ForeignKey, Float, Date

emp_data = [
    [7369, 'SMITH', 'CLERK', 7902.0, '1980-12-17', 800.0, float('nan'), 20],
    [7499, 'ALLEN', 'SALESMAN', 7698.0, '1981-02-20', 1600.0, 300.0, 30],
    [7521, 'WARD', 'SALESMAN', 7698.0, '1981-02-22', 1250.0, 500.0, 30],
    [7566, 'JONES', 'MANAGER', 7839.0, '1981-04-02', 2975.0, float('nan'), 20],
    [7654, 'MARTIN', 'SALESMAN', 7698.0, '1981-09-28', 1250.0, 1400.0, 30],
    [7698, 'BLAKE', 'MANAGER', 7839.0, '1981-05-01', 2850.0, float('nan'), 30],
    [7782, 'CLARK', 'MANAGER', 7839.0, '1981-06-09', 2450.0, float('nan'), 10],
    [7788, 'SCOTT', 'ANALYST', 7566.0, '1987-04-19', 3000.0, float('nan'), 20],
    [7839, 'KING', 'PRESIDENT', float('nan'), '1981-11-17', 5000.0, float('nan'), 10],
    [7844, 'TURNER', 'SALESMAN', 7698.0, '1981-09-08', 1500.0, 0.0, 30]
]

dept_data = [
    [10, 'ACCOUNTING', 'NEW YORK'],
    [20, 'RESEARCH', 'DALLAS'],
    [30, 'SALES', 'CHICAGO'],
    [40, 'OPERATIONS', 'BOSTON']
]

def create(password):

    #Creating database org_data
    try:
        engine = create_engine('postgres://postgres:'+password+'@localhost:5432/postgres', echo=False)
        conn = engine.connect()
        conn.execute('commit')
        conn.execute('CREATE DATABASE org_data')
        print('Database created successfully!')
        conn.close()
    except:
        print('Failed to create the database, please check if you entered the correct password.')
        return False

    #Creating table
    try:
        engine = create_engine('postgres://postgres:'+password+'@localhost:5432/org_data', echo=False)
        conn = engine.connect()
        conn.execute('commit')
        print('Connected to database.')

        metadata = MetaData()
        emp = Table('emp', metadata,
            Column('empno', Integer, primary_key=True),
            Column('ename', String),
            Column('job', String),
            Column('mgr', Float),
            Column('hiredate', Date),
            Column('sal', Float),
            Column('comm', Float),
            Column('deptno', Integer)
        )

        dept = Table('dept', metadata,
            Column('deptno', Integer, primary_key=True),
            Column('dname', String),
            Column('loc', String)  
        )

        metadata.create_all(engine)

    except:
        print('Failed to create table on the database.')
        return False

    #Inserting rows on database tables
    try:
        for row in emp_data:
            ins = emp.insert().values(
                empno=row[0],
                ename=row[1],
                job=row[2],
                mgr=row[3],
                hiredate=row[4],
                sal=row[5],
                comm=row[6],
                deptno=row[7]
            )
            conn.execute(ins)

        for row in dept_data:
            ins = dept.insert().values(
                deptno=row[0],
                dname=row[1],
                loc=row[2]
            )
            conn.execute(ins)

        print('Rows inserted on database tables!')

    except:
        print('Failed to insert rows.')
        return False

    conn.close()
    return True

    

    