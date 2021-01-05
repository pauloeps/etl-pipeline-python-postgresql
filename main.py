from sqlalchemy import create_engine
from getpass import getpass
import create_db as db
import pandas as pd

# Asks for the password
password='123456'
#password = getpass('Please enter the password for the superuser (postgres): ')
#db.create(password)

#Connecting to database
engine = create_engine('postgres://postgres:'+password+'@localhost:5432/org_data', echo=False)

# --- Extracting Data --- #
print('\n--- Extracting Data ---')

#Extracting employees data
emp_df=pd.read_sql_query('select * from emp', engine)
print('\n\nEmployees:\n')
print(emp_df.head(10))

#Extracting department info of employees
dept_df=pd.read_sql_query('select * from dept', engine)
print('\n\nDepartment:\n')
print(dept_df.head(10))

# --- Data transformation and Cleaning --- #
print('\n\n\n --- Data transformation and Cleaning ---')

#Function to calculate tax on salary of employees
def cal_taxes(sal):
	tax=0
	if sal >500 and sal <=1250:
 		tax=sal*.125
	elif sal>1250 and sal<=1700:
		tax=sal*.175
	elif sal>1700 and sal<=2500:
		tax=sal*.225
	elif sal>2500:
		tax=sal*.275
	else:
		tax=0
	return tax

#Creating a new column with the calculated tax
emp_df['Tax']=emp_df['sal'].map(cal_taxes)

#Cleaning of data: replaces NaN or nulls or 0 in comm with their
#respective salary values, otherwise it would impact future 
#calculations.
emp_df['comm']=emp_df[['sal','comm']].apply(lambda x: x[0]
    if(pd.isnull(x[1]) or int(x[1])==0) else x[1], axis=1)

#Calculate comm/sal percetage
emp_df['comm_%']=(emp_df['comm']/emp_df['sal'])*100

#100% comm values are invalid because the way the data was handled,
#so it is necessary to mark the data as valid or invalid.
emp_df['Comm_Flag']=emp_df[['sal','comm']].apply(lambda x: 'Invalid'
    if int(x[0])==int(x[1]) else 'Valid',axis=1)

#Now the data is filtered and seperate data frames are created.
comm_valid=emp_df[emp_df['Comm_Flag']=='Valid']
print('\n\n Valid data:\n')
print(comm_valid.head(5))

comm_invalid=emp_df[emp_df['Comm_Flag']=='Invalid']
print('\n\n Invalid data:\n')
print(comm_invalid.head(10))

#Calculate department wise average salary
agg_sal=emp_df.groupby(['deptno'])['sal'].mean()
print('\n\nAvg salary/dept:\n')
print(agg_sal)

#Combine the grouped dataset with original DataFrame
joined_df=pd.merge(emp_df,agg_sal,on='deptno')
joined_df.rename(columns=
{'sal_x':'sal','sal_y':'avg_sal'},inplace=True)
joined_df.drop(columns='Comm_Flag',inplace=True)
joined_df.sort_values('deptno')

#Creating a new job code for the DataFrame
job_map={'MANAGER':'MGR','PRESIDENT':'Country_Head','CLERK':'CLK',
    'ANALYST':'SDE2','SALESMAN':'Sales&Marketing'}
df=joined_df.replace({'job':job_map})

#Combining Data Sets of Employee and their respective Departments
final=pd.merge(df,dept_df[['deptno','dname','loc']],on='deptno',how='inner')

#Manipulate dept names, to get more cleanliness
dname_map={'RESEARCH':'R&D', 'SALES':'SALES', 'ACCOUNTING':'ACCT'}
final=final.replace({'dname':dname_map})

#Creating cleaned and final dataset
cleaned_df=final[['empno','ename','job','hiredate','sal','Tax','avg_sal','dname','loc']]

print('\n\nFinal and cleaned dataset:\n')
print(cleaned_df)

# --- Load to Target --- #
print('\n\n\n --- Load to Target ---')
cleaned_df.to_sql('emp_dept',con=engine,if_exists='replace',index=False)
print('\nQuerying the table...')
emp_df=pd.read_sql_query('select * from emp_dept', engine)
print('\n\nResult from query:\n')
print(emp_df.head(10))
print()