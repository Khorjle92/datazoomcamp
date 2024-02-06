# datazoomcamp workshop

# Q1 Q2
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

limit = 13
generator = square_root_generator(limit)
sum = 0
for seq,sqrt_value in enumerate(generator, start=1):
    print(f'seq {seq} : {sqrt_value}')
    sum = sum + sqrt_value
    print(f'sum is {sum}')

# Q3

import dlt
pipeline = dlt.pipeline(pipeline_name="people",
						destination='duckdb', 
						dataset_name='people_details')

--run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_1, 
                    table_name="users", 
                    write_disposition="replace")
info = pipeline.run(people_2, 
                    table_name="users", 
                    write_disposition="append"
                    )
--show the outcome
print(info)

conn  = duckdb.connect(f'{pipeline.pipeline_name}.duckdb')

details = conn.sql("SELECT * from people_details.users").df()
display(details)
age = conn.sql("SELECT SUM(AGE) from people_details.users").df()
display(age)


# Q4
import dlt
pipeline = dlt.pipeline(pipeline_name="people",
						destination='duckdb', 
						dataset_name='people_details2')

--run the pipeline with default settings, and capture the outcome
info = pipeline.run(people_1, 
                    table_name="users", 
                    write_disposition="replace")
info = pipeline.run(people_2, 
                    table_name="users", 
                    write_disposition="merge",
                    primary_key = 'id'
                    
                    )
--show the outcome
print(info)

conn  = duckdb.connect(f'{pipeline.pipeline_name}.duckdb')

details = conn.sql("SELECT * from people_details2.users").df()
display(details)
age = conn.sql("SELECT SUM(AGE) from people_details2.users").df()
display(age)