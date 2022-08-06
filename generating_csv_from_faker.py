from faker import Faker
import csv
import random
import time
import os

if os.path.exists('source_csv'):
    for files in os.scandir('source_csv'):
        os.remove(files)
else:
    os.makedirs('source_csv')

fake = Faker(locale=['en_US'])

file_names = ['file' + str(i) + '.csv' for i in range(100,1100,100)]

header = ['s/no', 'date_employed', 'f_name', 'l_name', 'gender', 'job_role', 'job_level', 'salary(in $)', 'country', 'email']
data_profession = ['data engineer', 'data analyst', 'data scientist', 'data architect', 'statistician', 'database adminstrator']

for filename in file_names:
    data = []
    for i in range(1, 16):
        data.append((i, fake.date(pattern='%d-%m-%Y'),fake.first_name(), fake.last_name(),\
                    ''.join(random.choices(('M', 'F'))),''.join(random.choices(data_profession)), random.randrange(1,6),\
                    random.randint(50000,150000), fake.country(),fake.email(domain='gmail.com')))
    
    time.sleep(8)

    with open(f'source_csv/{filename}', 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(header)
        writer.writerows(data)
    print('\nDone wriing file:', filename)


print('\n\nDone writing 10 files')





