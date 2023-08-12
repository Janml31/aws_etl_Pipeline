import pandas as pd
from faker import Faker
import random
import datetime

faker = Faker()

num_rows = 100
num_columns = 20

data = []
for _ in range(num_rows):
    row = [
        faker.first_name(),
        faker.last_name(),
        faker.company(),
        faker.job(),
        faker.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d'),
        faker.address(),
        faker.city(),
        faker.state(),
        faker.country(),
        faker.phone_number(),
        faker.email(),
        faker.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
        faker.random_int(min=1000, max=9999),
        faker.random_element(["High", "Medium", "Low"]),
        faker.random_element(["Pending", "In Progress", "Completed"]),
        faker.random_element(["Active", "Inactive"]),
        faker.random_element(["Sales", "Marketing", "Finance"]),
        faker.random_element(["Manager", "Supervisor", "Associate"]),
        faker.random_element(["Male", "Female"]),
        faker.random_element(["Yes", "No"])
    ]
    data.append(row)

column_names = [
    "first_name", "last_name", "company", "job_title", "date_of_birth",
    "address", "city", "state", "country", "phone_number", "email",
    "timestamp", "salary", "priority", "status", "employee_status",
    "department", "role", "gender", "is_active"
]

df = pd.DataFrame(data, columns=column_names)
csv_output_path = 'business_data.csv'
df.to_csv(csv_output_path, index=False)

print(f"Business data saved to {csv_output_path}")
