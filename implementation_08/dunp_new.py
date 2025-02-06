#############################################
# 2025 Winter                               #
# CSC 453 - Principles of Database Systems  #
# Instructor : Daniel Lee                   #
#                                           #
# Group Project 01 - Group 08               #
#############################################

import pymysql
import random
import time

# data fetch function when the tuple has one element
def fetch_data_one(cursor, query, params=None):
    cursor.execute(query, params or ())
    res = cursor.fetchall()
    res = list(item[0] for item in res)
    return res


# data fetch function when the tuple has many elements
def fetch_data_many(cursor, query, params=None):
    cursor.execute(query, params or ())
    res = cursor.fetchall()
    res = list(item for item in res)
    return res


# randomly select samples
def random_sample(data, k):
    return random.sample(data, min(k, len(data)))


def main():

    # number of times the program is run
    running_num = 10
    running_times = []  # list storing running times

    is_sample = False    # True: when program uses sample participants | False: when program uses whole participants
    sample_num = 100

    for i in range(running_num):

        print(f'{i+1}/{running_num} - Program starts!')
        start_time = time.time()

        # 1. Establish database connection
        conn = pymysql.connect(host='127.0.0.1', user='root', password='9078', database='gp_08')
        cursor = conn.cursor()

        # 2. Fetch distinct employee IDs
        distinct_employees = fetch_data_one(cursor, "SELECT DISTINCT employeeid FROM survey_result")

        # 3. Select sample employees.
        # If sampling, randomly select sample_num participants; otherwise, use distinctEmployees as is.
        #######sampled_employees = [101, 21034, 21685, 26382, 26561, 26752, 26849, ]
        if is_sample:
            print('# Sampled data selected #')
            sampled_employees = random_sample(distinct_employees, sample_num)
        else:
            print('# Whole data selected #')
            sampled_employees = distinct_employees

        # 4. Extract attribute IDs for AbsentEmployeeReason and Certifications
        [attr_id_cert, attr_id_aer] = fetch_data_one(cursor, """SELECT attributeid
                                         FROM survey_attribute
                                         WHERE attributename = 'Certifications' 
                                         OR attributename = 'AbsentEmployeeReason'""")

        # 5. Initialize an empty array
        insertData = []

        try:
            # 6. Process each employee
            for emp in sampled_employees:

                employee_id = emp

                # 6a. Fetch AbsentEmployeeReason
                absent_query = """
                    SELECT vs.valuedescription FROM survey_result sr
                        JOIN value_set vs ON sr.valuecode = vs.valuecode
                        WHERE employeeid = %s AND attributeid = %s
                    """
                absent_employee_reason = fetch_data_one(cursor, absent_query, (employee_id, attr_id_aer))

                # 6b. Fetch Certifications
                cert_query = """
                    SELECT vs.valuedescription FROM survey_result sr
                        JOIN value_set vs ON sr.valuecode = vs.valuecode
                        WHERE employeeid = %s AND attributeid = %s"""
                certifications = fetch_data_one(cursor, cert_query, (employee_id, attr_id_cert))

                # 6c. Fetch employee details when either one has value
                if absent_employee_reason or certifications:
                    if not absent_employee_reason:
                        #absent_employee_reason = ''
                        absent_employee_reason = None
                    else:
                        absent_employee_reason = absent_employee_reason[0]

                    if not certifications:
                        #certifications = ''
                        certifications = None
                    else:
                        certifications = certifications[0]

                    emp_query = "SELECT id, firstname, lastname FROM employee_name WHERE id = %s"
                    emp_details = fetch_data_many(cursor, emp_query, employee_id)[0]

                    # 6d. Combine data
                    record = {'EmployeeId': emp_details[0], 'FirstName': emp_details[1], 'LastName': emp_details[2],
                              'AbsentEmployeeReason': absent_employee_reason, 'Certifications': certifications}
                    insertData.append(record)

            # 7. Empty survey_report table
            cursor.execute("TRUNCATE TABLE survey_report")

            # 8. Insert data to survey_report
            for record in insertData:
                insert_query = """
                INSERT INTO survey_report (EmployeeId, FirstName, LastName, AbsentEmployeeReason, Certifications)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (record['EmployeeId'], record['FirstName'], record['LastName'],
                                              record['AbsentEmployeeReason'], record['Certifications']))

            # 9. commit the changes to the database
            conn.commit()
            print('Finish updating survey report table')

        except pymysql.MySQLError as e:
            print(f"Error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

        end_time = time.time()
        total_time = end_time - start_time
        running_times.append(total_time)
        print(f"Execution time: {total_time:.5f} seconds")

    avg_time = sum(running_times) / len(running_times)
    print(f"Average Execution time: {avg_time:.5f} seconds")


if __name__ == "__main__":
    main()
