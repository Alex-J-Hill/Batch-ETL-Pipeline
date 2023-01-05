import json
import boto3
import time
import subprocess
from send_email import send_email

def lambda_handler(event, context):
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='wcdmidterminputajh')['Contents']:
        s3_file_list.append(object['Key'])
    print(s3_file_list)
    
    #datestr = time.strftime("%Y-%m-%d")
    datestr = '2022-10-10'
    print('Datestr: ' + datestr)
    
    
    required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv']
    print('required_file_list:')
    print(required_file_list)
    print(all(elem in s3_file_list for elem in required_file_list))
    
    # scan S3 bucket
    #if s3_file_list==required_file_list:
    if all(elem in s3_file_list for elem in required_file_list):
        s3_file_url = ['s3://' + 'wcdmidterminputajh/' + a for a in required_file_list]
        print('s3_file_url:')
        print(s3_file_url)
        table_name = [a[:-15] for a in required_file_list]
        #print(len(table_name))
        #print(len(s3_file_url))
        #data = json.dumps({'conf':{a:b for a in table_name for b in s3_file_url}})
        data = json.dumps({'conf':{a:b for a, b in zip(table_name, s3_file_url)}})
        print(data)
    # send signal to Airflow    
        endpoint= 'http://54.196.218.28/api/v1/dags/midterm_dag/dagRuns'
        subprocess.run(['curl', '-X', 'POST', endpoint, '-H', 'Content-Type: application/json', "--user", "airflow:airflow", '-d', data])
        #subprocess.run(['curl', '--verbose', 'http://ec2-3-91-17-66.compute-1.amazonaws.com/api/v1/dags', '-H', 'content-type: application/json', '--user', "airflow:airflow"])
        print('Files are sent to Airflow')
    else:
        send_email()
