import boto3
import os
from pydub import AudioSegment
import ffmpeg
import time
import timeit
from botocore.exceptions import ClientError

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
QUEUE_URL = os.environ.get('QUEUE_URL')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
dir_path = os.path.dirname(os.path.realpath(__file__))
print(AWS_SECRET_ACCESS_KEY)
print(AWS_ACCESS_KEY_ID)
print(QUEUE_URL)
print(BUCKET_NAME)
print(dir_path)
# Create SQS client
sqs = boto3.client('sqs', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='us-east-1')
queue_url = QUEUE_URL
# Receive message from SQS queue
def receiveMessage():
    response = sqs.receive_message(
        QueueUrl=queue_url,
        VisibilityTimeout=10,
        WaitTimeSeconds=10
    )
    if 'Messages' in response:
        message = response['Messages'][0]["Body"]
        print(response)
        split = message.split(';;;')
        ruta = split[0]
        email = "da.babativa@uniandes.edu.co"
        if len(split)!=1:
            email = split[1]
        print(ruta,email)
        return (ruta,email, response['Messages'][0]["ReceiptHandle"])
    else:
        return None
        
def getFileTest():
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    #s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME') https://voicecontest.s3.amazonaws.com/originals/2022020_7ace19df-361f-4e20-a52d-ef611d9b4e19_Universidad_de_los_Andes.wav
    file =s3.download_file(BUCKET_NAME, 'originals/2022020_7ace19df-361f-4e20-a52d-ef611d9b4e19_Universidad_de_los_Andes.wav','unprocessed/2022020_7ace19df-361f-4e20-a52d-ef611d9b4e19_Universidad_de_los_Andes.wav')
    print(1)
    return '2022020_7ace19df-361f-4e20-a52d-ef611d9b4e19_Universidad_de_los_Andes.wav'

def getFile(message):
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    print(message)
    print(message.split('/')[1])    
    #s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')
    s3.download_file(BUCKET_NAME, message, 'unprocessed/'+message.split('/')[1])
    print(1)
    return message.split('/')[1]

def changeFileType(path):
    f_no_extension = path.split(".")[0]
    extension = path.split(".")[1]
    #'./unprocessed/{fi}'
    command = f"ffmpeg -i {dir_path}/unprocessed/{path} {dir_path}/processed/{f_no_extension}.mp3"
    print(command)
    os.system(command)
    delete_command = f"rm {dir_path}/unprocessed/{path}"
    os.system(delete_command)
    newPath = f_no_extension+'.mp3'
    return newPath

def uploadFileS3(newPath):
    print('uploadFile',newPath)
    uploadS3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    #response = uploadS3.upload_file(file_name, bucket, object_name)
    response = uploadS3.upload_file(f'{dir_path}/processed/'+newPath, BUCKET_NAME, 'processed/'+newPath)

def deleteMessage(receipt_handle):
    sqs.delete_message(
    QueueUrl=queue_url,
    ReceiptHandle=receipt_handle,
)

def sendEmailNotification(email):
    SENDER = "voice.contest.cloud@gmail.com"
    RECIPIENT = email
    AWS_REGION = "us-east-2"
    SUBJECT = "Voz Convertida"
    BODY_TEXT = ("Su voz ha sido convertida desde los servicios de Heroku\r\n"
                "Puede iniciar sesión para escucharla online en la plataforma"
                )
    CHARSET = "UTF-8"

    mail_client = boto3.client('ses', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='us-east-2')


    message = {
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            }

    try:
        response = mail_client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message = message,
            
            Source=SENDER,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def worker():
    (message, email, receiptHandle) = receiveMessage()
    if message is not None:
        path=getFile(message)
        print('File downloaded')
        newPath=changeFileType(path)
        print('File with new extension')
        deleteMessage(receiptHandle)
        # sendEmailNotification(email)
        if os.path.exists(f'{dir_path}/processed/'+newPath):
            uploadFileS3(newPath)
            print('File uploaded')
              
            print('Message deleted')


while True:
    worker()  