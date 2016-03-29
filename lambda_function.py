from __future__ import print_function

# Pacotes necessarios
import base64
import json
import MySQLdb
import boto3
import botocore
import datetime
import string

print('Loading function')

# Funcao principal, recebe o contexto e uma variavel tipo dict chamada event
def lambda_handler(event, context):

    #print("Received event: " + json.dumps(event, indent=2))
    # Declaracao de variaveis
    default_table_name = 'TabelaNo'
    default_stream_name = '***'
    _user = '***'
    _password = '***'
    _host = '***'
    _database = 'Dados'

    # Conexao com mysql
    conn = MySQLdb.Connect(host= _host, user= _user, passwd= _password, db= _database)

    # Lista que acomoda dados a serem reenviados ao kinesis, se necessario
    kinesis_put_data = []

    # Iteracao sobre as leituras recebidas
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data'])
        data = string.split(payload, ':')

        try:

            # Tratamento para dados do tipo UPS
            # UPS:NO_SIGLA:TIMESTAMP:ALT:LAT:LON:SENSORES{s1;s2;....sn}
            if data[0] == 'UPS':

                # Inserindo no atraves de stored procedure
                cursor = conn.cursor()
                dt = datetime.datetime.fromtimestamp(int(data[2]))
                args = [data[1], dt.strftime('%Y%m%d%H%M%S')]
                cursor.callproc('ins_no', args)
                cursor.close()

                # Relacionando sensores ao no atraves de stored procedure
                sensors = string.split(data[6], ';')
                for i in range(len(sensors)):
                    cursor = conn.cursor()
                    args = [data[1], dt.strftime('%Y%m%d%H%M%S'), int(sensors[i])]
                    cursor.callproc('ins_no_sens', args)
                    cursor.close()

                # Atualizando historico do no atraves de stored procedure
                cursor = conn.cursor()
                args = [data[1], dt.strftime('%Y%m%d%H%M%S'), float(data[3]), float(data[4]), float(data[5])]
                cursor.callproc('att_no_hist', args)

            # Tratamento para dados do tipo DTS
            # DTS:NO_SIGLA:TIMESTAMP:SENSOR_ID:SENSOR_VALOR
            elif data[0] == 'DTS':

                # Recuperando id do no atraves de stored procedure
                dynamo_client = boto3.client('dynamodb')
                cursor = conn.cursor()
                args = [data[1]]
                cursor.callproc('get_no_id', args)
                no_id = cursor.fetchone()
                try:

                    # Checa estado da tabela
                    response = dynamo_client.describe_table ( TableName=default_table_name + str(no_id))

                    # Tabela existe mas nao esta ativa, dados serao recolocados no kinesis
                    if response['Table']['TableStatus'] != 'ACTIVE':
                        kinesis_data = dict()
                        kinesis_data['Data'] = bytes(''.join(data))
                        kinesis_data['PartitionKey'] = 1
                        kinesis_put_data.append(kinesis_data)

                    # Tabela ativa, dados serao inseridos
                    else:
                        dynamo_client.put_item(
                            TableName='TabelaNo' + str(no_id),
                            Item={
                                'id_sensor': int(data[3]),
                                'data': int(data[2]),
                                'valor': float(data[4])
                            }
                        )

                #Tabela nao existe, ela sera criada e dados serao recolocados no kinesis
                except:
                    dynamo_client.create_table(
                        AttributeDefinitions=[
                            {
                                'AttributeName': 'id_sensor',
                                'AttributeType': 'N'
                            },
                            {
                                'AttributeName': 'data',
                                'AttributeType': 'N'
                            }
                        ],
                        TableName= 'TabelaNo' + str(no_id),
                        KeySchema=[
                            {
                                'AttributeName': 'id_sensor',
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': 'data',
                                'KeyType': 'RANGER'
                            }
                        ],
                        ProvisionedThroughput={
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    )
                    kinesis_data = dict()
                    kinesis_data['Data'] = bytes(''.join(data))
                    kinesis_data['PartitionKey'] = 1
                    kinesis_put_data.append(kinesis_data)



            # Tratamento para dados do tipo PG
            # PG:NO_SIGLA:TIMESTAMP
            elif data[0] == 'PG':

                # Atualizando data de ultima mensagem do no atraves de stored procedure
                cursor = conn.cursor()
                dt = datetime.datetime.fromtimestamp(int(data[2]))
                args = [dt.strftime('%Y%m%d%H%M%S'), data[1]]
                cursor.callproc('att_no_data', args)
                cursor.close()
        except:
            pass

        print("Decoded payload: " + payload)

    # Checa se ha dados para serem enviados ao kinesis
    if kinesis_put_data:
        client = boto3.client('kinesis')
        client.put_records(
            Records=kinesis_put_data,
            StreamName=default_stream_name
        )

    return 'Successfully processed {} records.'.format(len(event['Records']))