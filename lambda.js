console.log('Carregando função...');

//////////////////////////////////////////////////////////////////////////////////////////////////
//    Declaração de variaveis.
//////////////////////////////////////////////////////////////////////////////////////////////////
//    AWS:Variável para acesso ao sdk da AWS.
//    KINESIS: Variável para acesso as funções do kinesis.
//    DOC:Variável para acesso as funções do dynamoDB.
//    DYNAMODB:Variavel para acesso as funções do dynamoDB.
//    ASYNC:Package async, para controlar o fluxo das funções asincronas.
//    DADOS_BRUTOS:Dados pegos do stream do kinesis.
//    DADOS_KINESIS:Dados que serão recolocados no kinesis, caso tabela não se encontre acessível.
//    DADOS:Dados divididos em um array de string para melhor tratamento.
//    COMPRIMENTO_LEITURA:Número de dados contido em uma leitura de sensor.
//    PADRAO_NOME_TABELA:Nome padrao das tabelas do dynamoDB.
//    PADRAO_NOME_TABELA:Nome padrao da stream do kinesis
//////////////////////////////////////////////////////////////////////////////////////////////////
//    Outras variáveis são descritas em sua criação.

console.log('Criando variáveis...');

var aws = require('aws-sdk');
var kinesis = new aws.Kinesis({ apiVersion: '2013-12-02' });
var doc = require('dynamodb-doc');
var dynamodb = new doc.DynamoDB();
var async = require('async');

var dados_brutos = "";
var dados_kinesis = "";
var dados;
var comprimento_leitura = 4;
var padrao_nome_tabela = "TabelaNo";
var padrao_nome_stream = "StreamLeituras"

console.log('Função iniciada.');

exports.handler = function (event, context) {

    console.log('Lendo records do Kinesis...' + JSON.stringify(event, null, 2));

    //Aqui ocorre a leitura dos records contidos no kinesis, passados pelo evento que iniciou a função.
    //Os dados são concatenados em dados_brutos para futuro processamento.
    
    event.Records.forEach(function (record) {
        if (dados_brutos == "")
            dados_brutos = dados_brutos + new Buffer(record.kinesis.data, 'base64').toString('ascii');
        else
            dados_brutos = dados_brutos + " " + new Buffer(record.kinesis.data, 'base64').toString('ascii');
        console.log('Decoded payload:' + dados_brutos);
    });

    console.log('Decoded payload final:' + dados_brutos);
    //console.log('Decoded payload:', dados_brutos);
    console.log('Separando leituras...');
    
    //Os dados são separados pelos espaços contidos neles e colocados em um array de string

    dados = dados_brutos.split(" ");
    
    //Também é calculado o numero de leituras, considerando a quantidade de strings no array
    //    e o comprimento de uma leitura.
    
    var numero_leituras = dados.length / comprimento_leitura;
    var cont = 0;

    console.log(numero_leituras + ' leituras detectadas.');
    
    //Tudo ocore em uma função async whilst, que faz um teste lógico e decide qual caminho seguir.

    async.whilst(
        
        //Teste lógico. Verifica se o contador ja atingiu o número de leituras.
        //Caso o contador não tenha atingido, outra leitura é processada.
        
        function test() {
            console.log("teste n " + cont + " de " + numero_leituras);
            console.log(cont < numero_leituras);
            return cont < numero_leituras;
        },
        
        //Checa se a tabela na qual a leitura deve ir já existe.
        
        function checkTable(callback) {
            
            //Parametros para checar a tabela.
            
            var params_check = {
                TableName: padrao_nome_tabela + dados[cont * comprimento_leitura]
            };

            dynamodb.describeTable(params_check, function (err, data) {
                if (err) {
                    console.log(err);
                    
                    //Se ocorrer um erro ResourceNotFoundException a tabela não existe, deve ser criada.
                    
                    if (err.code == 'ResourceNotFoundException') {

                        console.log('Tabela ' + padrao_nome_tabela + dados[cont * comprimento_leitura] + ' não existe.');
                        
                        //Parametros de criação da tabela.

                        var params_create;
                        
                        //Funções colocadas em um async series para preservar a ordem correta de execução.
                        
                        async.series([
                            function defineParams(callback) {
                                console.log("criou params");
                                params_create = {
                                    AttributeDefinitions: [
                                        {
                                            AttributeName: 'id_sensor',
                                            AttributeType: 'N'
                                        },
                                        {
                                            AttributeName: 'data',
                                            AttributeType: 'N'
                                        }
                                    ],
                                    KeySchema: [
                                        {
                                            AttributeName: 'id_sensor',
                                            KeyType: 'HASH'
                                        },
                                        {
                                            AttributeName: 'data',
                                            KeyType: 'RANGE'
                                        }
                                    ],
                                    ProvisionedThroughput: {
                                        ReadCapacityUnits: 1,
                                        WriteCapacityUnits: 1
                                    },
                                    TableName: padrao_nome_tabela + dados[cont * comprimento_leitura]
                                };
                                callback(null);
                            },
                            
                            //Função que cria a tabela com os parametros fornecidos.
                            
                            function save(callback) {
                                console.log("Criando tabela...");
                                dynamodb.createTable(params_create, function (err, data) {
                                    if (err) {
                                        console.log('Erro ao criar tabela: ' + err.code);
                                        callback(null);
                                    }
                                    else {
                                        console.log('Tabela criada com sucesso!');
                                        callback(null);
                                    }
                                });
                            }],
                            
                            //Como a tabela esta sendo criada, os dados dessas leituras vão ser concatenados
                            //     e recolocados no kinesis futuramente.
                            
                            function kinesisPrepare() {
                                for (var i = 0; i < comprimento_leitura; i++) {
                                    dados_kinesis = dados_kinesis + dados[(cont * comprimento_leitura) + i] + " ";
                                    console.log("Kinesis prepare: da n " + i + " é " + dados[(cont * comprimento_leitura) + i]);
                                }
                                cont++;
                                callback(null);
                            })
                    }
                }
                else {
                    
                    //Checa se a tabela está ativa.
                    
                    if (data.Table.TableStatus == 'ACTIVE') {

                        console.log('Preparando inserção de dados...');
                        console.log("Parametros: a" + dados[(cont * comprimento_leitura) + 1] + " b" + dados[(cont * comprimento_leitura) + 2] + " c" + dados[(cont * comprimento_leitura) + 3]);

                        //Tabela está ativa, dados vão ser inseridos no dynamoDB.

                        async.series([
                            function insertLeitura(callback) {
                                
                                //Parametros para inserção na tabela.
                                
                                var params_insert = {
                                    Item: {
                                        id_sensor: parseInt(dados[(cont * comprimento_leitura) + 1]),
                                        data: parseInt(dados[(cont * comprimento_leitura) + 2]),
                                        valor: dados[(cont * comprimento_leitura) + 3]
                                    },
                                    TableName: padrao_nome_tabela + dados[cont * comprimento_leitura]
                                }
                                
                                //Função que insere os parametros fornecidos na tabela.
                                
                                dynamodb.putItem(params_insert, function (err, result) {
                                    if (err) {
                                        console.log('Ocorreu um erro ao inserir os dados: ' + err.code);
                                        callback(null);
                                    }
                                    if (result) {
                                        console.log('Dados inseridos com sucesso!');
                                        callback(null);
                                    }
                                });
                            }
                        ],
                            function contPlus() {
                                cont++;
                                callback(null);
                            })
                    }
                    else {
                        
                        //Como a tabela não está ativa ainda, os dados dessas leituras vão ser concatenados
                        //     e recolocados no kinesis futuramente.
                        
                        function kinesisPrepare() {
                            for (var i = 0; i < comprimento_leitura; i++) {
                                dados_kinesis = dados_kinesis + dados[(cont * comprimento_leitura) + i] + " ";
                                console.log("Kinesis prepare: da n " + i + " é " + dados[(cont * comprimento_leitura) + i]);
                            }
                            cont++;
                            callback(null);
                        }
                    }
                }
            });
        },
        
        //Função que checa se há dados que devem ser recolocados no kinesis
        
        function kinesisCheck() {
            async.series([
                function kinesisPut(callback) {
                    console.log(dados_kinesis);
                    if (dados_kinesis == "") {
                        callback(null);
                    }
                    else {
                        
                        //Algumas leituras são recolocadas no kinesis.
                        
                        dados_kinesis = dados_kinesis.slice(0, -1);
                        console.log(dados_kinesis);
                        
                        //Parametros para a inserção no kinesis.
                        
                        var params_put = {
                            Data: dados_kinesis,
                            PartitionKey: '1',
                            StreamName: padrao_nome_stream
                        };
                        console.log(params_put);
                        
                        //Função que insere os parametros passados no kinesis.
                        
                        kinesis.putRecord(params_put, function (err, data) {
                            if (err) {
                                console.log(err, err.stack); // an error occurred
                                callback(null);
                            }
                            else {
                                console.log(data);           // successful response
                                callback(null);
                            }
                        });
                    }
                }
            ],
            
                //Retorna uma mensagem de sucesso para o término da função.
            
                function suceedContext() {
                    console.log("Successfully processed " + event.Records.length + " records.");
                    context.succeed("Successfully processed " + event.Records.length + " records.");
                })
        })
};