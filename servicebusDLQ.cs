using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
// Purpose:     To copy values off of a dead letter queue and write them to a database
//              for easier analysis.                
// How to Use:  Script is HTTP triggered and so needs the correct path:
//              [GET,POST] http://{path}/api/{environment}/{queue}
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
namespace Company.Function
{
    public static class migration_dql_sync
    {
        [FunctionName("migration_dql_sync")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "{environment:alpha}/{queue:alpha}")] HttpRequest req, string environment, string queue,ILogger log)
        {
            log.LogInformation("DLQ sync function has been triggered by a HTTP request.");
            var responseMessage = "Default Return Message - if you are seeing this, something has gone wrong.";

            // Establish expanded variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
            //
            // // Environment connection string
            var dbConnection = "" ;
            var dlqConnection = "" ;
            if (environment == "env1") { dbConnection = Environment.GetEnvironmentVariable("DB-env1"); dlqConnection = Environment.GetEnvironmentVariable("DLQ-env1");}
            if (environment == "env2") { dbConnection = Environment.GetEnvironmentVariable("DB-env2"); dlqConnection = Environment.GetEnvironmentVariable("DLQ-env2");}
            if (environment == "env3") { dbConnection = Environment.GetEnvironmentVariable("DB-env3"); dlqConnection = Environment.GetEnvironmentVariable("DLQ-env3");}
            if (environment == "env4") { dbConnection = Environment.GetEnvironmentVariable("DB-env4"); dlqConnection = Environment.GetEnvironmentVariable("DLQ-env4");}
            if (environment == "env5") { dbConnection = Environment.GetEnvironmentVariable("DB-env5"); dlqConnection = Environment.GetEnvironmentVariable("DLQ-env5");}
            if (environment == "env6") { dbConnection = Environment.GetEnvironmentVariable("DB-env6"); dlqConnection = Environment.GetEnvironmentVariable("DLQ-env6");}
            //
            // // queue name
            var queueName = "";
            if (queue == "topic4"){queueName = string.Format($"{queue}");} //topic4 queue is the exception to below.
            else { queueName = string.Format($"migration-{queue}");} // Service Bus queue name format is migration-topic3 (for example)
            //
            // // Table Name
            var tableName = queue;
            // Validates the parameters passed are accurate.
            var ValidParamCheck = ValidateURL(environment,queue);
            //
            // Starting the process  -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
            if (ValidParamCheck == 1)
            {
                responseMessage = string.Format($"Correct Environment ({environment}) and Queue ({queue}) parameter passed in URL. Function Completed successfully.");
                if(ValidateTable(dbConnection,tableName) == 1)
                    {await QueryServiceBus(dlqConnection,queueName,dbConnection,tableName);}
                else{responseMessage = string.Format($"Issues with the database has failed validation.");}            
            }
            else {responseMessage = string.Format($"Error in Environment ({environment}) and/or Queue ({queue}) parameter passed in URL. Acceptable Environments: env1, env2, env3, env4, env5, env6. Acceptable Queues: topic1,topic2,topic3,topic4. Alternatively there may be an issue with the connection to the Azure Service Bus and or Integration Databases. ");}
            return (ActionResult)new OkObjectResult(responseMessage);
        }
        // validate parameters
        static int ValidateURL(string environment,string queue)
        {
            Console.WriteLine("Validating URL Parameters");
            // List of acceptable Environments
            var envCheck = false;
            List<string> envs = new List<string> { "env1", "env2", "env3", "env4", "env5", "env6" };
            // List of acceptable Queues  
            var queueCheck = false;
            List<string> queues = new List<string>{ "topic1", "topic2", "topic3", "topic4" };
            // Return value 
            var rValue = 0;
            //
            // Checks that the user has passed the correct parameters
            if (envs.Contains(environment)) { envCheck = true; } 
            if (queues.Contains(queue)) { queueCheck = true; }
            if (envCheck == true & queueCheck == true) { rValue = 1; }
            else { rValue = 0; }
            return (rValue); 
        }

        // Validates that tables exist, if not creates them.
        static int ValidateTable(string dbConnection,string tableName)
        {
            var checkSQLstring = "IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TEST' AND TABLE_NAME='dlq_"+ tableName +"') SELECT 1 AS res ELSE SELECT 0 AS res;";
            //
            Console.WriteLine("Validating if table exists.");
            // 
            using SqlConnection DBConnectionV =  new SqlConnection(dbConnection);
            DBConnectionV.Open();
            SqlCommand DBCommandV = new SqlCommand(checkSQLstring, DBConnectionV);
            Object queryTable = DBCommandV.ExecuteScalar();
            var queryResult = Convert.ToInt32(queryTable);
            DBConnectionV.Close();
            if (queryResult == 0) { 
                Console.WriteLine("Table Doesn't exist, creating table");
                var tableCreateSQL = "CREATE TABLE [test].[dlq_" + tableName + "] ([SequenceNumber] [int] NULL,[EnqueuedTime] [datetime] NULL,[DeliveryCount] [int] NULL,[ErrorReason] [varchar](max) NULL,[ErrorMessage] [varchar](max) NULL,[MessageBody] [varchar](max) NULL,[InsertDate] [datetime] NULL default getdate());";
                using SqlConnection DBConnectionT = new SqlConnection(dbConnection);
                DBConnectionT.Open();
                SqlCommand DBCommandT = new SqlCommand(tableCreateSQL, DBConnectionT);
                DBCommandT.ExecuteNonQuery();
                DBConnectionT.Close();
            }
            else
            {
                Console.WriteLine("Table exists, moving on.");
            }
            Console.WriteLine("Table Validation Complete");
            return 1;
        }

        // Query Service Bus
        static async Task QueryServiceBus(string dlqConnection, string queueName, string dbConnection, string tableName)
        {
            Console.WriteLine("Starting DLQ Query");
            var client = new ServiceBusClient(dlqConnection);
            var receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
            var stopper = false; // this flag helps break the reading of the service bus which will just continously read even with no messages.
            //
            // Query Service Bus
            Console.WriteLine("Querying DLQ");
            while (stopper == false)
            {
                var message = await receiver.PeekMessageAsync();
                if (Object.ReferenceEquals(null, message) == true)
                {
                    Console.WriteLine("Queue End Detected, ending script");
                    stopper = true;
                }
                else
                {  
                    WriteResults(dbConnection, tableName, message.SequenceNumber.ToString(), message.EnqueuedTime.DateTime, message.DeliveryCount.ToString(), message.DeadLetterReason.ToString(), message.DeadLetterErrorDescription.ToString(), message.Body.ToString());
                }
            }
            Console.WriteLine("Writing Done");
        }

        
        // Write to DB
        static void WriteResults(string dbConnection, string tableName, string SequenceNumber, DateTime EnqueuedTime, string DeliveryCount, string DeadLetterReason, string DeadLetterErrorDescription, string Body)
        {
            // Variables 
            var DBWriteCommands = "INSERT INTO [test].[dlq_" + tableName + "](SequenceNumber,EnqueuedTime,DeliveryCount,ErrorReason,ErrorMessage,MessageBody)VALUES ('" + SequenceNumber + "','" + EnqueuedTime + "','" + DeliveryCount + "','" + DeadLetterReason.Replace("'", "''") + "','" + DeadLetterErrorDescription.Replace("'", "''") + "','" + Body.Replace("'", "''") + "')";
            //
            // Write to tables
            Console.WriteLine("Writing rows to table");
            using SqlConnection DBConnectionW = new(dbConnection);
            SqlCommand DBCommandW = new(DBWriteCommands, DBConnectionW);
            DBConnectionW.Open();
            DBCommandW.ExecuteNonQuery();
            DBCommandW.Parameters.Clear();
            DBConnectionW.Close();
            //
        }
    }
}