using System;
using System.IO;
using System.ServiceModel;
using System.Reflection;
using System.Configuration;
using System.Data;
using Newtonsoft.Json;
using System.Net;
using System.Collections.Generic;
using System.Linq;

/// <summary>
/// In this class there's 3 ways of pushing data to a single data source.
/// Using a Data Object: Expand the object region below and uncommend the code to use it. 
///                      Comment back again when swtiching the data mode
/// This is by calling the function PushSampleDataFromObjects that is part of the BlueWSImporter class.
/// 
/// Using a CSV file: Expand the CSV region below and uncommend the code to use it. 
///                   Comment back again when swtiching the data mode
/// This is by calling the function PushDataFromCSV that is part of the BlueWSImporter class.
/// 
/// Using JSON file: Expand the JSON region below and uncommend the code to use it. 
///                  Comment back again when swtiching the data mode
/// 
/// This is by calling the function PushDataFromDataTable that is part of the BlueWSImporter class.
/// It also has a StreamReader readJson that Deserialize the data Object
/// 
/// </summary>
namespace Explorance_Blue_sample_API_Client
{    
    class Program
    {
        public static void Main(string[] args)
        {
            //Calling the Security function from the BlueWSImporter class.
            BlueWSImporter.Security();

            try
            {
                AddLog("********** START");

                string EndpointName = "WSHttpBinding_IBlueWebService1";
                string RemoteAddress = ConfigurationManager.AppSettings["WS:EndpointAddress"];
                BlueWS.BlueWebServiceClient BlueWSclient = new BlueWS.BlueWebServiceClient(EndpointName, new EndpointAddress(RemoteAddress));
                string APIKey = GetApiKey();
                
                AddLog("Get Datasource By Caption");

                long transactionID;
                string dataSourceId = BlueWSImporter.GetDataSourceByCaption(BlueWSclient, APIKey, ConfigurationManager.AppSettings["Blue:DataSourceName"]);

                AddLog("Start Import");

                BlueWSclient = new BlueWS.BlueWebServiceClient(EndpointName, new EndpointAddress(RemoteAddress));
                APIKey = GetApiKey();
                transactionID = BlueWSImporter.StartTransaction(BlueWSclient, APIKey, dataSourceId);

                AddLog("Start Data Push");

                #region Use Data from JSON
                //string jsonData = ConfigurationManager.AppSettings["File:JsonFileLocation"];

                ////local json sample
                ////string jsonData = @"{
                ////    'data' : [{  
                ////    'Userid':'000001',
                ////    'FirstName':'FirstName 1',  
                ////    'LastName':'LastName 1',
                ////    'emailAddress':'user1@test.org',
                ////    'BlueRole':'528'
                ////    }]}";

                //using (StreamReader readJson = new StreamReader(ConfigurationManager.AppSettings["File:JsonFileLocation"]))
                //{
                //    string json = readJson.ReadToEnd();

                //    DataSet ds = JsonConvert.DeserializeObject<DataSet>(json);
                //    DataTable dataTable = ds.Tables["data"];

                //    BlueWSclient = new BlueWS.BlueWebServiceClient(EndpointName, new EndpointAddress(RemoteAddress));
                //    APIKey = GetApiKey();

                //    BlueWSImporter.PushDataFromDataTable(BlueWSclient, APIKey, transactionID, dataTable);

                //}

                #endregion

                #region Use Sample Objects data
                //BlueWSclient = new BlueWS.BlueWebServiceClient(EndpointName, new EndpointAddress(RemoteAddress));
                //APIKey = GetApiKey();
                //BlueWSImporter.PushSampleDataFromObjects(BlueWSclient, APIKey, transactionID);
                #endregion

                #region Use Sample CSV File           

                //string csvFile = ConfigurationManager.AppSettings["File:CSVFileLocation"];
                //BlueWSclient = new BlueWS.BlueWebServiceClient(EndpointName, new EndpointAddress(RemoteAddress));
                //APIKey = GetApiKey();
                //BlueWSImporter.PushDataFromCSV(BlueWSclient, APIKey, transactionID, csvFile);

                #endregion use csv file

                AddLog("Finalize Import");

                BlueWSclient = new BlueWS.BlueWebServiceClient(EndpointName, new EndpointAddress(RemoteAddress));
                APIKey = GetApiKey();
                BlueWSImporter.FinalizeTransaction(BlueWSclient, APIKey, transactionID);

                AddLog("********** SUCCESSFULLY COMPLETED");
            }
            catch (Exception ex)
            {
                AddLog("********** ERROR ***************");
                AddLog(string.Format(ex.Message));
                AddLog("********************************");
            }
        }

        /// <summary>
        /// Fonction that gets the api key from the AppSettings.config file
        /// </summary>
        public static string GetApiKey()
        {
            string apiKey = ConfigurationManager.AppSettings["Blue:APIKey"];
            return apiKey;
        }
        /// <summary>
        /// Fonction that creates a log when trying to perform a data push.
        /// </summary>
        public static void AddLog(string logMessage)
        {
            try
            {
                string logFileName = string.Format("{0}/ImportLog.log", System.IO.Path.GetDirectoryName(Assembly.GetEntryAssembly().Location));
                string logMsg = string.Format("{0} : {1}", DateTime.Now.ToString(), logMessage);
                using (StreamWriter w = File.AppendText(logFileName))
                {
                    w.WriteLine(logMsg);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);

            }
        }
    }
}
