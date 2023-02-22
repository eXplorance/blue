using System;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Net;
using System.Reflection;

namespace Explorance_Blue_sample_API_Client
{
    public static class BlueWSImporter
    {
        /// <summary>
        /// Starts an import of a datasource by providing Datasource Id.
        /// This will create a transactionid from the backend (transaction table from Blue)
        /// Will return a Transaction Id
        /// </summary>
        /// <param name="BlueWSclient"></param>
        /// <param name="APIKey"></param>
        /// <param name="sourceID"></param>
        /// <returns></returns>
        public static long StartTransaction(BlueWS.BlueWebServiceClient BlueWSclient, string APIKey, string sourceID)
        {
            Security();

            long transactionID = 0;

            try
            {
                // By setting abortOnEmpyty to true, the import gets aborted by Blue if there is no data.
                bool abortOnEmpyty = true;
                bool replaceBlueRole = false;
                bool replaceDataSourceAccessKey = false;
                bool replaceLanguagesPreference = false;

                // call RegisterImport method
                BlueWSclient.RegisterImport(ref APIKey, abortOnEmpyty, sourceID, replaceBlueRole, replaceDataSourceAccessKey, replaceLanguagesPreference, out transactionID);
                BlueWSclient.Close();

                
            }
            catch (Exception ex)
            {
                AddLog("********** ERROR ***************");
                AddLog(string.Format(ex.Message));
                AddLog("********************************");
            }

            return transactionID;
        }

        /// <summary>
        /// Finalize the import given a transaction Id.
        /// This functions prepare the data and if the push is succesful, the import is complete.
        /// </summary>
        /// <param name="BlueWSclient"></param>
        /// <param name="APIKey"></param>
        /// <param name="transactionID"></param>
        public static void FinalizeTransaction(BlueWS.BlueWebServiceClient BlueWSclient, string APIKey, long transactionID)
        {
            Security();

            string result = "";
            bool isSuccess = false;
            bool hasWarningMessage = false;

            try
            {
                string ResultImport = BlueWSclient.PrepareDataToFinzalizeImportV2(APIKey, transactionID, out hasWarningMessage, out result, out isSuccess);

                if (isSuccess)
                {
                    string resultFinalize = BlueWSclient.FinalizeImport(ref APIKey, transactionID);
                }
                else
                {
                    string cancelResultF = BlueWSclient.CancelImport(ref APIKey, transactionID);
                }
            }
            catch (Exception ex)
            {
                AddLog("********** ERROR ***************");
                AddLog(string.Format(ex.Message));
                AddLog("********************************");
            }
            
            BlueWSclient.Close();
        }

        /// <summary>
        /// Push data to blue given a transaction id and a Data Table.
        /// </summary>
        /// <param name="BlueWSclient"></param>
        /// <param name="APIKey"></param>
        /// <param name="transactionID"></param>
        /// <param name="dataTable"></param>
        public static void PushDataFromDataTable(BlueWS.BlueWebServiceClient BlueWSclient, string APIKey, long transactionID, DataTable dataTable)
        {
            Security();

            try
            {
                string blocName = GetDataBlockName(BlueWSclient, APIKey);

                string result = "";
                bool isSuccess = false;
                bool hasWarningMessage = false;

                string[] intColumns = new string[dataTable.Columns.Count];
                int intRowNum = 0;

                BlueWS.IDataRow[] myData = new BlueWS.IDataRow[dataTable.Rows.Count];
                BlueWS.IDataRow data;

                foreach (DataRow row in dataTable.Rows)
                {
                    data = new BlueWS.IDataRow();
                    data.IDataRowValue = new BlueWS.IDataObj[intColumns.Length];

                    for (int ColNum = 0; ColNum < intColumns.Length; ColNum++)
                    {
                        data.IDataRowValue[ColNum] = new BlueWS.IDataObj();
                        data.IDataRowValue[ColNum].IDataObjValue = row[ColNum].ToString();
                    }

                    myData[intRowNum] = data;
                    intRowNum++;
                }

                string[] columnName = (from dc in dataTable.Columns.Cast<DataColumn>() select dc.ColumnName).ToArray();

                BlueWSclient.PushObjectDataV2(APIKey, columnName, blocName, transactionID, myData, out hasWarningMessage, out result, out isSuccess);
                BlueWSclient.Close();
            }
            catch (Exception ex)
            {
                AddLog("********** ERROR ***************");
                AddLog(string.Format(ex.Message));
                AddLog("********************************");
            }
            
        }
        
        /// <summary>
        /// Get the Id of a datasource by caption
        /// </summary>
        /// <param name="BlueWSclient"></param>
        /// <param name="APIKey"></param>
        /// <param name="caption"></param>
        /// <returns></returns>
        public static string GetDataSourceByCaption(BlueWS.BlueWebServiceClient BlueWSclient, string APIKey, string caption)
        {
            Security();

            BlueWS.IDataSource[] tmp;
            BlueWSclient.GetDataSourceList(ref APIKey, out tmp);

            foreach (BlueWS.IDataSource datasource in tmp)
            {
                if (datasource.Caption == caption)
                {
                    return datasource.SourceID;
                }
            }
            return String.Empty;
        }

        /// <summary>
        /// Get the name of the block by passing the data sourceid
        /// </summary>
        /// <param name="BlueWSclient"></param>
        /// <param name="APIKey"></param>
        /// <param name="sourceId"></param>
        public static string GetDataBlockName(BlueWS.BlueWebServiceClient BlueWSclient, string APIKey)
        {
            Security();

            string dataSourceId = BlueWSImporter.GetDataSourceByCaption(BlueWSclient, APIKey, ConfigurationManager.AppSettings["Blue:DataSourceName"]);

            string blockName = "";

            BlueWS.DataBlockInfo[] tmp;
            BlueWSclient.GetDataBlockInformation(ref APIKey, dataSourceId, out tmp);

            foreach (BlueWS.DataBlockInfo datasource in tmp)
            {
                blockName = datasource.DataBlockName;
                //break;
            }
            return blockName;
        }

        /// <summary>
        /// Exmple of pushing sample data using Data Object import
        /// Push data given a Transaction Id
        /// </summary>
        /// <param name="BlueWSclient"></param>
        /// <param name="APIKey"></param>
        /// <param name="transactionID"></param>
        public static void PushSampleDataFromObjects(BlueWS.BlueWebServiceClient BlueWSclient, string APIKey, long transactionID)
        {
            string blocName = GetDataBlockName(BlueWSclient, APIKey);

            string result = "";
            bool isSuccess = false;
            bool hasWarningMessage = false;

            // define the column names
            string[] columnName = new string[5];
            columnName[0] = "Userid_1";
            columnName[1] = "FirstName_1";
            columnName[2] = "LastName_1";
            columnName[3] = "emailAddress";
            columnName[4] = "BlueRole";

            // define the column types
            string[] columnType = new string[5];
            columnType[0] = "string";
            columnType[1] = "string";
            columnType[2] = "string";
            columnType[3] = "string";
            columnType[4] = "string";

            
            //create three data objects
            BlueWS.IDataRow[] myData = new BlueWS.IDataRow[3];
            BlueWS.IDataRow data = new BlueWS.IDataRow();
            data.IDataRowValue = new BlueWS.IDataObj[5];

            data.IDataRowValue[0] = new BlueWS.IDataObj();
            data.IDataRowValue[0].IDataObjValue = "000001";
            data.IDataRowValue[1] = new BlueWS.IDataObj();
            data.IDataRowValue[1].IDataObjValue = "FirstName 1";
            data.IDataRowValue[2] = new BlueWS.IDataObj();
            data.IDataRowValue[2].IDataObjValue = "LastName 1";
            data.IDataRowValue[3] = new BlueWS.IDataObj();
            data.IDataRowValue[3].IDataObjValue = "user1@test.org";
            data.IDataRowValue[4] = new BlueWS.IDataObj();
            data.IDataRowValue[4].IDataObjValue = "3";
            myData[0] = data;

            BlueWSclient.PushObjectDataV2(APIKey, columnName, blocName, transactionID, myData, out hasWarningMessage, out result, out isSuccess);
            BlueWSclient.Close();
        }

        /// <summary>
        /// Exmple of pushing sample data using CSV file import
        /// Push data given a Transaction Id
        /// </summary>
        /// <param name="BlueWSclient"></param>
        /// <param name="APIKey"></param>
        /// <param name="transactionID"></param>
        /// <param name="csvFilename"></param>
        public static void PushDataFromCSV(BlueWS.BlueWebServiceClient BlueWSclient, string APIKey, long transactionID, string csvFilename)
        {
            //calling security fonction
            Security();

            // define the column names
            string[] columnName = new string[5];
            columnName[0] = "Userid_1";
            columnName[1] = "FirstName_1";
            columnName[2] = "LastName_1";
            columnName[1] = "emailAddress";
            columnName[2] = "BlueRole";

            // define the column types
            string[] columnType = new string[5];
            columnType[0] = "string";
            columnType[1] = "string";
            columnType[2] = "string";
            columnType[1] = "string";
            columnType[2] = "string";

            //create a fileStream to read the CSV file containing the users
            FileStream fs = new FileStream(csvFilename, FileMode.Open, FileAccess.Read);
            
            DataTable res = ConvertCSVtoDataTable(csvFilename);
            
            PushDataFromDataTable(BlueWSclient, APIKey, transactionID, res);

            fs.Close();
        }

        /// <summary>
        /// Fonction to convert csv into DataTable 
        /// Passing the csv location to creates the columns and rows from the file.
        /// </summary>
        public static DataTable ConvertCSVtoDataTable(string strFilePath)
        {
            Security();

            string csvFile = ConfigurationManager.AppSettings["File:CSVFileLocation"];

            StreamReader sr = new StreamReader(csvFile);
            string[] headers = sr.ReadLine().Split(',');
            DataTable dt = new DataTable();

            try
            {
                
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }
                while (!sr.EndOfStream)
                {
                    string[] rows = Regex.Split(sr.ReadLine(), ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }
            }
            catch (Exception ex)
            {
                AddLog("********** ERROR ***************");
                AddLog(string.Format(ex.Message));
                AddLog("********************************");
            }            
            return dt;
        }

        /// <summary>
        /// Creating fonction to handle not https certificate, this can be used for local testing.
        /// Adds all tls protocols
        /// </summary>
        public static void Security()
        {
            //this can be removed if the SSL certificate is valid (for a production implementation)
            System.Net.ServicePointManager.ServerCertificateValidationCallback +=
            (se, cert, chain, sslerror) =>
            {
                return true;
            };

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls
           | SecurityProtocolType.Tls11
           | SecurityProtocolType.Tls12
           | SecurityProtocolType.Ssl3;            
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
