using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data.SqlClient;
using System.Data;

using System.IO.Compression;



using System.IO;
using System.Net;
using LCL.Base;

using ICSharpCode.SharpZipLib;
using ICSharpCode.SharpZipLib.Zip;
using ICSharpCode.SharpZipLib.Core;

using System.Xml;


namespace GetFamilyXml2DB
{
    class XMLClient
    {

        string ftp;
        string id;
        string pwd;
        string unzipPwd;
        string downloadPath;
        string dbString;
        string ecKey;
        string execDate;
        PROCESS_TYPE type;
        ProcessLog pl;

        public XMLClient( string ftp,
                        string id,
                        string pwd,
                        string unzipPwd,
                        string downloadPath,
                        string dbString ) {

            this.ftp = ftp;
            this.id = id;
            this.pwd = pwd;
            this.unzipPwd = unzipPwd;
            this.downloadPath = downloadPath;
            this.dbString = dbString;
            ecKey = Config.getConfig("EC_KEY");
            execDate = Config.getConfig("EXEC_DATE");

            if (execDate == "") { execDate = DateTime.Now.ToString("yyyyMMdd"); }

            pl= new ProcessLog(execDate + ".log");

        }

        public enum PROCESS_TYPE {
                    R00,R04,RS4,RS9,R08,R22,R23,R96,R89,R99};

        /// <summary>
        /// 下載XML檔案
        /// </summary>
        public void getXmlFiles( ) {

            string[] arr = new string[] { "R22", "R23", "R04", "RS9", "RS4", "R96", "R08","R89","R99" };
            string filePath = "";
            string fileName = "";
            string okPath = "";
            string returnPath = ""; //回傳下載的路徑

            try
            {
                //批次操作
              
                pl.startLog();
                for (int i = 0; i < arr.Length - 1; i++)
                {
                    this.type = (PROCESS_TYPE)Enum.Parse(typeof(PROCESS_TYPE),arr[i]);
                    filePath = this.ftp + "/" + arr[i] + "/WORK/";
                    okPath = this.ftp + "/" + arr[i] + "/OK/";
                    // fileName = arr[i] + this.ecKey + "DFM" + DateTime.Now.ToString("yyyyMMdd") + ".XML.zip";
                    //測試用
                    fileName = arr[i] + this.ecKey + "DFM" + execDate + ".XML.zip";
                    pl.logMessage("\r\n開始下載檔案:" + filePath + fileName );
                    returnPath  = downloadFile(filePath , fileName,okPath );                   
                    pl.logMessage("完成下載檔案:" + filePath + fileName);
                    if (returnPath != "") {
                        pl.logMessage("\r\n開始讀入檔案:" + returnPath);
                        processFile(returnPath);
                        pl.logMessage("完成讀入檔案:" + returnPath);
                    }
                    
                }
                pl.endLog();
            }
            catch (Exception e) {
                FileLog.logError( e.ToString());
            }

        }

        #region 處理各檔案的內容



        /// <summary>
        /// 處理下載完成後的檔案
        /// </summary>
        /// <param name="filePath"></param>
        public void processFile(string filePath) {

            try
            {
               
                switch (this.type)
                {
                    
                    case PROCESS_TYPE.R22:
                        pl.logMessage("開始新增R22資料");
                        processR22(filePath);
                        pl.logMessage("完成新增R22資料");
                        break;
                    case PROCESS_TYPE.R23:
                        pl.logMessage("開始新增R23資料");
                        processR23(filePath);
                        pl.logMessage("完成新增R23資料");
                        break;
                    case PROCESS_TYPE.R04:
                        pl.logMessage("開始新增R04資料");
                        processR04(filePath);
                        pl.logMessage("完成新增R04資料");
                        break;
                    case PROCESS_TYPE.RS9:
                        pl.logMessage("開始新增RS9資料");
                        processRS9(filePath);
                        pl.logMessage("完成新增RS9資料");
                        break;
                    case PROCESS_TYPE.RS4:
                        pl.logMessage("開始新增RS4資料");
                        processRS4(filePath);
                        pl.logMessage("完成新增RS4資料");
                        break;
                    case PROCESS_TYPE.R96:
                        pl.logMessage("開始新增R96資料");
                        processR96(filePath);
                        pl.logMessage("完成新增R96資料");
                        break;
                    case PROCESS_TYPE.R08:
                        pl.logMessage("開始新增R08資料");
                        processR08(filePath);
                        pl.logMessage("完成新增R08資料");
                        break;
                    case PROCESS_TYPE.R99:
                        pl.logMessage("開始新增R99資料");
                        processR99(filePath);
                        pl.logMessage("完成新增R99資料");
                        break;
                    case PROCESS_TYPE.R89:
                        pl.logMessage("開始新增R89資料");
                        processR89(filePath);
                        pl.logMessage("完成新增R89資料");
                        break;
                }
            }

            catch (XmlException ex)
            {
                pl.logMessage(ex.Message);
            }
            catch (Exception ex) {
                pl.logMessage(ex.Message );
            }
           

        }


        #region R04

        private void processR04(string filePath) {

            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string ShipmentNo = "";
                string DCReceiveDate = "";
                string DCReceiveStatus = "";
                string FlowType = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//R04");
                if (xnode != null) {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        ShipmentNo = node2.SelectSingleNode("ShipmentNo").InnerText;
                        DCReceiveDate = node2.SelectSingleNode("DCReceiveDate").InnerText;
                        DCReceiveStatus = node2.SelectSingleNode("DCReceiveStatus").InnerText;
                        FlowType = node2.SelectSingleNode("FlowType").InnerText;

                        sql = @"INSERT INTO Order_Transport_Family_r04(PRDT,ParentId,EshopId,ShipmentNo,DCReceiveDate,DCReceiveStatus,FlowType)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + ShipmentNo + "','" + DCReceiveDate + "','" + DCReceiveStatus + "','" + FlowType + "')";
                        Execute(sql);
                    }
                }
              
              
            }
            catch (Exception ex) {

                pl.logMessage(ex.Message );
            }

        }

        #endregion

        #region RS9

        private void processRS9(string filePath)
        {
            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string ShipmentNo = "";
                string ReceiveStoreId = "";
                string DCReceiveDate = "";
                string DCReceiveStatus = "";
                string StatusDetails = "";
                string StatusRemark = "";
                string FlowType = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//RS9");
                if (xnode != null)
                {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        ShipmentNo = node2.SelectSingleNode("ShipmentNo").InnerText;
                        ReceiveStoreId = node2.SelectSingleNode("ReceiveStoreId").InnerText;
                        DCReceiveDate = node2.SelectSingleNode("DCReceiveDate").InnerText;
                        DCReceiveStatus = node2.SelectSingleNode("DCReceiveStatus").InnerText;
                        StatusDetails = node2.SelectSingleNode("StatusDetails").InnerText;
                        StatusRemark = node2.SelectSingleNode("StatusRemark").InnerText;
                        FlowType = node2.SelectSingleNode("FlowType").InnerText;

                        sql = @"INSERT INTO Order_Transport_Family_RS9(PRDT,ParentId,EshopId,ShipmentNo,ReceiveStoreId,DCReceiveDate,DCReceiveStatus,StatusDetails,StatusRemark,FlowType)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + ShipmentNo + "','" + ReceiveStoreId + "','" + DCReceiveDate + "','" + DCReceiveStatus + "','" 
                                     + StatusDetails + "','" + StatusRemark + "','" + FlowType + "')";
                        Execute(sql);
                    }
                }


            }
            catch (Exception ex)
            {

                pl.logMessage(ex.Message);
            }
        }

        #endregion

        #region RS4

        private void processRS4(string filePath)
        {
            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string ShipmentNo = "";
                string ReceiveStoreId = "";
                string DCReceiveDate = "";
                string DCReceiveStatus = "";
                string FlowType = "";
                string StoreType = "";
                string StoreName = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//RS4");
                if (xnode != null)
                {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        ShipmentNo = node2.SelectSingleNode("ShipmentNo").InnerText;
                        ReceiveStoreId = node2.SelectSingleNode("ReceiveStoreId").InnerText;
                        DCReceiveDate = node2.SelectSingleNode("DCReceiveDate").InnerText;
                        DCReceiveStatus = node2.SelectSingleNode("DCReceiveStatus").InnerText;
                        FlowType = node2.SelectSingleNode("FlowType").InnerText;
                        StoreType = node2.SelectSingleNode("StoreType").InnerText;
                        StoreName = node2.SelectSingleNode("StoreName").InnerText;                        

                        sql = @"INSERT INTO Order_Transport_Family_RS4(PRDT,ParentId,EshopId,ShipmentNo,ReceiveStoreId,DCReceiveDate,DCReceiveStatus,FlowType,StoreType,StoreName)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + ShipmentNo + "','" + ReceiveStoreId + "','" + DCReceiveDate + "','" + DCReceiveStatus + "','" 
                                     + FlowType + "','" + StoreType + "','" + StoreName + "')";
                        Execute(sql);
                    }
                }


            }
            catch (Exception ex)
            {

                pl.logMessage(ex.Message);
            }
        }

        #endregion

        #region R96

        private void processR96(string filePath)
        {
            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string StoreId = "";
                string SPDate = "";
                string SPAmount = "";
                string ServiceType = "";
                string ShipmentNo = "";
                string FlowType = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//R96");
                if (xnode != null)
                {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        StoreId = node2.SelectSingleNode("StoreId").InnerText;
                        SPDate = node2.SelectSingleNode("SPDate").InnerText;
                        SPAmount = node2.SelectSingleNode("SPAmount").InnerText;
                        ServiceType = node2.SelectSingleNode("ServiceType").InnerText;
                        ShipmentNo = node2.SelectSingleNode("ShipmentNo").InnerText;
                        FlowType = node2.SelectSingleNode("FlowType").InnerText;

                        sql = @"INSERT INTO Order_Transport_Family_R96(PRDT,ParentId,EshopId,ShipmentNo,StoreId,SPDate,SPAmount,ServiceType,FlowType)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + ShipmentNo + "','" + StoreId + "','" + SPDate + "','" + SPAmount + "','" + ServiceType + "','" + FlowType + "')";
                        Execute(sql);
                    }
                }


            }
            catch (Exception ex)
            {

                pl.logMessage(ex.Message);
            }


        }

        #endregion

        #region R08

        private void processR08(string filePath)
        {
            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string ShipmentNo = "";
                string TotalAmount = "";
                string DCReturnDate = "";
                string DCReturnStatus = "";
                string FlowType = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//R08");
                if (xnode != null)
                {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        ShipmentNo = node2.SelectSingleNode("ShipmentNo").InnerText;
                        TotalAmount = node2.SelectSingleNode("TotalAmount").InnerText;
                        DCReturnDate = node2.SelectSingleNode("DCReturnDate").InnerText;
                        DCReturnStatus = node2.SelectSingleNode("DCReturnStatus").InnerText;
                        FlowType = node2.SelectSingleNode("FlowType").InnerText;

                        sql = @"INSERT INTO Order_Transport_Family_R08(PRDT,ParentId,EshopId,ShipmentNo,TotalAmount,DCReturnDate,DCReturnStatus,FlowType)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + ShipmentNo + "','" + TotalAmount + "','" + DCReturnDate + "','" + DCReturnStatus + "','" + FlowType + "')";
                        Execute(sql);
                    }
                }


            }
            catch (Exception ex)
            {

                pl.logMessage(ex.Message);
            }
        }

        #endregion

        #region R22

        private void processR22(string filePath)
        {
            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string OrderNo = "";
                string OrderDate = "";
                string OrderTime = "";
                string OPMode = "";
                string StoreId = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//R22");
                if (xnode != null)
                {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        OrderNo = node2.SelectSingleNode("OrderNo").InnerText;
                        OrderDate = node2.SelectSingleNode("OrderDate").InnerText;
                        OrderTime = node2.SelectSingleNode("OrderTime").InnerText;
                        OPMode = node2.SelectSingleNode("OPMode").InnerText;
                        StoreId = node2.SelectSingleNode("StoreId").InnerText;
                        

                        sql = @"INSERT INTO Order_Transport_Family_R22(PRDT,ParentId,EshopId,OrderNo,OrderDate,OrderTime,OPMode,StoreId)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + OrderNo + "','" + OrderDate + "','" + OrderTime + "','" + OPMode + "','" + StoreId + "')";
                        Execute(sql);
                    }
                }


            }
            catch (Exception ex)
            {

                pl.logMessage(ex.Message);
            }
        }

        #endregion

        #region R23

        private void processR23(string filePath)
        {
            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string OrderNo = "";
                string OrderDate = "";
                string OrderTime = "";
                string OPMode = "";
                string StoreId = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//R23");
                if (xnode != null)
                {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        OrderNo = node2.SelectSingleNode("OrderNo").InnerText;
                        OrderDate = node2.SelectSingleNode("OrderDate").InnerText;
                        OrderTime = node2.SelectSingleNode("OrderTime").InnerText;
                        OPMode = node2.SelectSingleNode("OPMode").InnerText;
                        StoreId = node2.SelectSingleNode("StoreId").InnerText;


                        sql = @"INSERT INTO Order_Transport_Family_R23(PRDT,ParentId,EshopId,OrderNo,OrderDate,OrderTime,OPMode,StoreId)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + OrderNo + "','" + OrderDate + "','" + OrderTime + "','" + OPMode + "','" + StoreId + "')";
                        Execute(sql);
                    }
                }


            }
            catch (Exception ex)
            {

                pl.logMessage(ex.Message);
            }
        }

        #endregion

        #region R99


        private void processR99(string filePath)
        {
            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string ServiceType = "";
                string OPMode = "";
                string ShipmentNo = "";
                string SPAmount = "";
                string StoreId = "";
                string SPAdate = "";
                string SPAstatus = "";
                string SPFee = "";
                string SPArate = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//R99");
                if (xnode != null)
                {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        ServiceType = node2.SelectSingleNode("ServiceType").InnerText;
                        OPMode = node2.SelectSingleNode("OPMode").InnerText;
                        ShipmentNo = node2.SelectSingleNode("ShipmentNo").InnerText;
                        SPAmount = node2.SelectSingleNode("SPAmount").InnerText;
                        
                        StoreId = node2.SelectSingleNode("StoreId").InnerText;

                        SPAdate = node2.SelectSingleNode("SPAdate").InnerText;
                        SPAstatus = node2.SelectSingleNode("SPAstatus").InnerText;
                        SPFee = node2.SelectSingleNode("SPFee").InnerText;
                        SPArate = node2.SelectSingleNode("SPArate").InnerText;

                        sql = @"INSERT INTO Order_Transport_Family_R99(PRDT,ParentId,EshopId,ServiceType,OPMode,ShipmentNo,SPAmount,StoreId,SPAdate,SPAstatus,SPFee,SPArate)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + ServiceType + "','" + OPMode + "','" + ShipmentNo + "','" + SPAmount + "','" 
                                     + StoreId + "','"  + SPAdate + "','" + SPAstatus + "','" + SPFee + "','" + SPArate + "')";
                        Execute(sql);
                    }
                }


            }
            catch (Exception ex)
            {

                pl.logMessage(ex.Message);
            }
        }

        #endregion


        #region R89


        private void processR89(string filePath)
        {
            try
            {

                string mypath = filePath.Substring(0, filePath.Length - 4);
                string doc = File.ReadAllText(mypath);
                doc = doc.Replace("''", "");
                XmlDocument xdoc = new XmlDocument();
                xdoc.LoadXml(doc);

                string PRDT = "";

                string ParentId = "";
                string EshopId = "";
                string ServiceType = "";
                string OPMode = "";
                string ShipmentNo = "";
                string SPAmount = "";
                string StoreId = "";
                string SPAdate = "";
                string SPAstatus = "";
                string SPFee = "";
                string SPArate = "";
                string sql;

                XmlNode xheader = xdoc.DocumentElement.SelectSingleNode("//HEADER");
                //Console.WriteLine(xheader.SelectSingleNode("PRDT").InnerText);
                PRDT = xheader.SelectSingleNode("PRDT").InnerText;

                XmlNodeList xnode = xdoc.DocumentElement.SelectNodes("//R89");
                if (xnode != null)
                {
                    foreach (XmlNode node2 in xnode)
                    {
                        //Console.WriteLine(node2.SelectSingleNode("ShipmentNo").InnerText);
                        ParentId = node2.SelectSingleNode("ParentId").InnerText;
                        EshopId = node2.SelectSingleNode("EshopId").InnerText;
                        ServiceType = node2.SelectSingleNode("ServiceType").InnerText;
                        OPMode = node2.SelectSingleNode("OPMode").InnerText;
                        ShipmentNo = node2.SelectSingleNode("ShipmentNo").InnerText;
                        SPAmount = node2.SelectSingleNode("SPAmount").InnerText;

                        StoreId = node2.SelectSingleNode("StoreId").InnerText;

                        SPAdate = node2.SelectSingleNode("SPAdate").InnerText;
                        SPAstatus = node2.SelectSingleNode("SPAstatus").InnerText;
                        SPFee = node2.SelectSingleNode("SPFee").InnerText;
                        SPArate = node2.SelectSingleNode("SPArate").InnerText;

                        sql = @"INSERT INTO Order_Transport_Family_R89(PRDT,ParentId,EshopId,ServiceType,OPMode,ShipmentNo,SPAmount,StoreId,SPAdate,SPAstatus,SPFee,SPArate)
                                     VALUES('" + PRDT + "','" + ParentId + "','" + EshopId + "','" + ServiceType + "','" + OPMode + "','" + ShipmentNo + "','" + SPAmount + "','"
                                     + StoreId + "','" + SPAdate + "','" + SPAstatus + "','" + SPFee + "','" + SPArate + "')";
                        Execute(sql);
                    }
                }


            }
            catch (Exception ex)
            {

                pl.logMessage(ex.Message);
            }
        }

        #endregion


        #endregion


        /// <summary>
        /// 從ftp下載檔案
        /// </summary>
        /// <param name="path">ftp路徑</param>
        /// <param name="filename">全家規格的檔案名稱</param>
        /// <returns>下載後的檔案路徑供後面解析用</returns>
        private string  downloadFile(string path,string filename,string okPath) {

            FtpWebResponse response = null;
            string url = path + filename;            
            string myDonwloadPath = this.downloadPath + "\\" + this.execDate + "\\";
            if (!Directory.Exists(myDonwloadPath) ) {
                Directory.CreateDirectory(myDonwloadPath);
            }

            myDonwloadPath += filename;
            string myOkPath ="/" + this.type.ToString() + "/OK/" + filename;    //ftp下載後要搬到完成的資料夾

            try
            {

                // Get the object used to communicate with the server.
                FtpWebRequest request = (FtpWebRequest)WebRequest.Create(url);
                request.Method = WebRequestMethods.Ftp.DownloadFile;

                // This example assumes the FTP site uses anonymous logon.
                request.Credentials = new NetworkCredential(this.id, this.pwd);

                response = (FtpWebResponse)request.GetResponse();

                Stream reader = response.GetResponseStream();

                MemoryStream memStream = new MemoryStream();

                byte[] buffer = new byte[1024];    //每次讀取1024 bytes

                while (true)
                {
                    //Application.DoEvents();

                    int bytesRead = reader.Read(buffer, 0, buffer.Length);

                    if (bytesRead == 0)
                    {
                        //已經沒有資料, 結束下載
                        //  Application.DoEvents();
                        break;
                    }
                    else
                    {
                        //資料寫入buffer
                        memStream.Write(buffer, 0, bytesRead);

                    }
                }

                memStream.Position = 0;
                //byte[] downloadedData = memStream.ToArray();



                //在串流中直接解壓縮
                MemoryStream unzipMem = DeCompressionToStreamByPassword(memStream, this.unzipPwd);
                byte[] unzipDownloadedData = unzipMem.ToArray();

                reader.Close();
                memStream.Close();
                response.Close();

                //串流寫入檔案
                FileStream newFile = new FileStream(myDonwloadPath.Substring(0, myDonwloadPath.Length - 4), FileMode.Create);
                newFile.Write(unzipDownloadedData, 0, unzipDownloadedData.Length);
                newFile.Close();
                
                //移動檔案以告知全家成功讀檔
                moveFtp2OkFolder(url,filename);
                               

             

                pl.logMessage("檔案" + filename + "處理已完成！");

            }
            
            catch (Exception e)
            {
                if (e.Message.IndexOf("550") > -1) {
                    pl.logMessage("檔案" + filename  + "不存在！");
                }
                else { 
                    pl.logMessage(e.Message);
                }
            }
           
            finally {
                if (response != null) {
                    response.Close();
                }

            }

            return myDonwloadPath;


        }



        private void DownloadFile(string userName, string password, string ftpSourceFilePath, string localDestinationFilePath)
        {
            int bytesRead = 0;
            byte[] buffer = new byte[2048];
            FileStream fileStream = null;

            FtpWebRequest request = CreateFtpWebRequest(ftpSourceFilePath, userName, password, true);
            request.Method = WebRequestMethods.Ftp.DownloadFile;
            try
            {
                Stream reader = request.GetResponse().GetResponseStream();
                 fileStream = new FileStream(localDestinationFilePath, FileMode.Create);

                while (true)
                {
                    bytesRead = reader.Read(buffer, 0, buffer.Length);

                    if (bytesRead == 0)
                        break;

                    fileStream.Write(buffer, 0, bytesRead);
                }
               
            }
            catch (Exception e)
            {
               // FileLog.logError(e.Source + ":\r\n" + e.Message);
                pl.logMessage("\r\n" + e.Message);
            }
            finally {
                if (fileStream != null) {
                    fileStream.Close();
                }
            }
        }

        private FtpWebRequest CreateFtpWebRequest(string ftpDirectoryPath, string userName, string password, bool keepAlive = false)
        {
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(new Uri(ftpDirectoryPath));

            //Set proxy to null. Under current configuration if this option is not set then the proxy that is used will get an html response from the web content gateway (firewall monitoring system)
            request.Proxy = null;

            request.UsePassive = true;
            request.UseBinary = true;
            request.KeepAlive = keepAlive;

            request.Credentials = new NetworkCredential(userName, password);

            return request;
        }

        private void Execute(string sqlstring)
        {
                      
            string connectionString = this.dbString ;          
            SqlConnection conn = new SqlConnection(connectionString);
            string cmdString = sqlstring;
            SqlCommand cmd = new SqlCommand(cmdString, conn);
            cmd.Connection.Open();
            cmd.ExecuteNonQuery();
            cmd.Connection.Close();
        }

        /// <summary>
        /// 移動FTP上的檔案
        /// </summary>
        /// <param name="sourceFolder">來源資料夾</param>
        /// <param name="destination">目的資料夾(相對路徑)</param>
        private void moveFtp2OkFolder(string sourceFolder,string filename) {

        
            try
            {
                string url = sourceFolder;

                //string url = this.ftp;
                FtpWebRequest req = FtpWebRequest.Create(new Uri(url)) as FtpWebRequest ;
                req.Credentials = new NetworkCredential(this.id, this.pwd);
               
                req.KeepAlive = true;
                req.UsePassive = true;
                req.Method = WebRequestMethods.Ftp.Rename;
                req.RenameTo = "../OK/" + filename;
                //req.RenameTo = destination;
                req.UseBinary = true;

                FtpWebResponse back = (FtpWebResponse)req.GetResponse();
                bool Success = back.StatusCode == FtpStatusCode.CommandOK || back.StatusCode == FtpStatusCode.FileActionOK;

            }
            catch (Exception ex)
            {
                pl.logMessage(ex.Message);
            }

        }



        /// <summary>
        /// 在資料流中直接解壓縮
        /// </summary>
        /// <param name="zipMemStream">壓縮的資料流</param>
        /// <param name="password">解壓縮密碼</param>
        /// <returns>解壓縮後的資料流</returns>
        public static MemoryStream DeCompressionToStreamByPassword(MemoryStream zipMemStream, string password)
        {

            MemoryStream outputUnzipMemStream = new MemoryStream();

            //Console.WriteLine("ZipInutStream Size = " + zipMemStream.Length);

            ZipInputStream zipStream = new ZipInputStream(zipMemStream);

            zipStream.Password = password;

            //需要透過ZipEntry此類別才能對Zip Input/Output Stream作壓縮解壓縮動作

             ZipEntry entry = zipStream.GetNextEntry();
            
            StreamUtils.Copy(zipStream, outputUnzipMemStream, new byte[4096]);

            return outputUnzipMemStream;

        }





    }








}
