using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Configuration;

namespace GetFamilyXml2DB
{
    class Program
    {

        static string FTP;
        static string ID;
        static string PW;
        static string UNZIP_PW;
        static string DOWN_PATH;
        static string DB_STR;


        static void Main(string[] args)
        {

            FTP = Config.getConfig("FTP");
            ID = Config.getConfig("ID");
            PW = Config.getConfig("PW");
            UNZIP_PW = Config.getConfig("UNZIP_PW");
            DOWN_PATH = Config.getConfig("DOWN_PATH");
            DB_STR = Config.getConfig("DB_STR");

           
            XMLClient c = new XMLClient(FTP,ID,PW,UNZIP_PW,DOWN_PATH,DB_STR);
            c.getXmlFiles();

        }


     



    }
}
