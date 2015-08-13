using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace GetFamilyXml2DB
{
    public class Config
    {


        public static  string getConfig(string key)
        {
            string value = ConfigurationManager.AppSettings[key];
            return value;
        }

    }
}
