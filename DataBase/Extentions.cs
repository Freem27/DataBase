using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TDV.DataBase.Extentions
{
    public static class Shared
    {
        private static bool fileIsBusy;
        public static void InFileLog(string str, string filePath = null)
        {
            if (filePath == null)
                filePath = $"{AppDomain.CurrentDomain.BaseDirectory}uvaProjects.log";
            string dt = DateTime.Now.ToString("dd.MM.yyyy hh.mm.ss") + " - ";
            while (fileIsBusy)
                Thread.Sleep(10);
            fileIsBusy = true;
            if (!File.Exists(filePath))
            {
                using (StreamWriter sw = File.CreateText(filePath))
                {
                    sw.WriteLine(dt + str);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filePath))
                {
                    sw.WriteLine(dt + str);
                }
            }
            fileIsBusy = false;
        }
    }
}
