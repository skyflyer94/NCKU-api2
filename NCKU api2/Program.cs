using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Diagnostics;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Data;

namespace NCKU_api2
{
    public class Meter2
    {
        public string _id { get; set; }
        public int meter_index { get; set; }
        public DateTime time_logged_to_seconds { get; set; }
        public double volt { get; set; }
        public double amp_r { get; set; }
        public double amp_s { get; set; }
        public double amp_t { get; set; }
        public int pf_r { get; set; }
        public int pf_s { get; set; }
        public int pf_t { get; set; }
        public double kw { get; set; }
        public double kwh { get; set; }
    }

    public class EEMeter
    {
        public string devices { get; set; }
        public List<Meter2> data { get; set; }
    }
    public class Datum
    {
        public string _id { get; set; }
        public int sensorId { get; set; }
        public DateTime dataTime { get; set; }
        public double temperature { get; set; }
        public double humidity { get; set; }
        public int co2 { get; set; }
        public int pm10 { get; set; }
        public int pm25 { get; set; }
        public int voc { get; set; }
        public string dataDate { get; set; }
    }

    public class ARCHenv
    {
        public string devices { get; set; }
        public List<Datum> data { get; set; }
    }

    public class Meter
    {
        public DateTime date { get; set; }
        public double KW { get; set; }
        public int KWH { get; set; }
        public int KWR { get; set; }
        public int KWS { get; set; }
        public int KWT { get; set; }
        public int PF { get; set; }
    }

    public class ARCHMeter
    {
        public string devices { get; set; }
        public List<Meter> meter { get; set; }
    }

    class Program
    {
        static HttpClient client = new HttpClient();
        static string todayDate = DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd");

        static void Main()
        {
            RunAsync().Wait();
        }

        static async Task RunAsync()
        {
            //int status = await getEEMeter();
            int status = await getARCHENV();
            //int status = await getARCHMTR();
            
        }

        static async Task<int> getEEMeter()
        {
            using (var httpClient = new HttpClient())
            {
                var url = "http://140.116.49.241:3010/EEmeter";
                var parameters = new Dictionary<string, string> { { "startdate", todayDate+" 00:00:00" }, { "enddate", todayDate + " 23:59:00" } };
                var encodedContent = new FormUrlEncodedContent(parameters);
                try
                {
                    var response = await httpClient.PostAsync(url, encodedContent);

                    if (response.StatusCode == HttpStatusCode.OK) 
                    {
                        // Do something with response. Example get content:
                        string content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        var result = JsonConvert.DeserializeObject<EEMeter>(content);
                        Console.WriteLine("API results");
                        using (SqlConnection connection = new SqlConnection("Data Source=smartnckuiotdb.database.windows.net;Initial Catalog=smartnckuiotdb;User ID=iotdbadmin;Password=54cjo4t/6284"))
                        {
                            connection.Open();
                            foreach (Meter2 info in result.data)
                            {
                                using (SqlCommand command = new SqlCommand())
                                {
                                    command.Connection = connection;            // <== lacking
                                    command.CommandType = CommandType.Text;
                                    command.CommandText = "BEGIN IF NOT EXISTS (SELECT * FROM EEMeter WHERE _id=@id AND deviceName=@device AND meter_index=@idx AND time=@time) BEGIN INSERT INTO EEMeter (_id, deviceName, meter_index , time, volt, amp_r, amp_s, amp_t, pf_r, pf_s, pf_t, kw, kwh) VALUES (@id, @device, @idx, @time, @volt, @ampr, @amps, @ampt, @pfr, @pfs, @pft, @kw, @kwh) END END";
                                    command.Parameters.AddWithValue("@device", result.devices);

                                    try
                                    {
                                        command.Parameters.AddWithValue("@id", info._id);
                                        command.Parameters.AddWithValue("@idx", info.meter_index);
                                        command.Parameters.AddWithValue("@time", info.time_logged_to_seconds);
                                        command.Parameters.AddWithValue("@volt", info.volt);
                                        command.Parameters.AddWithValue("@ampr", info.amp_r);
                                        command.Parameters.AddWithValue("@amps", info.amp_s);
                                        command.Parameters.AddWithValue("@ampt", info.amp_t);
                                        command.Parameters.AddWithValue("@pfr", info.pf_r);
                                        command.Parameters.AddWithValue("@pfs", info.pf_s);
                                        command.Parameters.AddWithValue("@pft", info.pf_t);
                                        command.Parameters.AddWithValue("@kw", info.kw);
                                        command.Parameters.AddWithValue("@kwh", info.kwh);
                                        int recordsAffected = command.ExecuteNonQuery();
                                        //Console.WriteLine("here");
                                    }
                                    catch (SqlException e)
                                    {
                                        Console.WriteLine(e.Message);
                                    }
                                }
                            }
                            connection.Close();
                        }

                        return 1;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return 0;
                }
            }
            return 0;
        }

        static async Task<int> getARCHENV()
        {
            using (var httpClient = new HttpClient())
            {
                var url = "http://211.22.123.99:3010/ARCHenv";
                var parameters = new Dictionary<string, string> { { "startdate", todayDate + " 00:00:00" }, { "enddate", todayDate + " 23:59:00" } };
                var encodedContent = new FormUrlEncodedContent(parameters);
                try
                {
                    var response = await httpClient.PostAsync(url, encodedContent);

                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        // Do something with response. Example get content:
                        string content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        var result = JsonConvert.DeserializeObject<ARCHenv>(content);
                        Console.WriteLine("API results");
                        using (SqlConnection connection = new SqlConnection("Data Source=smartnckuiotdb.database.windows.net;Initial Catalog=smartnckuiotdb;User ID=iotdbadmin;Password=54cjo4t/6284"))
                        {
                            connection.Open();
                            foreach (Datum info in result.data) {
                                using (SqlCommand command = new SqlCommand())
                                {
                                    command.Connection = connection;            // <== lacking
                                    command.CommandType = CommandType.Text;
                                    command.CommandText = "BEGIN IF NOT EXISTS (SELECT * FROM ARCHenv WHERE _id=@id AND deviceName=@device AND sensorId=@sensor AND dataTime=@Time AND dataDate=@Date) BEGIN INSERT into ARCHenv (_id, sensorId, dataTime, temperature, humidity, co2, pm10, pm25, voc, dataDate, deviceName) VALUES (@id, @sensor, @Time, @temp, @humid, @co2, @pm10, @pm25, @voc, @Date, @device) END END";
                                    command.Parameters.AddWithValue("@device", result.devices);

                                    try
                                    {
                                        command.Parameters.AddWithValue("@id", info._id);
                                        command.Parameters.AddWithValue("@sensor", info.sensorId);
                                        command.Parameters.AddWithValue("@Time", info.dataTime);
                                        command.Parameters.AddWithValue("@Date", info.dataDate);
                                        command.Parameters.AddWithValue("@humid", info.humidity);
                                        command.Parameters.AddWithValue("@temp", info.temperature);
                                        command.Parameters.AddWithValue("@co2", info.co2);
                                        command.Parameters.AddWithValue("@pm10", info.pm10);
                                        command.Parameters.AddWithValue("@pm25", info.pm25);
                                        command.Parameters.AddWithValue("@voc", info.voc);
                                        int recordsAffected = command.ExecuteNonQuery();
                                        //Console.WriteLine("here");
                                    }
                                    catch (SqlException e)
                                    {
                                        Console.WriteLine(e.Message);
                                    }
                                }
                            }
                            connection.Close();
                        }

                        return 1;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return 0;
                }
             }
            return 0;
        }

        static async Task<int> getARCHMTR()
        {
            using (var httpClient = new HttpClient())
            {
                var url = "http://211.22.123.99:3010/ARCHmeter";
                var parameters = new Dictionary<string, string> { { "startdate", "2017-10-01 00:00:00" }, { "enddate", "2017-10-11 00:00:00" } };
                var encodedContent = new FormUrlEncodedContent(parameters);
                try
                {
                    var response = await httpClient.PostAsync(url, encodedContent);

                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        // Do something with response. Example get content:
                        string content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        var result = JsonConvert.DeserializeObject<ARCHMeter>(content);

                        Debug.WriteLine(content);
                        return 1;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return 0;
                }
            }
            return 0;
        }
    }
}