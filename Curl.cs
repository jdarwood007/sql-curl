using Microsoft.SqlServer.Server;
using System;
using System.Data.SqlTypes;
using System.Net;
using System.Threading;

/// <summary>
/// Provides CURL-like functionalities in T-SQL code.
/// </summary>
public partial class Curl
{
    [SqlFunction]
    [return: SqlFacet(MaxSize = -1)]
    public static SqlChars Get(SqlChars H, SqlChars url)
    {
        /* 
         * We need to set TLS 1.2 for all connections as WebClient is built with a older .NET framework and does not default to TLS 1.2
         * References: https://stackoverflow.com/questions/2813674/webclient-from-asp-net-gives-an-existing-connection-was-forcibly-closed-by-the
         */
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        var client = new CustomWebClient();

        AddHeader(H, client);
        return new SqlChars(
                client.DownloadString(
                    Uri.EscapeUriString(url.ToSqlString().Value)
                    ).ToCharArray());
    }

    [SqlProcedure]
    public static SqlChars Post(SqlChars H, SqlChars d, SqlChars url)
    {
        /* 
         * We need to set TLS 1.2 for all connections as WebClient is built with a older .NET framework and does not default to TLS 1.2
         * References: https://stackoverflow.com/questions/2813674/webclient-from-asp-net-gives-an-existing-connection-was-forcibly-closed-by-the
         */
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        var client = new WebClient();
        AddHeader(H, client);
        if (d.IsNull)
            throw new ArgumentException("You must specify data that will be sent to the endpoint", "@d");
        var response =
                client.UploadString(
                    Uri.EscapeUriString(url.ToSqlString().Value),
                    "POST",
                    d.ToSqlString().Value
                    );
        return new SqlChars(response);
    }

    [SqlProcedure]
    public static SqlChars PostWithRetry(SqlChars H, SqlChars d, SqlChars url)
    {
        /* 
         * We need to set TLS 1.2 for all connections as WebClient is built with a older .NET framework and does not default to TLS 1.2
         * References: https://stackoverflow.com/questions/2813674/webclient-from-asp-net-gives-an-existing-connection-was-forcibly-closed-by-the
         */
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        var client = new WebClient();
        AddHeader(H, client);
        if (d.IsNull)
            throw new ArgumentException("You must specify data that will be sent to the endpoint", "@d");
        int i = RETRY_COUNT;
        string response = "";
        do try
            {
                response =
                        client.UploadString(
                            Uri.EscapeUriString(url.ToSqlString().Value),
                            "POST",
                            d.ToSqlString().Value
                            );
                i = -1;
                break;
            }
            catch (Exception ex)
            {
                SqlContext.Pipe.Send("Error:\t" + ex.Message + ". Waiting " + DELAY_ON_ERROR + "ms.");
                i--;
                Thread.Sleep(DELAY_ON_ERROR);
            }
        while (i > 0);
        if (i == -1)
            return new SqlChars(response);
        else
        {
            SqlContext.Pipe.Send("Request is executed." + response);
            return new SqlChars(String.Empty);

        }
    }

    static readonly int RETRY_COUNT = 3;
    static readonly int DELAY_ON_ERROR = 50;

    private static void AddHeader(SqlChars H, WebClient client)
    {
        char[] charsToTrim = { ' ', '"'};

        if (!H.IsNull)
        {
            var header = H.ToSqlString().Value;
            if (!string.IsNullOrWhiteSpace(header))
            {
                if (header.ToLower().Contains(";"))
                {
                    var headers = header.Split(';');
                    foreach(string h2 in headers)
                    {
                        /*
                         * As much as we would like to just directly set content-type, it seems Microsoft doesn't allow this.
                         * References: https://stackoverflow.com/questions/6560769/webclient-set-headers
                         * References: https://stackoverflow.com/questions/15091300/posting-json-to-url-via-webclient-in-c-sharp
                        */
                        var tempHeader = h2.Split(':');
                        if (tempHeader.Length > 1 && tempHeader[0].ToLower() == "content-type")
                        {
                            client.Headers["Content-Type"] = tempHeader[1].ToLower().Trim(charsToTrim);
                        }
                        else
                        {
                            client.Headers.Add(h2);
                        }
                    }
                }
                else
                {
                    client.Headers.Add(header);
                }
            }
        }
    }

    [SqlProcedure]
    public static SqlChars Put(SqlChars H, SqlChars d, SqlChars url)
    {
        /* 
         * We need to set TLS 1.2 for all connections as WebClient is built with a older .NET framework and does not default to TLS 1.2
         * References: https://stackoverflow.com/questions/2813674/webclient-from-asp-net-gives-an-existing-connection-was-forcibly-closed-by-the
         */
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        var client = new WebClient();
        AddHeader(H, client);
        if (d.IsNull)
            throw new ArgumentException("You must specify data that will be sent to the endpoint", "@d");
        var response =
                client.UploadString(
                    Uri.EscapeUriString(url.ToSqlString().Value),
                    "PUT",
                    d.ToSqlString().Value
                    );
        return new SqlChars(response);
    }

    [SqlProcedure]
    public static SqlChars PutWithRetry(SqlChars H, SqlChars d, SqlChars url)
    {
        /* 
         * We need to set TLS 1.2 for all connections as WebClient is built with a older .NET framework and does not default to TLS 1.2
         * References: https://stackoverflow.com/questions/2813674/webclient-from-asp-net-gives-an-existing-connection-was-forcibly-closed-by-the
         */
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        var client = new WebClient();
        AddHeader(H, client);
        if (d.IsNull)
            throw new ArgumentException("You must specify data that will be sent to the endpoint", "@d");
        int i = RETRY_COUNT;
        string response = "";
        do try
            {
                response =
                        client.UploadString(
                            Uri.EscapeUriString(url.ToSqlString().Value),
                            "PUT",
                            d.ToSqlString().Value
                            );
                i = -1;
                break;
            }
            catch (Exception ex)
            {
                SqlContext.Pipe.Send("Error:\t" + ex.Message + ". Waiting " + DELAY_ON_ERROR + "ms.");
                i--;
                Thread.Sleep(DELAY_ON_ERROR);
            }
        while (i > 0);
        if (i == -1)
            return new SqlChars(response);
        else
        {
            SqlContext.Pipe.Send("Request is executed." + response);
            return new SqlChars(String.Empty);
        }
            
    }

};

/* 
 * We use this class to override the default WebClient and specify keep alive to false (closed) and set the Except
 * References: https://stackoverflow.com/questions/2813674/webclient-from-asp-net-gives-an-existing-connection-was-forcibly-closed-by-the
 */
    public class CustomWebClient : WebClient
{

    protected override WebRequest GetWebRequest(Uri address)
    {
        var request = base.GetWebRequest(address);
        if (request is HttpWebRequest)
        {
            (request as HttpWebRequest).KeepAlive = false;
            (request as HttpWebRequest).ServicePoint.Expect100Continue = false;
        }
        return request;
    }
}