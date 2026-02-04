using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Seq.Api;
using SeqCli.EndToEnd.Support;
using Serilog;
using Xunit;

namespace SeqCli.EndToEnd.Forwarder;

public class ForwarderLegacyRawIngestionTestCase : ICliTestCase
{
    public async Task ExecuteAsync(SeqConnection _, ILogger logger, CliCommandRunner runner)
    {
        var (forwarder, forwarderUri) = await runner.SpawnForwarderAsync();
        using (forwarder)
        {
            using var connection = new SeqConnection(forwarderUri);

            // Test case from problem statement
            var legacyRawPayload = @"{""Events"":[{""Timestamp"":""2026-02-02T18:32:53.0512280+00:00"",""Level"":""Information"",""Exception"":null,""MessageTemplate"":""[{Step}] succeeded ({Duration} seconds)"",""Properties"":{""Application"":""SerilogPS PS2"",""Environment"":""Prod"",""Step"":""tudo pronto"",""Duration"":0.0,""Version"":""2.3"",""Host"":null}}]}";

            await IngestLegacyRaw(connection, legacyRawPayload, HttpStatusCode.Created);
            
            // Test with multiple events
            var multipleEvents = @"{""Events"":[
                {""Timestamp"":""2026-02-02T18:32:53.0512280+00:00"",""Level"":""Information"",""Exception"":null,""MessageTemplate"":""Event 1"",""Properties"":{""Prop1"":""Value1""}},
                {""Timestamp"":""2026-02-02T18:32:54.0512280+00:00"",""Level"":""Warning"",""Exception"":null,""MessageTemplate"":""Event 2"",""Properties"":{""Prop2"":42}}
            ]}";
            
            await IngestLegacyRaw(connection, multipleEvents, HttpStatusCode.Created);
            
            // Test with exception
            var withException = @"{""Events"":[{""Timestamp"":""2026-02-02T18:32:53.0512280+00:00"",""Level"":""Error"",""Exception"":""System.Exception: Test exception"",""MessageTemplate"":""An error occurred"",""Properties"":{}}]}";
            
            await IngestLegacyRaw(connection, withException, HttpStatusCode.Created);
            
            // Test with nested properties
            var withNestedProps = @"{""Events"":[{""Timestamp"":""2026-02-02T18:32:53.0512280+00:00"",""Level"":""Debug"",""Exception"":null,""MessageTemplate"":""Complex event"",""Properties"":{""Nested"":{""Inner"":""Value""},""Array"":[1,2,3]}}]}";
            
            await IngestLegacyRaw(connection, withNestedProps, HttpStatusCode.Created);
        }
    }

    static async Task IngestLegacyRaw(SeqConnection connection, string rawPayload, HttpStatusCode expectedStatusCode)
    {
        var content = new StringContent(rawPayload, Encoding.UTF8, "application/json");
        var response = await connection.Client.HttpClient.PostAsync("api/events/raw", content);
        Assert.Equal(expectedStatusCode, response.StatusCode);
    }
}
