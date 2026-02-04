// Copyright © Datalust Pty Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Net.Http.Headers;
using Seq.Api.Model.Shared;
using SeqCli.Api;
using SeqCli.Config;
using SeqCli.Forwarder.Channel;
using SeqCli.Forwarder.Diagnostics;
using JsonException = System.Text.Json.JsonException;

namespace SeqCli.Forwarder.Web.Api;

class IngestionEndpoints : IMapEndpoints
{
    static readonly Encoding Utf8 = new UTF8Encoding(false);

    readonly ForwardingAuthenticationStrategy _forwardingChannels;
    readonly SeqCliConfig _config;

    public IngestionEndpoints(ForwardingAuthenticationStrategy forwardingChannels, SeqCliConfig config)
    {
        _forwardingChannels = forwardingChannels;
        _config = config;
    }

    public void MapEndpoints(WebApplication app)
    {
        app.MapPost("ingest/clef", (Delegate) (async (HttpContext context) => await IngestCompactFormatAsync(context)));
        app.MapPost("api/events/raw", (Delegate) (async (HttpContext context) => await IngestAsync(context)));
    }

    async Task<IResult> IngestAsync(HttpContext context)
    {
        var clef = DefaultedBoolQuery(context.Request, "clef");

        if (clef) return await IngestCompactFormatAsync(context);

        var contentType = (string?)context.Request.Headers[HeaderNames.ContentType];
        const string clefMediaType = "application/vnd.serilog.clef";

        if (contentType != null && contentType.StartsWith(clefMediaType)) return await IngestCompactFormatAsync(context);

        return await IngestLegacyRawFormatAsync(context);

        // IngestionLog.ForClient(context.Connection.RemoteIpAddress)
        //     .Error("Client supplied a legacy raw-format (non-CLEF) payload");
        // return Error(HttpStatusCode.BadRequest, "Only newline-delimited JSON (CLEF) payloads are supported.");
    }

    async Task<IResult> IngestCompactFormatAsync(HttpContext context)
    {
        byte[]? rented = null;

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
            cts.CancelAfter(TimeSpan.FromSeconds(5));

            var requestApiKey = GetApiKey(context.Request);
            var log = _forwardingChannels.GetForwardingChannel(requestApiKey);

            // Add one for the extra newline that we have to insert at the end of batches.
            var bufferSize = _config.Connection.BatchSizeLimitBytes + 1;
            rented = ArrayPool<byte>.Shared.Rent(bufferSize);
            var buffer = new ArraySegment<byte>(rented, 0, bufferSize);
            var writeHead = 0;
            var readHead = 0;

            var done = false;
            while (!done)
            {
                // Fill the memory buffer from as much of the incoming request payload as possible; buffering in memory increases the
                // size of write batches.
                while (!done)
                {
                    var remaining = buffer.Count - 1 - writeHead;
                    if (remaining == 0)
                    {
                        IngestionLog.ForClient(context.Connection.RemoteIpAddress)
                            .Error("An incoming request exceeded the configured batch size limit");
                        return Error(HttpStatusCode.RequestEntityTooLarge, "the request is too large to process");
                    }

                    var read = await context.Request.Body.ReadAsync(buffer.AsMemory(writeHead, remaining), cts.Token);
                    if (read == 0)
                    {
                        done = true;
                    }

                    writeHead += read;

                    // Ingested batches must be terminated with `\n`, but this isn't an API requirement.
                    if (done && writeHead > 0 && writeHead < buffer.Count && buffer[writeHead - 1] != (byte)'\n')
                    {
                        buffer[writeHead] = (byte)'\n';
                        writeHead += 1;
                    }
                }

                // Validate what we read, marking out a batch of one or more complete newline-delimited events.
                var batchStart = readHead;
                var batchEnd = readHead;
                while (batchEnd < writeHead)
                {
                    var eventStart = batchEnd;
                    var nlIndex = buffer.AsSpan()[eventStart..].IndexOf((byte)'\n');

                    if (nlIndex == -1)
                    {
                        break;
                    }

                    var eventEnd = eventStart + nlIndex + 1;

                    batchEnd = eventEnd;
                    readHead = batchEnd;

                    if (!ValidateClef(buffer.AsSpan()[eventStart..eventEnd], out var error))
                    {
                        var payloadText = Encoding.UTF8.GetString(buffer.AsSpan()[eventStart..eventEnd]);
                        IngestionLog.ForPayload(context.Connection.RemoteIpAddress, payloadText)
                            .Error("Payload validation failed: {Error}", error);
                        return Error(HttpStatusCode.BadRequest, $"Payload validation failed: {error}.");
                    }
                }

                if (batchStart != batchEnd)
                {
                    await log.WriteAsync(buffer[batchStart..batchEnd], cts.Token);
                }

                // Copy any unprocessed data into our buffer and continue
                if (!done && readHead != 0)
                {
                    var retain = writeHead - readHead;
                    buffer.AsSpan()[readHead..writeHead].CopyTo(buffer.AsSpan()[..retain]);
                    readHead = 0;
                    writeHead = retain;
                }
            }

            return SuccessfulIngestion();
        }
        catch (Exception ex)
        {
            IngestionLog.ForClient(context.Connection.RemoteIpAddress)
                .Error(ex, "Ingestion failed");
            return Error(HttpStatusCode.InternalServerError, "Ingestion failed.");
        }
        finally
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    async Task<IResult> IngestLegacyRawFormatAsync(HttpContext context)
    {
        byte[]? rented = null;

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
            cts.CancelAfter(TimeSpan.FromSeconds(5));

            var requestApiKey = GetApiKey(context.Request);
            var log = _forwardingChannels.GetForwardingChannel(requestApiKey);

            // Add one for the extra newline that we have to insert at the end of batches.
            var bufferSize = _config.Connection.BatchSizeLimitBytes + 1;
            rented = ArrayPool<byte>.Shared.Rent(bufferSize);
            var buffer = new ArraySegment<byte>(rented, 0, bufferSize);
            var writeHead = 0;
            var readHead = 0;

            var done = false;
            while (!done)
            {
                // Fill the memory buffer from as much of the incoming request payload as possible; buffering in memory increases the
                // size of write batches.
                while (!done)
                {
                    var remaining = buffer.Count - 1 - writeHead;
                    if (remaining == 0)
                    {
                        IngestionLog.ForClient(context.Connection.RemoteIpAddress)
                            .Error("An incoming request exceeded the configured batch size limit");
                        return Error(HttpStatusCode.RequestEntityTooLarge, "the request is too large to process");
                    }

                    var read = await context.Request.Body.ReadAsync(buffer.AsMemory(writeHead, remaining), cts.Token);
                    if (read == 0)
                    {
                        done = true;
                    }

                    writeHead += read;

                    // Ingested batches must be terminated with `\n`, but this isn't an API requirement.
                    if (done && writeHead > 0 && writeHead < buffer.Count && buffer[writeHead - 1] != (byte)'\n')
                    {
                        buffer[writeHead] = (byte)'\n';
                        writeHead += 1;
                    }
                }

                // Validate what we read, marking out a batch of one or more complete newline-delimited events.
                var batchStart = readHead;
                var batchEnd = readHead;
                while (batchEnd < writeHead)
                {
                    var eventStart = batchEnd;
                    var nlIndex = buffer.AsSpan()[eventStart..].IndexOf((byte)'\n');

                    if (nlIndex == -1)
                    {
                        break;
                    }

                    var eventEnd = eventStart + nlIndex + 1;

                    batchEnd = eventEnd;
                    readHead = batchEnd;

                    if (!ValidateRaw(buffer.AsSpan()[eventStart..eventEnd], out var error))
                    {
                        var payloadText = Encoding.UTF8.GetString(buffer.AsSpan()[eventStart..eventEnd]);
                        IngestionLog.ForPayload(context.Connection.RemoteIpAddress, payloadText)
                            .Error("Payload validation failed: {Error}", error);
                        return Error(HttpStatusCode.BadRequest, $"Payload validation failed: {error}.");
                    }
                }

                if (batchStart != batchEnd)
                {
                    var rawSpan = buffer[batchStart..batchEnd];
                    var jsonDoc = JsonDocument.Parse(rawSpan.ToArray());
                    
                    // Parse raw json to extract events
                    JsonElement eventsArray = default;
                    foreach (var element in jsonDoc.RootElement.EnumerateObject())
                    {
                        if (element.Name == "events" || element.Name == "Events")
                        {
                            eventsArray = element.Value;
                            break;
                        }
                    }
                    
                    // Convert each legacy event to CLEF
                    using var clefStream = new MemoryStream();
                    foreach (var evt in eventsArray.EnumerateArray())
                    {
                        ConvertLegacyEventToClef(evt, clefStream);
                    }
                    
                    if (clefStream.Length > 0)
                    {
                        await log.WriteAsync(clefStream.ToArray(), cts.Token);
                    }
                }

                // Copy any unprocessed data into our buffer and continue
                if (!done && readHead != 0)
                {
                    var retain = writeHead - readHead;
                    buffer.AsSpan()[readHead..writeHead].CopyTo(buffer.AsSpan()[..retain]);
                    readHead = 0;
                    writeHead = retain;
                }
            }

            return SuccessfulIngestion();
        }
        catch (Exception ex)
        {
            IngestionLog.ForClient(context.Connection.RemoteIpAddress)
                .Error(ex, "Ingestion failed");
            return Error(HttpStatusCode.InternalServerError, "Ingestion failed.");
        }
        finally
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    static void ConvertLegacyEventToClef(JsonElement legacyEvent, Stream output)
    {
        using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { SkipValidation = true });
        writer.WriteStartObject();

        string? timestamp = null;
        string? level = null;
        string? messageTemplate = null;
        string? exception = null;
        JsonElement? properties = null;

        // Extract known fields from legacy event
        foreach (var prop in legacyEvent.EnumerateObject())
        {
            switch (prop.Name)
            {
                case "Timestamp":
                    timestamp = prop.Value.GetString();
                    break;
                case "Level":
                    level = prop.Value.GetString();
                    break;
                case "MessageTemplate":
                    messageTemplate = prop.Value.GetString();
                    break;
                case "Exception":
                    if (prop.Value.ValueKind != JsonValueKind.Null)
                        exception = prop.Value.GetString();
                    break;
                case "Properties":
                    properties = prop.Value;
                    break;
            }
        }

        // Write CLEF fields
        if (!string.IsNullOrEmpty(timestamp))
        {
            writer.WriteString("@t", timestamp);
        }

        if (!string.IsNullOrEmpty(level))
        {
            writer.WriteString("@l", level);
        }

        if (!string.IsNullOrEmpty(messageTemplate))
        {
            writer.WriteString("@mt", messageTemplate);
        }

        if (!string.IsNullOrEmpty(exception))
        {
            writer.WriteString("@x", exception);
        }

        // Write properties as top-level fields
        if (properties.HasValue)
        {
            foreach (var prop in properties.Value.EnumerateObject())
            {
                writer.WritePropertyName(prop.Name);
                WriteJsonElement(writer, prop.Value);
            }
        }

        writer.WriteEndObject();
        writer.Flush();
        
        // Write newline after each event
        output.WriteByte((byte)'\n');
    }

    static void WriteJsonElement(Utf8JsonWriter writer, JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var prop in element.EnumerateObject())
                {
                    writer.WritePropertyName(prop.Name);
                    WriteJsonElement(writer, prop.Value);
                }
                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in element.EnumerateArray())
                {
                    WriteJsonElement(writer, item);
                }
                writer.WriteEndArray();
                break;
            case JsonValueKind.String:
                writer.WriteStringValue(element.GetString());
                break;
            case JsonValueKind.Number:
                if (element.TryGetInt32(out var intValue))
                    writer.WriteNumberValue(intValue);
                else if (element.TryGetInt64(out var longValue))
                    writer.WriteNumberValue(longValue);
                else if (element.TryGetDouble(out var doubleValue))
                    writer.WriteNumberValue(doubleValue);
                else
                    writer.WriteRawValue(element.GetRawText());
                break;
            case JsonValueKind.True:
                writer.WriteBooleanValue(true);
                break;
            case JsonValueKind.False:
                writer.WriteBooleanValue(false);
                break;
            case JsonValueKind.Null:
                writer.WriteNullValue();
                break;
        }
    }

    static bool DefaultedBoolQuery(HttpRequest request, string queryParameterName)
    {
        var parameter = request.Query[queryParameterName];
        if (parameter.Count != 1)
            return false;

        var value = (string?) parameter;

        if (value == "" && (
                request.QueryString.Value!.Contains($"&{queryParameterName}=") ||
                request.QueryString.Value.Contains($"?{queryParameterName}=")))
        {
            return false;
        }

        return "true".Equals(value, StringComparison.OrdinalIgnoreCase) || value == "" || value == queryParameterName;
    }

    static string? GetApiKey(HttpRequest request)
    {
        var apiKeyHeader = request.Headers[ApiConstants.ApiKeyHeaderName];

        if (apiKeyHeader.Count > 0) return apiKeyHeader.Last();
        return request.Query.TryGetValue("apiKey", out var apiKey) ? apiKey.Last() : null;
    }

    bool ValidateClef(Span<byte> evt, [NotNullWhen(false)] out string? errorFragment)
    {
        // Note that `errorFragment` does not include user-supplied values; we opt in to adding this to
        // the ingestion log and include it using `ForPayload()`.

        if (evt.Length > _config.Connection.EventSizeLimitBytes)
        {
            errorFragment = "an event exceeds the configured size limit";
            return false;
        }

        var reader = new Utf8JsonReader(evt);

        var foundTimestamp = false;
        try
        {
            reader.Read();
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                errorFragment = $"unexpected token type `{reader.TokenType}`";
                return false;
            }

            while (reader.Read())
            {
                if (reader.CurrentDepth == 1)
                {
                    if (reader.TokenType == JsonTokenType.PropertyName)
                    {
                        var name = reader.GetString();

                        if (name == "@t")
                        {
                            if (!reader.Read())
                            {
                                errorFragment = "payload ended prematurely";
                                return false;
                            }
                            var value = reader.GetString();
                            if (!DateTimeOffset.TryParse(value, out _))
                            {
                                errorFragment = "unparseable `@t` timestamp value";
                                return false;
                            }

                            foundTimestamp = true;
                        }
                    }
                }
            }
        }
        catch (JsonException)
        {
            errorFragment = "JSON parsing failure";
            return false;
        }

        if (!foundTimestamp)
        {
            errorFragment = "missing `@t` timestamp property";
            return false;
        }

        errorFragment = null;
        return true;
    }

    bool ValidateRaw(Span<byte> evt, [NotNullWhen(false)] out string? errorFragment)
    {
        // Note that `errorFragment` does not include user-supplied values; we opt in to adding this to
        // the ingestion log and include it using `ForPayload()`.

        if (evt.Length > _config.Connection.EventSizeLimitBytes)
        {
            errorFragment = "an event exceeds the configured size limit";
            return false;
        }

        try
        {

            var jsonDoc = JsonDocument.Parse(evt.ToArray());
            if (!(jsonDoc.RootElement.TryGetProperty("events", out var eventsToken) ||
                  jsonDoc.RootElement.TryGetProperty("Events", out eventsToken)))
            {
                errorFragment = "events were not found in raw payload";
                return false;
            }
        }
        catch (JsonException)
        {
            errorFragment = "JSON parsing failure";
            return false;
        }

        errorFragment = null;
        return true;
    }

    static IResult Error(HttpStatusCode statusCode, string message)
    {
        return Results.Json(new ErrorPart { Error = message }, statusCode: (int)statusCode);
    }

    static IResult SuccessfulIngestion()
    {
        return TypedResults.Content(
            "{}",
            "application/json",
            Utf8,
            StatusCodes.Status201Created);
    }
}
