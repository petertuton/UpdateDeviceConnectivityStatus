using System.Net.Http.Headers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.Net.Http;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace UpdateDeviceConnectivityStatus
{
    public static class UpdateDeviceConnectivityStatus
    {
        // Create a static HTTP client for calls to IoTC's REST API
        private static HttpClient httpClient = new HttpClient();

        // Function code
        [FunctionName("UpdateDeviceConnectivityStatus")]
        public static async Task Run([EventHubTrigger("connectivity", Connection = "EventHubConnectionString")] EventData[] events, ILogger log)
        {
            // Initialize a list of potential exceptions for each event
            var exceptions = new List<Exception>();

            // Process each event
            foreach (EventData eventData in events)
            {
                try
                {
                    // Deserialize the event body into a DeviceConnectivityEvent object
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    DeviceConnectivityEvent connectivityEvent = JsonSerializer.Deserialize<DeviceConnectivityEvent>(messageBody);
                    log.LogInformation($"UpdateDeviceConnectivityStatus event: {messageBody}");

                    // Set the PATCH content
                    PatchContent patchContent = new PatchContent
                    {
                        Connected = connectivityEvent.Connected
                    };
                    var content = JsonSerializer.Serialize(patchContent);
                    var buffer = System.Text.Encoding.UTF8.GetBytes(content);
                    var byteContent = new ByteArrayContent(buffer);
                    byteContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                    // Set the Authentication header to the IoTC API token (see here for instructions on how to create an ioTC API token: https://docs.microsoft.com/en-us/azure/iot-central/core/howto-authorize-rest-api#get-an-api-token)
                    // ** strip "SharedAccessSignature" from the generated token
                    httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("SharedAccessSignature", System.Environment.GetEnvironmentVariable("IOTC_API_TOKEN"));

                    // Call the IoTC API 
                    String requestUri = $"https://{System.Environment.GetEnvironmentVariable("IOTC_APP_NAME")}.azureiotcentral.com/api/devices/{connectivityEvent.DeviceId}/properties?api-version=1.1-preview";
                    var result = await httpClient.PatchAsync(requestUri, byteContent);

                    // Log the result and yield the task
                    log.LogInformation($"UpdateDeviceConnectivityStatus result: {result.ReasonPhrase}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        // Ensure you apply a transformation of the exported data to the following:
        // {
        //     DeviceId: .device.id,
        //     Connected: (.messageType == "connected")
        // }
        // See here for more information on transforming data for export: https://docs.microsoft.com/en-us/azure/iot-central/core/howto-transform-data-internally
        public class DeviceConnectivityEvent
        {
            public string DeviceId { get; set; }
            public bool Connected { get; set; }
        }

        // Ensure you add a 'cloud property' to your device template called "Connected", otherwise your patch call will return a 422 'Unprocessable Entity'
        // See here for details on how to create a property: https://docs.microsoft.com/en-us/azure/iot-central/core/howto-use-properties 
        public class PatchContent
        {
            public bool Connected { get; set; }
        }
    }
}
