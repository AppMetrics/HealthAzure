// <copyright file="AzureQueueStorageHealthCheckBuilderExtensions.cs" company="App Metrics Contributors">
// Copyright (c) App Metrics Contributors. All rights reserved.
// </copyright>

using System;
using System.Threading.Tasks;
using App.Metrics.Health.Logging;
using Microsoft.WindowsAzure.Storage;

namespace App.Metrics.Health.Checks.AzureStorage
{
    public static class AzureQueueStorageHealthCheckBuilderExtensions
    {
        private static readonly ILog Logger = LogProvider.For<IRunHealthChecks>();

        public static IHealthBuilder AddAzureQueueStorageCheck(
            this IHealthCheckBuilder builder,
            string name,
            CloudStorageAccount storageAccount,
            string queueName,
            TimeSpan cacheDuration)
        {
            builder.AddCachedCheck(name, CheckQueueExistsAsync(name, storageAccount, queueName), cacheDuration);

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureQueueStorageCheck(
            this IHealthCheckBuilder builder,
            string name,
            CloudStorageAccount storageAccount,
            string queueName)
        {
            builder.AddCheck(name, CheckQueueExistsAsync(name, storageAccount, queueName));

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureQueueStorageConnectivityCheck(
            this IHealthCheckBuilder builder,
            string name,
            CloudStorageAccount storageAccount,
            TimeSpan cacheDuration)
        {
            builder.AddCachedCheck(
                name,
                CheckStorageAccountConnectivity(name, storageAccount),
                cacheDuration);

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureQueueStorageConnectivityCheck(
            this IHealthCheckBuilder builder,
            string name,
            CloudStorageAccount storageAccount)
        {
            builder.AddCheck(
                name,
                CheckStorageAccountConnectivity(name, storageAccount));

            return builder.Builder;
        }

        private static Func<ValueTask<HealthCheckResult>> CheckQueueExistsAsync(string name, CloudStorageAccount storageAccount, string queueName)
        {
            return async () =>
            {
                bool result;

                try
                {
                    var queueClient = storageAccount.CreateCloudQueueClient();

                    var queue = queueClient.GetQueueReference(queueName);

                    result = await queue.ExistsAsync();
                }
                catch (Exception ex)
                {
                    Logger.ErrorException($"{name} failed.", ex);

                    result = false;
                }

                return result
                    ? HealthCheckResult.Healthy($"OK. '{queueName}' is available.")
                    : HealthCheckResult.Unhealthy($"Failed. '{queueName}' is unavailable.");
            };
        }

        private static Func<ValueTask<HealthCheckResult>> CheckStorageAccountConnectivity(string name, CloudStorageAccount storageAccount)
        {
            return async () =>
            {
                var result = true;

                try
                {
                    var queueClient = storageAccount.CreateCloudQueueClient();

                    await queueClient.GetServicePropertiesAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Logger.ErrorException($"{name} failed.", ex);

                    result = false;
                }

                return result
                    ? HealthCheckResult.Healthy($"OK. '{storageAccount.BlobStorageUri}' is available.")
                    : HealthCheckResult.Unhealthy($"Failed. '{storageAccount.BlobStorageUri}' is unavailable.");
            };
        }
    }
}