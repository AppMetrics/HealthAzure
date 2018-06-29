// <copyright file="AzureServiceBusTopicHealthCheckBuilderExtensions.cs" company="App Metrics Contributors">
// Copyright (c) App Metrics Contributors. All rights reserved.
// </copyright>

using System;
using System.Text;
using System.Threading.Tasks;
using App.Metrics.Health.Logging;
using Microsoft.Azure.ServiceBus;

// ReSharper disable CheckNamespace
namespace App.Metrics.Health
    // ReSharper restore CheckNamespace
{
    public static class AzureServiceBusTopicHealthCheckBuilderExtensions
    {
        private static readonly ILog Logger = LogProvider.For<IRunHealthChecks>();
        private static readonly Message HealthMessage = new Message(Encoding.UTF8.GetBytes("Topic Health Check"));
        private static readonly DateTimeOffset HealthMessageTestSchedule = new DateTimeOffset(DateTime.UtcNow).AddDays(1);
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

        public static IHealthBuilder AddAzureServiceBusTopicConnectivityCheck(
            this IHealthCheckBuilder builder,
            string name,
            TopicClient topicClient,
            TimeSpan cacheDuration)
        {
            builder.AddCachedCheck(name, CheckServiceBusTopicConnectivity(name, topicClient), cacheDuration);

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureServiceBusTopicConnectivityCheck(
            this IHealthCheckBuilder builder,
            string name,
            TopicClient topicClient)
        {
            builder.AddCheck(name, CheckServiceBusTopicConnectivity(name, topicClient));

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureServiceBusTopicConnectivityCheck(
            this IHealthCheckBuilder builder,
            string name,
            string connectionString,
            string topicName,
            TimeSpan cacheDuration)
        {
            var topicClient = new TopicClient(connectionString, topicName) { OperationTimeout = DefaultTimeout };

            builder.AddCachedCheck(name, CheckServiceBusTopicConnectivity(name, topicClient), cacheDuration);

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureServiceBusTopicConnectivityCheck(
            this IHealthCheckBuilder builder,
            string name,
            string connectionString,
            string topicName)
        {
            var topicClient = new TopicClient(connectionString, topicName) { OperationTimeout = DefaultTimeout };

            builder.AddCheck(name, CheckServiceBusTopicConnectivity(name, topicClient));

            return builder.Builder;
        }

        private static Func<ValueTask<HealthCheckResult>> CheckServiceBusTopicConnectivity(string name, TopicClient topicClient)
        {
            return async () =>
            {
                var result = true;

                try
                {
                    var id = await topicClient.ScheduleMessageAsync(HealthMessage, HealthMessageTestSchedule).ConfigureAwait(false);
                    await topicClient.CancelScheduledMessageAsync(id);
                }
                catch (Exception ex)
                {
                    Logger.ErrorException($"{name} failed.", ex);

                    result = false;
                }

                return result
                    ? HealthCheckResult.Healthy($"OK. '{topicClient.Path}/{topicClient.TopicName}' is available.")
                    : HealthCheckResult.Unhealthy($"Failed. '{topicClient.Path}/{topicClient.TopicName}' is unavailable.");
            };
        }
    }
}