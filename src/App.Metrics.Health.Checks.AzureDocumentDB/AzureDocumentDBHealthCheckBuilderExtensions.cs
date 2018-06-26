// <copyright file="AzureDocumentDBHealthCheckBuilderExtensions.cs" company="App Metrics Contributors">
// Copyright (c) App Metrics Contributors. All rights reserved.
// </copyright>

using System;
using System.Net;
using System.Threading.Tasks;
using App.Metrics.Health.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace App.Metrics.Health.Checks.AzureDocumentDB
{
    public static class AzureDocumentDBHealthCheckBuilderExtensions
    {
        private static readonly ILog Logger = LogProvider.For<IRunHealthChecks>();

        public static IHealthBuilder AddAzureDocumentDBCollectionCheck(
            this IHealthCheckBuilder builder,
            string name,
            Uri collectionUri,
            DocumentClient documentClient,
            TimeSpan cacheDuration)
        {
            builder.AddCachedCheck(
                name,
                CheckDocumentDbCollectionExists(collectionUri, documentClient),
                cacheDuration);

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureDocumentDBCollectionCheck(
            this IHealthCheckBuilder builder,
            string name,
            Uri collectionUri,
            DocumentClient documentClient)
        {
            builder.AddCheck(name, CheckDocumentDbCollectionExists(collectionUri, documentClient));

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureDocumentDBDatabaseCheck(
            this IHealthCheckBuilder builder,
            string name,
            Uri databaseUri,
            DocumentClient documentClient,
            TimeSpan cacheDuration)
        {
            builder.AddCachedCheck(
                name,
                CheckDocumentDBDatabaseConnectivity(databaseUri, documentClient),
                cacheDuration);

            return builder.Builder;
        }

        public static IHealthBuilder AddAzureDocumentDBDatabaseCheck(
            this IHealthCheckBuilder builder,
            string name,
            Uri databaseUri,
            DocumentClient documentClient)
        {
            builder.AddCheck(name, CheckDocumentDBDatabaseConnectivity(databaseUri, documentClient));

            return builder.Builder;
        }

        private static Func<ValueTask<HealthCheckResult>> CheckDocumentDbCollectionExists(Uri collectionUri, DocumentClient documentClient)
        {
            return async () =>
            {
                bool result;

                try
                {
                    var database = await documentClient.ReadDocumentCollectionAsync(collectionUri);

                    result = database?.StatusCode == HttpStatusCode.OK;
                }
                catch (DocumentClientException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    Logger.ErrorException($"{collectionUri} was not found.", ex);

                    result = false;
                }
                catch (Exception ex)
                {
                    Logger.ErrorException($"{collectionUri} failed.", ex);

                    result = false;
                }

                return result
                    ? HealthCheckResult.Healthy($"OK. '{collectionUri}' is available.")
                    : HealthCheckResult.Unhealthy($"Failed. '{collectionUri}' is unavailable.");
            };
        }

        private static Func<ValueTask<HealthCheckResult>> CheckDocumentDBDatabaseConnectivity(Uri databaseUri, DocumentClient documentClient)
        {
            return async () =>
            {
                bool result;

                try
                {
                    var database = await documentClient.ReadDatabaseAsync(databaseUri);

                    result = database?.StatusCode == HttpStatusCode.OK;
                }
                catch (DocumentClientException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    Logger.ErrorException($"{databaseUri} was not found.", ex);

                    result = false;
                }
                catch (Exception ex)
                {
                    Logger.ErrorException($"{databaseUri} failed.", ex);

                    result = false;
                }

                return result
                    ? HealthCheckResult.Healthy($"OK. '{databaseUri}' is available.")
                    : HealthCheckResult.Unhealthy($"Failed. '{databaseUri}' is unavailable.");
            };
        }
    }
}