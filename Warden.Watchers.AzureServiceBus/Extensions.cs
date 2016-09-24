using System;
using Warden.Core;

namespace Warden.Watchers.AzureServiceBus
{
    public static class Extensions
    {
        public static WardenConfiguration.Builder AddAzureServiceBusWatcher(this WardenConfiguration.Builder builder, string connectionString,
            Action<WatcherHooksConfiguration.Builder> hooks = null, TimeSpan? interval = null, string group = null)
        {
            builder.AddWatcher(AzureServiceBusWatcher.Create(connectionString, @group: group), hooks, interval);
            return builder;
        }

        public static WardenConfiguration.Builder AddAzureServiceBusWatcher(this WardenConfiguration.Builder builder, string name,
            string connectionString, Action<WatcherHooksConfiguration.Builder> hooks = null, TimeSpan? interval = null,
            string group = null)
        {
            builder.AddWatcher(AzureServiceBusWatcher.Create(name, connectionString, @group: group), hooks, interval);
            return builder;
        }

        public static WardenConfiguration.Builder AddAzureServiceBusWatcher(this WardenConfiguration.Builder builder, string connectionString, Action<AzureServiceBusWatcherConfiguration.Default> configurator, Action<WatcherHooksConfiguration.Builder> hooks = null, TimeSpan? interval = null,
            string group = null)
        {
            builder.AddWatcher(AzureServiceBusWatcher.Create(connectionString, configurator, @group: group), hooks, interval);
            return builder;
        }

        public static WardenConfiguration.Builder AddAzureServiceBusWatcher(this WardenConfiguration.Builder builder, string name, string connectionString, Action<AzureServiceBusWatcherConfiguration.Default> configurator, Action<WatcherHooksConfiguration.Builder> hooks = null, TimeSpan? interval = null,
            string group = null)
        {
            builder.AddWatcher(AzureServiceBusWatcher.Create(name, connectionString, configurator, @group: group), hooks, interval);
            return builder;
        }
    }
}
