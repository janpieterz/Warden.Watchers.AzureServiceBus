using Microsoft.ServiceBus;

namespace Warden.Watchers.AzureServiceBus
{
    public class AzureServiceBusWatcherCheckResult : WatcherCheckResult
    {
        public NamespaceManager NamespaceManager { get; set; }

        public AzureServiceBusWatcherCheckResult(AzureServiceBusWatcher watcher, bool isValid, string description, NamespaceManager namespaceManager) : base(watcher, isValid, description)
        {
            NamespaceManager = namespaceManager;
        }

        public static AzureServiceBusWatcherCheckResult Create(AzureServiceBusWatcher watcher, bool isValid,
                NamespaceManager namespaceManager, string description = "")
            => new AzureServiceBusWatcherCheckResult(watcher, isValid, description, namespaceManager);
    }
}