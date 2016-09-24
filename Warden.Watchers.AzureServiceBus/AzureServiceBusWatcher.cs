using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Warden.Watchers.AzureServiceBus
{
    public class MonitoringState
    {
        public List<string> MessageIds { get; set; } = new List<string>();
    }

    public class AzureServiceBusWatcher : IWatcher
    {
        public const string DefaultName = "Azure Service Bus Watcher";
        private readonly AzureServiceBusWatcherConfiguration _configuration;
        public string Group { get; }
        public string Name { get; }
        private readonly ConcurrentDictionary<string, string> _queueProcessingMonitoring = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<string, string> _topicProcessingMonitoring = new ConcurrentDictionary<string, string>();
        private readonly Dictionary<string, MonitoringState> _queueMonitoringStates = new Dictionary<string, MonitoringState>();
        private readonly Dictionary<string, MonitoringState> _topicMonitoringStates = new Dictionary<string, MonitoringState>();
        public async Task<IWatcherCheckResult> ExecuteAsync()
        {
            try
            {
                NamespaceManager manager = NamespaceManager.CreateFromConnectionString(_configuration.ConnectionString);
                List<string> errors = new List<string>();
                if (_configuration.QueueProcessingConfiguration.MonitorProcessing || _configuration.MonitorQueues.Any())
                {
                    var queues = (await manager.GetQueuesAsync()).ToList();
                    if (_configuration.QueueProcessingConfiguration.MonitorProcessing)
                    {
                        List<QueueDescription> queuesToMonitor = GetMonitorProcessingQueues(_configuration.QueueProcessingConfiguration, queues);
                        foreach (QueueDescription queueDescription in queuesToMonitor)
                        {
                            QueueClient client = QueueClient.CreateFromConnectionString(_configuration.ConnectionString, queueDescription.Path);
                            var nextMessage = await client.PeekAsync();
                            if (nextMessage == null) continue;
                            string lastMessageId;
                            _queueProcessingMonitoring.TryGetValue(queueDescription.Path, out lastMessageId);
                            if (lastMessageId == nextMessage.MessageId)
                            {
                                errors.Add(
                                    $"The queue `{queueDescription.Path}` seems to be stalling it's processing.");
                            }
                            else
                            {
                                _queueProcessingMonitoring.TryAdd(queueDescription.Path, nextMessage.MessageId);
                            }
                        }
                    }
                    if (_configuration.MonitorQueues.Any())
                    {
                        var queuesToMonitor = GetSpecificQueues(queues, _configuration.MonitorQueues);
                        foreach (QueueDescription queueDescription in queuesToMonitor)
                        {
                            QueueClient client = QueueClient.CreateFromConnectionString(_configuration.ConnectionString, queueDescription.Path);
                            MonitoringState monitoringState;
                            if (!_queueMonitoringStates.TryGetValue(queueDescription.Path, out monitoringState))
                            {
                                monitoringState = new MonitoringState();
                                _queueMonitoringStates.Add(queueDescription.Path, monitoringState);
                            }
                            bool hasNewMessages = true;
                            while (hasNewMessages)
                            {
                                var message = await client.PeekAsync();
                                if (message != null)
                                {
                                    if (!monitoringState.MessageIds.Contains(message.MessageId))
                                    {
                                        monitoringState.MessageIds.Add(message.MessageId);
                                        errors.Add($"The queue `{queueDescription.Path}` has one new message. Message Id `{message.MessageId}`. Content: ```{new StreamReader(message.GetBody<Stream>()).ReadToEnd()}```");
                                    }
                                }
                                else
                                {
                                    hasNewMessages = false;
                                }
                            }
                        }
                    }
                }
                if (_configuration.TopicProcessingConfiguration.MonitorProcessing || _configuration.MonitorTopics.Any())
                {
                    var topics = (await manager.GetTopicsAsync()).ToList();
                    if (_configuration.TopicProcessingConfiguration.MonitorProcessing)
                    {
                        var topicsToMonitor = GetMonitorProcessingTopics(_configuration.TopicProcessingConfiguration,
                            topics);
                        foreach (TopicDescription topicDescription in topicsToMonitor)
                        {
                            TopicClient client = TopicClient.CreateFromConnectionString(_configuration.ConnectionString, topicDescription.Path);
                            var nextMessage = await client.PeekAsync();
                            if (nextMessage == null) continue;
                            string lastMessageId;
                            _topicProcessingMonitoring.TryGetValue(topicDescription.Path, out lastMessageId);
                            if (lastMessageId == nextMessage.MessageId)
                            {
                                errors.Add(
                                    $"The topic `{topicDescription.Path}` seems to be stalling it's processing. Message Id {nextMessage.MessageId}.");
                            }
                            else
                            {
                                _topicProcessingMonitoring.TryAdd(topicDescription.Path, nextMessage.MessageId);
                            }
                        }
                    }
                    if (_configuration.MonitorTopics.Any())
                    {
                        var topicsToMonitor = GetSpecificTopics(topics, _configuration.MonitorTopics);
                        foreach (TopicDescription topicDescription in topicsToMonitor)
                        {
                            TopicClient client = TopicClient.CreateFromConnectionString(_configuration.ConnectionString, topicDescription.Path);
                            MonitoringState monitoringState;
                            if (!_topicMonitoringStates.TryGetValue(topicDescription.Path, out monitoringState))
                            {
                                monitoringState = new MonitoringState();
                                _topicMonitoringStates.Add(topicDescription.Path, monitoringState);
                            }
                            bool hasNewMessages = true;
                            while (hasNewMessages)
                            {
                                var message = await client.PeekAsync();
                                if (message != null)
                                {
                                    if (!monitoringState.MessageIds.Contains(message.MessageId))
                                    {
                                        monitoringState.MessageIds.Add(message.MessageId);
                                        errors.Add($"The queue `{topicDescription.Path}` has one new message. Message Id `{message.MessageId}`. Content: ```{new StreamReader(message.GetBody<Stream>()).ReadToEnd()}```");
                                    }
                                }
                                else
                                {
                                    hasNewMessages = false;
                                }
                            }
                        }
                    }
                }
                if (errors.Any())
                {
                    return AzureServiceBusWatcherCheckResult.Create(this, false, manager, string.Join("\r\n", errors));
                }

                return await EnsureAsync(manager);
            }
            catch (Exception exception)
            {
                throw new WatcherException($"There was an error while trying to access the Service Bus", exception);
            }
        }

        private List<TopicDescription> GetMonitorProcessingTopics(AzureServiceBusWatcherConfiguration.ProcessingConfiguration processingConfiguration, List<TopicDescription> topics)
        {
            if (processingConfiguration.MonitorAllItems)
            {
                return topics;
            }
            else if (processingConfiguration.ExcemptItems.Any())
            {
                return
                    topics.Where(
                            topic => processingConfiguration.ExcemptItems.All(excemptTopic => excemptTopic != topic.Path))
                        .ToList();
            }
            else if (processingConfiguration.SpecificItems.Any())
            {
                return GetSpecificTopics(topics, _configuration.TopicProcessingConfiguration.SpecificItems);
            }
            throw new ArgumentException("Configuration for topic processing has been setup incorrectly.");
        }

        private List<TopicDescription> GetSpecificTopics(List<TopicDescription> topics, List<string> specificTopics)
        {
            return
                topics.Where(
                        topic => specificTopics.Any(specificTopic => specificTopic == topic.Path))
                    .ToList();
        }

        private List<QueueDescription> GetMonitorProcessingQueues(AzureServiceBusWatcherConfiguration.ProcessingConfiguration processingConfiguration, List<QueueDescription> queues)
        {
            if (processingConfiguration.MonitorAllItems)
            {
                return queues;
            }
            else if (processingConfiguration.ExcemptItems.Any())
            {
                return
                    queues.Where(
                            queue => processingConfiguration.ExcemptItems.All(excemptQueue => excemptQueue != queue.Path))
                        .ToList();
            }
            else if (processingConfiguration.SpecificItems.Any())
            {
                return GetSpecificQueues(queues, _configuration.QueueProcessingConfiguration.SpecificItems);
            }
            throw new ArgumentException("Configuration for queue processing has been setup incorrectly.");
        }

        private List<QueueDescription> GetSpecificQueues(List<QueueDescription> queues, List<string> specificQueues)
        {
            return
                   queues.Where(
                           queue => specificQueues.Any(specificQueue => specificQueue == queue.Path))
                       .ToList();
        }

        private async Task<IWatcherCheckResult> EnsureAsync(NamespaceManager namespaceManager)
        {
            bool isValid = true;
            if (_configuration.EnsureThatAsync != null)
            {
                isValid = await _configuration.EnsureThatAsync?.Invoke(namespaceManager);
            }
            isValid = isValid && (_configuration.EnsureThat?.Invoke(namespaceManager) ?? true);

            return AzureServiceBusWatcherCheckResult.Create(this, isValid, namespaceManager,
                $"Service Bus {namespaceManager.Address} is checked with result {isValid}");
        }
        protected AzureServiceBusWatcher(string name, AzureServiceBusWatcherConfiguration configuration, string group)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Watcher name cannot be empty.");
            }
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration), "Azure Service Bus Watcher configuration has not been provided.");
            }

            Name = name;
            _configuration = configuration;
            Group = group;
        }

        public static AzureServiceBusWatcher Create(string name,
                AzureServiceBusWatcherConfiguration configuration, string group = null)
            => new AzureServiceBusWatcher(name, configuration, group);
        public static AzureServiceBusWatcher Create(string connectionString, Action<AzureServiceBusWatcherConfiguration.Default> configurator = null, string group = null) => Create(DefaultName, connectionString, configurator, group);
        public static AzureServiceBusWatcher Create(string name, string connectionString,
                Action<AzureServiceBusWatcherConfiguration.Default> configurator = null, string group = null)
        {
            var config = new AzureServiceBusWatcherConfiguration.Builder(connectionString);
            configurator?.Invoke((AzureServiceBusWatcherConfiguration.Default) config);
            return new AzureServiceBusWatcher(DefaultName, config.Build(), group);
        }

    }
}
