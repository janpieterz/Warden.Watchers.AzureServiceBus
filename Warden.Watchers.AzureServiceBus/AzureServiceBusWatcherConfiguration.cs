using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.ServiceBus;

namespace Warden.Watchers.AzureServiceBus
{
    public class AzureServiceBusWatcherConfiguration
    {
        public class ProcessingConfiguration
        {
            public bool MonitorProcessing { get; set; }
            public List<string> ExcemptItems { get; set; } = new List<string>();
            public List<string> SpecificItems { get; set; } = new List<string>();
            public bool MonitorAllItems { get; set; }

            public bool ConfigurationValid
            {
                get
                {
                    if (MonitorAllItems && (ExcemptItems.Any() || SpecificItems.Any()))
                    {
                        return false;
                    }
                    if (ExcemptItems.Any() && SpecificItems.Any())
                    {
                        return false;
                    }
                    return true;
                }
            }
        }
        public ProcessingConfiguration QueueProcessingConfiguration { get; set; } = new ProcessingConfiguration();
        public ProcessingConfiguration TopicProcessingConfiguration { get; set; } = new ProcessingConfiguration();
        public List<string> MonitorQueues { get; set; } = new List<string>();
        public List<string> MonitorTopics { get; set; } = new List<string>();
        public string ConnectionString { get; protected set; }
        public Func<NamespaceManager, Task<bool>> EnsureThatAsync { get; protected set; }
        public Func<NamespaceManager, bool> EnsureThat { get; protected set; }

        protected internal AzureServiceBusWatcherConfiguration(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("URL can not be empty.", nameof(connectionString));
            }
            ConnectionString = connectionString;
        }

        public class Default : Configurator<Default>
        {
            public Default(AzureServiceBusWatcherConfiguration configuration) : base(configuration)
            {
                SetInstance(this);
            }
        }

        public class Configurator<T> : WatcherConfigurator<T, AzureServiceBusWatcherConfiguration> where T : Configurator<T>
        {
            protected Configurator(string url)
            {
                Configuration = new AzureServiceBusWatcherConfiguration(url);
            }

            protected Configurator(AzureServiceBusWatcherConfiguration configuration) : base(configuration)
            {
            }

            public T EnsureThat(Func<NamespaceManager, bool> ensureThat)
            {
                if (ensureThat == null)
                {
                    throw new ArgumentException("Ensure that predicate can not be null", nameof(ensureThat));
                }
                Configuration.EnsureThat = ensureThat;
                return Configurator;
            }

            public T EnsureThatAsync(Func<NamespaceManager, Task<bool>> ensureThat)
            {
                if (ensureThat == null)
                    throw new ArgumentException("Ensure that async predicate can not be null.", nameof(ensureThat));

                Configuration.EnsureThatAsync = ensureThat;

                return Configurator;
            }
            /// <summary>
            /// Monitor the messages being processed and not retained by the queues
            /// </summary>
            public T MonitorMessageProcessingInAllQueues()
            {
                Configuration.QueueProcessingConfiguration.MonitorProcessing = true;
                Configuration.QueueProcessingConfiguration.MonitorAllItems = true;

                if (!Configuration.QueueProcessingConfiguration.ConfigurationValid)
                {
                    throw new ArgumentException($"Ensure that you call  only one of: `{nameof(MonitorMessageProcessingInQueues)}`, `{nameof(MonitorMessageProcessingInAllQueuesExcept)}`, `{nameof(MonitorMessageProcessingInAllQueues)}` for one watcher");
                }

                return Configurator;
            }
            /// <summary>
            /// Monitor the messages being processed and not retained by the queues
            /// </summary>
            /// <param name="excemptQueues">Exclude specific queues</param>
            public T MonitorMessageProcessingInAllQueuesExcept(IEnumerable<string> excemptQueues)
            {
                var queues = excemptQueues.ToList();
                if (!queues.Any())
                {
                    throw new ArgumentException(" Ensure that the excempt queues contain at least 1 queue", nameof(excemptQueues));
                }
                Configuration.QueueProcessingConfiguration.ExcemptItems = queues;
                Configuration.QueueProcessingConfiguration.MonitorProcessing = true;
                if (!Configuration.QueueProcessingConfiguration.ConfigurationValid)
                {
                    throw new ArgumentException($"Ensure that you call  only one of: `{nameof(MonitorMessageProcessingInQueues)}`, `{nameof(MonitorMessageProcessingInAllQueuesExcept)}`, `{nameof(MonitorMessageProcessingInAllQueues)}` for one watcher");
                }
                return Configurator; 
            }
            /// <summary>
            /// Monitor the messages being processed and not retained by the queues
            /// </summary>
            /// <param name="specificQueues">Specific queues to monitor</param>
            public T MonitorMessageProcessingInQueues(IEnumerable<string> specificQueues)
            {
                var queues = specificQueues.ToList();
                if (!queues.Any())
                {
                    throw new ArgumentException(" Ensure that the specific queues contain at least 1 queue", nameof(specificQueues));
                }
                Configuration.QueueProcessingConfiguration.SpecificItems = queues;
                Configuration.QueueProcessingConfiguration.MonitorProcessing = true;
                if (!Configuration.QueueProcessingConfiguration.ConfigurationValid)
                {
                    throw new ArgumentException($"Ensure that you call  only one of: `{nameof(MonitorMessageProcessingInQueues)}`, `{nameof(MonitorMessageProcessingInAllQueuesExcept)}`, `{nameof(MonitorMessageProcessingInAllQueues)}` for one watcher");
                }
                return Configurator;
            }
            /// <summary>
            /// Monitor messages in queues (very handy for error queue monitoring). This will result in a 'failure' whenever a message stays in a queue
            /// </summary>
            /// <param name="specificQueues"></param>
            public T MonitorMessagesInQueues(IEnumerable<string> specificQueues)
            {
                var queues = specificQueues.ToList();
                if (!queues.Any())
                {
                    throw new ArgumentException(" Ensure that the specific queues contain at least 1 queue", nameof(specificQueues));
                }
                Configuration.MonitorQueues = queues;
                return Configurator;
            }

            /// <summary>
            /// Monitor the message being processed and not retained by the topics
            /// </summary>
            public T MonitorMessageProcessingInAllTopics()
            {
                Configuration.TopicProcessingConfiguration.MonitorProcessing = true;
                Configuration.TopicProcessingConfiguration.MonitorAllItems = true;
                if (!Configuration.TopicProcessingConfiguration.ConfigurationValid)
                {
                    throw new ArgumentException($"Ensure that you call  only one of: `{nameof(MonitorMessageProcessingInTopics)}`, `{nameof(MonitorMessageProcessingInAllTopicsExcept)}`, `{nameof(MonitorMessageProcessingInAllTopics)}` for one watcher");
                }
                return Configurator;
            }
            /// <summary>
            /// Monitor the message being processed and not retained by the topics
            /// </summary>
            /// <param name="excemptTopics">Topics that are excempt from the monitoring</param>
            public T MonitorMessageProcessingInAllTopicsExcept(IEnumerable<string> excemptTopics)
            {
                var queues = excemptTopics.ToList();
                if (!queues.Any())
                {
                    throw new ArgumentException(" Ensure that the excempt queues contain at least 1 queue", nameof(excemptTopics));
                }
                Configuration.TopicProcessingConfiguration.ExcemptItems = queues;
                Configuration.TopicProcessingConfiguration.MonitorProcessing = true;
                if (!Configuration.TopicProcessingConfiguration.ConfigurationValid)
                {
                    throw new ArgumentException($"Ensure that you call  only one of: `{nameof(MonitorMessageProcessingInTopics)}`, `{nameof(MonitorMessageProcessingInAllTopicsExcept)}`, `{nameof(MonitorMessageProcessingInAllTopics)}` for one watcher");
                }
                return Configurator;
            }
            /// <summary>
            /// Monitor the message being processed and not retained by the topics
            /// </summary>
            /// <param name="specificTopics">Specific topics to monitor</param>
            public T MonitorMessageProcessingInTopics(IEnumerable<string> specificTopics)
            {
                var queues = specificTopics.ToList();
                if (!queues.Any())
                {
                    throw new ArgumentException(" Ensure that the specific queues contain at least 1 queue", nameof(specificTopics));
                }

                Configuration.TopicProcessingConfiguration.SpecificItems = queues;
                Configuration.TopicProcessingConfiguration.MonitorProcessing = true;
                if (!Configuration.TopicProcessingConfiguration.ConfigurationValid)
                {
                    throw new ArgumentException($"Ensure that you call  only one of: `{nameof(MonitorMessageProcessingInTopics)}`, `{nameof(MonitorMessageProcessingInAllTopicsExcept)}`, `{nameof(MonitorMessageProcessingInAllTopics)}` for one watcher");
                }
                return Configurator;
            }
            /// <summary>
            /// Monitor messages in topics. Fails every message it catches.
            /// </summary>
            /// <param name="specificTopics">Specific topics to monitor</param>
            public T MonitorMessagesInTopics(IEnumerable<string> specificTopics)
            {
                var queues = specificTopics.ToList();
                if (!queues.Any())
                {
                    throw new ArgumentException(" Ensure that the specific queues contain at least 1 queue", nameof(specificTopics));
                }
                Configuration.MonitorTopics = queues;
                return Configurator;
            }
        }
        public class Builder : Configurator<Builder>
        {
            public Builder(string connectionString) : base(connectionString)
            {
                SetInstance(this);
            }

            public AzureServiceBusWatcherConfiguration Build() => Configuration;

            public static explicit operator Default(Builder builder) => new Default(builder.Configuration);
        }
    }
}