using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace KinesisProducerNet
{
    public sealed class FileAgeManager
    {
        private static readonly Lazy<FileAgeManager> LazyInstance =
            new Lazy<FileAgeManager>(() => new FileAgeManager());

        public static FileAgeManager Instance => LazyInstance.Value;

        private readonly HashSet<string> watchedFiles;
        private readonly Timer ticker;
        private readonly ILogger logger = Logging.CreateLogger<FileAgeManager>();

        private FileAgeManager()
        {
            this.watchedFiles = new HashSet<string>();
            this.ticker = new Timer(Run, null, (int)TimeSpan.TicksPerMinute, (int)TimeSpan.TicksPerMinute);
        }

        public void RegisterFiles(List<string> files)
        {
            foreach (var file in files)
            {
                this.watchedFiles.Add(file);
            }
        }

        private void Run(object state)
        {
            foreach (var file in watchedFiles)
            {
                if (!File.Exists(file))
                {
                    this.logger.LogError($"File '{file}' doesn't exist or has been removed. This could cause problems with the native components.");
                }
                else
                {
                    try
                    {
                        File.SetLastWriteTime(file, DateTime.UtcNow);
                    }
                    catch (Exception)
                    {
                        this.logger.LogWarning($"Failed to update the last modified time of '{file}'.");
                    }
                }
            }
        }
    }
}