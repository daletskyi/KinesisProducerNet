using System;
using System.Diagnostics;

namespace KinesisProducerNet
{
    public static class BashHelper
    {
        public static string Run(string cmd)
        {
            if (string.IsNullOrWhiteSpace(cmd))
            {
                throw new ArgumentException("cmd cannot be not null or whitespace", nameof(cmd));
            }

            var escapedArgs = cmd.Replace("\"", "\\\"");

            var bashProcess = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"{escapedArgs}\"",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                }
            };

            bashProcess.Start();
            bashProcess.WaitForExit();

            var result = bashProcess.StandardOutput.ReadToEnd();

            bashProcess.Dispose();
            return result;
        }
    }
}