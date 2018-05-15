using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace KinesisProducerNet
{
    public class CertificateExtractor
    {
        public const string CA_CERTS_DIRECTORY_NAME = "cacerts";

        public const string LOCK_FILE_NAME = CA_CERTS_DIRECTORY_NAME + ".lock";

        public static readonly List<string> CERTIFICATE_FILES = new List<string>
        {
            "062cdee6.0", "09789157.0", "116bf586.0", "1d3472b9.0",
            "244b5494.0", "2c543cd1.0", "2e4eed3c.0", "3513523f.0", "480720ec.0", "4a6481c9.0", "4bfab552.0",
            "5ad8a5d6.0", "607986c7.0", "653b494a.0", "6d41d539.0", "75d1b2ed.0", "76cb8f92.0", "7d0b38bd.0",
            "7f3d5d1d.0", "8867006a.0", "8cb5ee0f.0", "9d04f354.0", "ad088e1d.0", "b0e59380.0", "b1159c4c.0",
            "b204d74a.0", "ba89ed3b.0", "c01cdfa2.0", "c089bbbd.0", "c0ff1f52.0", "cbeee9e2.0", "cbf06781.0",
            "ce5e74ef.0", "dd8e9d41.0", "de6d66f3.0", "e2799e36.0", "f081611a.0", "f387163d.0"
        };

        private readonly ILogger logger = Logging.CreateLogger<CertificateExtractor>();
        public List<string> ExtractedCertificates { get; }

        public CertificateExtractor()
        {
            this.ExtractedCertificates = new List<string>();
        }

        public string ExtractCertificates(string tempDirectory)
        {
            var lockFile = Path.Combine(tempDirectory, LOCK_FILE_NAME);
            var lockHeld = false;
            var attempts = 1;
            var destinationCaDirectory = this.PrepareDestination(tempDirectory);

            while (!lockHeld)
            {
                try
                {
                    using (var fileStream = new FileStream(lockFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
                    {
                        lockHeld = true;
                        this.ExtractAndVerifyCertificates(destinationCaDirectory);
                    }
                }
                catch (Exception)
                {
                    attempts++;
                    this.logger.LogInformation($"Another thread holds the certificate lock, sleeping for 1 second and will attempt again.  Lock Attempts: {attempts}");

                    try
                    {
                        Thread.Sleep(1000);
                    }
                    catch (Exception)
                    {
                        this.logger.LogInformation($"Interrupted while sleeping for lock.  Giving up on certificate extraction.");
                        break;
                    }
                }
            }

            return destinationCaDirectory;
        }

        private void ExtractAndVerifyCertificates(string destinationCaDirectory)
        {
            foreach (var certificate in CERTIFICATE_FILES)
            {
                var certResourceName = $"{CA_CERTS_DIRECTORY_NAME}.{certificate}";
                var data = IOUtils.GetResourceFile(certResourceName, null);
                var destinationCertificatePath = Path.Combine(destinationCaDirectory, certificate);
                this.logger.LogDebug($"Extracting certificate '{certificate}' to '{destinationCertificatePath}'");
                this.ExtractedCertificates.Add(destinationCertificatePath);

                if (File.Exists(destinationCertificatePath))
                {
                    var contentEqual = false;
                    if (new FileInfo(destinationCertificatePath).Length == data.Length)
                    {
                        byte[] existingData = File.ReadAllBytes(destinationCertificatePath);
                        contentEqual = data.SequenceEqual(existingData);
                    }

                    if (contentEqual)
                    {
                        this.logger.LogDebug($"Certificate '{certificate}' already exists, and content matches. Skipping");
                        continue;
                    }
                    else
                    {
                        this.logger.LogInformation($"Certificate '{certificate}' already exists, but the content doesn't match. Overwriting");
                    }
                }

                File.WriteAllBytes(destinationCertificatePath, data);
            }
        }

        private string PrepareDestination(string tempDirectory)
        {
            var path = Path.Combine(tempDirectory, CA_CERTS_DIRECTORY_NAME);
            if (!Directory.Exists(path) && Directory.CreateDirectory(path) == null)
            {
                throw new IOException($"Failed to create directory for certs at '{path}'");
            }

            return path;
        }
    }
}