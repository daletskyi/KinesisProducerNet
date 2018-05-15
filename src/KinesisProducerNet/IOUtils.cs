using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

namespace KinesisProducerNet
{
    internal static class IOUtils
    {
        public static byte[] GetResourceFile(string path, string os)
        {
            Assembly assembly;
            if (os == null)
            {
                assembly = Assembly.Load(new AssemblyName("KinesisProducerNet"));
            }
            else
            {
                switch (os.ToLowerInvariant())
                {
                    case "windows":
                        assembly = Assembly.Load(new AssemblyName("KinesisProducerNet.Windows"));
                        break;
                    case "linux":
                        assembly = Assembly.Load(new AssemblyName("KinesisProducerNet.Linux"));
                        break;
                    case "macos":
                        assembly = Assembly.Load(new AssemblyName("KinesisProducerNet.MacOs"));
                        break;
                    default:
                        throw new Exception($"Your operation system is not supported ({RuntimeInformation.OSDescription}), the library only supports Linux, OSX and Windows");
                }
            }

            var resources = assembly.GetManifestResourceNames();
            var name = resources.FirstOrDefault(x => x.ToLowerInvariant().EndsWith(path.ToLowerInvariant()));
            if (name == null)
            {
                throw new IOException($"Cannot find resource file {path}");
            }

            var resource = assembly.GetManifestResourceStream(name);

            using (var ms = new MemoryStream())
            {
                resource.CopyTo(ms);
                var result = ms.ToArray();
                return result;
            }
        }
    }
}