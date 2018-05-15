using System.Collections.Generic;
using System.Linq;

namespace KinesisProducerNet
{
    public class Metric
    {
        public string Name { get; }

        public ulong Duration { get; }

        public IReadOnlyDictionary<string, string> Dimensions { get; }

        public double Sum { get; }

        public double Mean { get; }

        public double SampleCount { get; }

        public double Min { get; }

        public double Max { get; }

        public Metric(Protobuf.Metric m)
        {
            this.Name = m.Name;
            this.Duration = m.Seconds;

            var dimensions = new Dictionary<string, string>();
            foreach (var dimension in m.Dimensions)
            {
                dimensions.Add(dimension.Key, dimension.Value);
            }

            this.Dimensions = dimensions;
            this.Sum = m.Stats.Sum;
            this.Mean = m.Stats.Mean;
            this.SampleCount = m.Stats.Count;
            this.Min = m.Stats.Min;
            this.Max = m.Stats.Max;
        }

        public override string ToString()
        {
            var dimensionsString = string.Join(",", Dimensions.Select(x => $"\"{x.Key}\":\"{x.Value}\""));
            return $"Metric [name={Name}, duration={Duration}, dimensions={dimensionsString}, sum={Sum}," +
                   $" mean={Mean}, sampleCount={SampleCount}, min={Min}, max={Max}]";

        }
    }
}