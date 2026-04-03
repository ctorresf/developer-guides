import apache_beam as beam
from apache_beam.transforms import window
import datetime

# Event Time Injection (Pardo + DoFn)
class InjectTimeDoFn(beam.DoFn):
    def process(self, element):
        dt = datetime.datetime.strptime(element['ts'], "%Y-%m-%dT%H:%M:%SZ")
        yield beam.window.TimestampedValue(element, dt.timestamp())

# Mock data: Test Records (10 records, 2 transformers)
# The T-100 will drop below 200V at the center windows.
monitoring_data = [
    # Block 1: T-100 (Stable) and T-200 (Stable)
    {'id': 'T-100', 'v': 220, 'ts': '2026-04-03T19:00:05Z'},
    {'id': 'T-200', 'v': 215, 'ts': '2026-04-03T19:00:10Z'},
    {'id': 'T-100', 'v': 218, 'ts': '2026-04-03T19:00:20Z'},
    
    # Block 2: T-100 starts to drop (190V)
    {'id': 'T-100', 'v': 190, 'ts': '2026-04-03T19:00:35Z'},
    {'id': 'T-200', 'v': 212, 'ts': '2026-04-03T19:00:40Z'},
    {'id': 'T-100', 'v': 185, 'ts': '2026-04-03T19:00:50Z'},
    
    # Block 3: T-100 continues low and T-200 remains stable
    {'id': 'T-100', 'v': 180, 'ts': '2026-04-03T19:01:05Z'},
    {'id': 'T-200', 'v': 218, 'ts': '2026-04-03T19:01:15Z'},
    {'id': 'T-100', 'v': 195, 'ts': '2026-04-03T19:01:20Z'},
    {'id': 'T-200', 'v': 220, 'ts': '2026-04-03T19:01:25Z'}
]

with beam.Pipeline() as p:
    # 1. Reading and Time Stamping
    stream_data = (
        p | "Sensor Readings" >> beam.Create(monitoring_data)
          | "Assign Event Time" >> beam.ParDo(InjectTimeDoFn())
    )

    # 2. Sliding Window: 1 min size, slides every 30s
    windows_data = stream_data | "Sliding Window 60/30" >> beam.WindowInto(
        window.SlidingWindows(size=60, period=30)
    )

    # 3. Aggregation: Average per Transformer
    averages_data = (
        windows_data
        | "Extract ID and V" >> beam.Map(lambda x: (x['id'], x['v']))
        | "Moving Average" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
    )

    # 4. Alert Logic (Filter < 200V)
    alert_data = averages_data | "Detect Undervoltage" >> beam.Filter(lambda x: x[1] < 200)

    # 5. Alert Formatting for Technical Team
    def issue_alert(element, window=beam.DoFn.WindowParam):
        id_trans, avg_v = element
        t_start = window.start.to_utc_datetime().strftime('%H:%M:%S')
        t_end = window.end.to_utc_datetime().strftime('%H:%M:%S')
        
        return (f"🚨 [TECHNICAL ALERT] | Window: {t_start} - {t_end} | "
                f"Transformer: {id_trans} | Average Voltage: {avg_v:.1f}V | "
                f"ACTION: Dispatch crew immediately.")

    (
        alert_data
        | "Format Alert Message" >> beam.Map(issue_alert)
        | "Output Alerts" >> beam.Map(print)
    )