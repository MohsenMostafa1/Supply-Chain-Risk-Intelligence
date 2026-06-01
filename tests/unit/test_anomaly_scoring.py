import sys
import os
import unittest

# Add project root to Python path so that 'processing' module is found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Now import the function from your streaming job
from processing.streaming.streaming_anomaly_job import score_anomaly

class TestAnomalyScoring(unittest.TestCase):
    def test_score_anomaly_high_vibration(self):
        row = {"avg_vib": 4.5}
        self.assertEqual(score_anomaly(row), 1.0)

    def test_score_anomaly_normal_vibration(self):
        row = {"avg_vib": 1.2}
        self.assertEqual(score_anomaly(row), 0.0)

    def test_score_anomaly_boundary(self):
        # Assuming threshold is > 3.0, exactly 3.0 should be normal (0)
        row = {"avg_vib": 3.0}
        self.assertEqual(score_anomaly(row), 0.0)

if __name__ == "__main__":
    unittest.main()
